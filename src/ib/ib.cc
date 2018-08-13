#include "ib.h"

#include <boost/filesystem.hpp>

#include "jts/Contract.h"
#include "jts/EClientSocket.h"
#include "jts/Execution.h"
#include "jts/Order.h"
#include "jts/OrderState.h"
#include "opentrade/logger.h"
#include "opentrade/order.h"
#include "opentrade/security.h"
#include "opentrade/task_pool.h"

static inline decltype(auto) GetTime(const char* timestr) {
  int y, h, m, s;
  if (sscanf(timestr, "%d  %d:%d:%d", &y, &h, &m, &s) != 4)
    return opentrade::NowUtcInMicro();
  boost::posix_time::ptime pt(
      boost::gregorian::date(y / 10000, y % 10000 / 100, y % 100),
      boost::posix_time::time_duration(h, m, s));
  tm tmp = boost::posix_time::to_tm(pt);
  return mktime(&tmp) * 1000000l;
}

static inline decltype(auto) CreateContract(const opentrade::Security& sec) {
  auto contract = std::make_shared<Contract>();
  contract->secType = sec.type;
  contract->currency = sec.currency;
  if (contract->currency == "CNY") contract->currency = "CNH";
  if (sec.type == opentrade::kForexPair) {
    contract->symbol.assign(sec.symbol, std::min(3lu, strlen(sec.symbol)));
  } else {
    contract->localSymbol = sec.local_symbol;
  }

  contract->exchange = sec.exchange->ib_name;
  return contract;
}

IB::IB() : client_(new EClientSocket(this, &os_signal_)) {}

IB::~IB() {
  if (reader_) delete reader_;
  delete client_;
}

void IB::Start() noexcept {
  auto path = boost::filesystem::path(".") / "store" / (name() + "-session");

  std::ifstream ifs(path.c_str());
  if (ifs.good()) {
    auto n = 0;
    std::string line;
    while (std::getline(ifs, line)) {
      if (line.empty() || line.at(0) == '#') continue;
      uint32_t id;
      uint32_t id2;
      if (sscanf(line.c_str(), "%u %u", &id, &id2) == 2) {
        orders_[id] = id2;
        orders2_[id2] = id;
        n++;
      }
    }
    LOG_INFO(name() << ": #" << n << " offline orders loaded");
  }
  of_.open(path.c_str(), std::ofstream::app);
  if (!of_.good()) {
    LOG_FATAL(name() << ": Failed to write file: " << path << ": "
                     << strerror(errno));
  }

  host_ = config("host");
  if (host_.empty()) {
    LOG_FATAL(name() << ": host not given");
  }

  port_ = atoi(config("port").c_str());
  if (!port_) {
    LOG_FATAL(name() << ": port not given");
  }

  auto n = atoi(config("heartbeat_interval").c_str());
  if (n > 0) heartbeat_interval_ = n;
  LOG_INFO(name() << ": heartbeat_interval=" << heartbeat_interval_ << "s");

  n = atoi(config("client_id").c_str());
  if (n > 0) {
    client_id_ = n;
  }
  // client_id 0 is special, which receive all messages sent from the other ids
  LOG_INFO(name() << ": client_id=" << client_id_);

  Connect();
  Heartbeat();
}

void IB::Connect(bool delay) {
  if (delay) {
    if (connected_ == -1) return;
    connected_ = -1;
    reader_tp_.AddTask([this]() { Connect(host_.c_str(), port_, client_id_); },
                       boost::posix_time::seconds(heartbeat_interval_));
    return;
  }
  reader_tp_.AddTask([this]() { Connect(host_.c_str(), port_, client_id_); });
}

void IB::Read() {
  reader_tp_.AddTask([this]() {
    if (!client_->isConnected()) return;
    os_signal_.waitForSignal();
    reader_->processMsgs();
    Read();
  });
}

void IB::Heartbeat() {
  tp_.AddTask([this] {
    if (client_->isConnected()) client_->reqCurrentTime();
  });
  tp_.AddTask(
      [this] {
        if (time(nullptr) - last_heartbeat_tm_ > 2 * heartbeat_interval_) {
          if (client_->isConnected()) {
            LOG_ERROR(name() << ": timeout");
            Reconnect();
          }
        }
        Heartbeat();
      },
      boost::posix_time::seconds(heartbeat_interval_));
}

void IB::Reconnect() noexcept {
  Disconnect();
  Connect();
}

bool IB::Connect(const char* host, unsigned int port, int client_id) {
  LOG_INFO(name() << ": Connecting to " << host << ':' << port
                  << " client_id: " << client_id);

  bool res = client_->eConnect(host, port, client_id, false);

  if (res && client_->isConnected()) {
    last_heartbeat_tm_ = time(nullptr);
    LOG_INFO(name() << ": Connected");
    if (reader_) delete reader_;
    reader_ = new EReader(client_, &os_signal_);
    reader_->start();
    tp_.AddTask([this, client_id]() {
      for (auto id : subs_) {
        auto sec = opentrade::SecurityManager::Instance().Get(id);
        if (!sec) continue;
        Subscribe2(*sec);
      }

      client_->reqOpenOrders();
      ExecutionFilter f;
      f.m_clientId = client_id;
      client_->reqExecutions(++ticker_id_counter_, f);
    });
    Read();
    connected_ = 1;
  } else {
    LOG_ERROR(name() << ": Failed to Connect");
    Connect(true);
  }
  return res;
}

void IB::Disconnect() {
  connected_ = 0;
  if (client_->isConnected()) {
    client_->eDisconnect();
    LOG_DEBUG(name() << ": Disconnect");
  }
}

void IB::connectionClosed() {
  LOG_ERROR(name() << ": Connection closed");
  Disconnect();
  Connect(true);
}

void IB::currentTime(int64_t) { last_heartbeat_tm_ = time(nullptr); }

void IB::error(const int id, const int errorCode,
               const std::string& errorString) {
  if (id > 0) {
    auto id0 = orders2_[id];

    auto eid = std::to_string(id);
    if (id0 == 0) {
      LOG_DEBUG(name() << ": Unknown orderid of error: " << id);
    }
    if (errorCode == 202 || strstr(errorString.c_str(), "Order Canceled")) {
      HandleCanceled(id0, id0, errorString);
    } else if (errorCode == 136 ||
               strstr(errorString.c_str(), "can not be cancelled")) {
      HandleCancelRejected(id0, id0, errorString);
    } else if (errorCode >= 2000 && errorCode < 3000) {
      // warn
    } else if (errorCode != 399) {
      // 399: Order message error, example
      // # <- 2018-05-03 10:46:46.607802 id=2 errorCode=399 errorString=Order
      // Message: BUY 100 USD.CNH Forex Warning: Your ord size is below the
      // USD 25000 IdealPro minimum and will be routed as an odd lot ord.
      HandleNewRejected(id0, errorString);
    }
    io_tp_.AddTask([=]() {
      of_ << "# <- " << opentrade::GetNowStr() << ' ' << "id=" << id << ' '
          << "errorCode=" << errorCode << ' ' << "errorString=" << errorString
          << std::endl;
    });
  } else {
    LOG_ERROR(name() << ": id=" << id << ", errorCode=" << errorCode
                     << ", errorString=" << errorString);
    if (errorCode == 1100 || errorCode == 2110) {
      // errorCode=1100, errorString=Connectivity between IB and Trader
      // Workstation has been lost.
      // errorCode=2110, errorString=Connectivity between Trader Workstation and
      // server is broken. It will be restored automatically
      connected_ = 0;
    } else if (errorCode == 1102) {
      // errorCode=1102, errorString=Connectivity between IB and Trader
      // Workstation has been restored - data maintained.
      connected_ = 1;
    } else if (errorCode == 504) {
      // errorCode=504, errorString=Not connected
      connectionClosed();
    }
  }
}

void IB::execDetails(int reqId, const Contract& contract,
                     const Execution& execution) {
  auto id0 = orders2_[execution.orderId];

  if (id0 == 0) {
    LOG_DEBUG(name() << ": Unknown orderid of execdetails: "
                     << execution.orderId);
  }
  auto tm = GetTime(execution.time.c_str());
  // to-do parse execution.time
  auto eid = std::to_string(execution.orderId);
  auto exec_id = execution.execId;
  // Note if a correction to an execution is published it will be received as an
  // additional IBApi.EWrapper.execDetails callback with all parameters
  // identical except for the execID in the Execution object. The execID will
  // differ only in the digits after the final period. so we remove final digits
  // for the time being before we support correction/bust
  exec_id = exec_id.substr(0, exec_id.find_last_of("."));
  HandleFill(id0, execution.shares, execution.price, exec_id, tm);
  io_tp_.AddTask([=]() {
    of_ << "# <- " << opentrade::GetNowStr() << ' ' << "reqId=" << reqId << ' '
        << "exec_id=" << execution.execId << ' ' << "time=" << execution.time
        << ' ' << "acctNumber=" << execution.acctNumber << ' '
        << "exchange=" << execution.exchange << ' ' << "side=" << execution.side
        << ' ' << "shares=" << execution.shares << ' '
        << "price=" << execution.price << ' ' << "permId=" << execution.permId
        << ' ' << "clientId=" << execution.clientId << ' '
        << "liquidation=" << execution.liquidation << ' '
        << "cumQty=" << execution.cumQty << ' '
        << "avgPrice=" << execution.avgPrice << ' '
        << "orderId=" << execution.orderId << ' '
        << "orderRef=" << execution.orderRef << ' '
        << "evRule=" << execution.evRule << ' '
        << "evMultiplier=" << execution.evMultiplier << ' '
        << "modelCode=" << execution.modelCode << ' '
        << "lastLiquidity=" << execution.lastLiquidity << std::endl;
  });
}

void IB::orderStatus(OrderId orderId, const std::string& status, double filled,
                     double remaining, double avgFillPrice, int permId,
                     int parentId, double lastFillPrice, int clientId,
                     const std::string& whyHeld, double mktCapPrice) {
  io_tp_.AddTask([=]() {
    of_ << "# <- " << opentrade::GetNowStr() << ' ' << "orderId=" << orderId
        << ' ' << "status=" << status << ' ' << "filled=" << filled << ' '
        << "remaining=" << remaining << ' ' << "avgFillPrice=" << avgFillPrice
        << ' ' << "permId=" << permId << ' ' << "parentId=" << parentId << ' '
        << "lastFillPrice=" << lastFillPrice << ' ' << "clientId=" << clientId
        << ' ' << "whyHeld=" << whyHeld << ' ' << "mktCapPrice=" << mktCapPrice
        << std::endl;
  });
}

void IB::openOrder(OrderId orderId, const Contract& contract, const Order& ord,
                   const OrderState& orderState) {
  auto eid = std::to_string(orderId);
  auto id0 = orders2_[orderId];

  if (id0 == 0) {
    LOG_DEBUG(name() << ": Unknown orderId of openOrder: " << orderId);
  }
  auto status = orderState.status;
  if (status == "Submitted") {
    HandleNew(id0, eid);
  } else if (status == "PreSubmitted") {
    HandlePendingNew(id0, status);
  }
  io_tp_.AddTask([=]() {
    of_ << "# <- " << opentrade::GetNowStr() << ' ' << "orderId=" << orderId
        << ' ' << "symbol=" << contract.symbol << ' '
        << "localSymbol=" << contract.localSymbol << ' '
        << "secType=" << contract.secType << ' '
        << "exchange=" << contract.exchange << ' ' << "action=" << ord.action
        << ' ' << "orderType=" << ord.orderType << ' '
        << "totalQuantity=" << ord.totalQuantity << ' ' << "status=" << status
        << std::endl;
  });
}

std::string IB::Place(const opentrade::Order& ord) noexcept {
  auto contract = CreateContract(*ord.sec);

  auto ib_ord = std::make_shared<Order>();
  switch (ord.type) {
    case opentrade::kMarket:
      ib_ord->orderType = "MKT";
      break;
    case opentrade::kStop:
      ib_ord->orderType = "STP";
      ib_ord->auxPrice = ord.stop_price;
      break;
    case opentrade::kStopLimit:
      ib_ord->orderType = "STP LMT";
      ib_ord->auxPrice = ord.stop_price;
      ib_ord->lmtPrice = ord.price;
      break;
    default:
      ib_ord->orderType = "LMT";
      ib_ord->lmtPrice = ord.price;
      break;
  }
  ib_ord->totalQuantity = ord.qty;
  ib_ord->action = ord.IsBuy() ? "BUY" : "SELL";
  auto id = ord.id;
  auto id2 = next_valid_id_++;
  orders_[id] = id2;
  orders2_[id2] = id;
  tp_.AddTask([=]() { client_->placeOrder(id2, *contract, *ib_ord); });
  io_tp_.AddTask([=]() {
    of_ << id << ' ' << id2 << '\n'
        << "# -> " << opentrade::GetNowStr() << ' ' << "id=" << id2 << ' '
        << "secType=" << contract->secType << ' '
        << "symbol=" << contract->symbol << ' '
        << "localSymbol=" << contract->localSymbol << ' '
        << "exchange=" << contract->exchange << ' '
        << "currency=" << contract->currency << ' '
        << "orderType=" << ib_ord->orderType << ' '
        << "lmtPrice=" << ib_ord->lmtPrice << ' '
        << "auxPrice=" << ib_ord->auxPrice << ' '
        << "totalQuantity=" << ib_ord->totalQuantity << ' '
        << "action=" << ib_ord->action << std::endl;
  });

  return {};
}

void IB::nextValidId(OrderId orderId) {
  next_valid_id_ = orderId;
  LOG_INFO(name() << ": nextValidId=" << next_valid_id_);
}

std::string IB::Cancel(const opentrade::Order& ord) noexcept {
  auto id = ord.orig_id;
  uint32_t id2;
  auto it = orders_.find(id);
  if (it == orders_.end()) return "Original IB order id not found";
  id2 = it->second;

  tp_.AddTask([=]() {
    client_->cancelOrder(id2);
    io_tp_.AddTask([this, id2]() {
      of_ << "# -> " << opentrade::GetNowStr() << ' ' << "Cancel " << id2
          << std::endl;
    });
  });
  return {};
}

void IB::Subscribe2(const opentrade::Security& sec) {
  LOG_DEBUG(name() << ": reqMktData " << sec.symbol << ' ' << sec.id);
  auto c = CreateContract(sec);
  auto ticker = ++ticker_id_counter_;
  /*
  // snapshot
  client_->reqMktData(ticker, *c, "", true, false, TagValueListSPtr{});
  tickers_[ticker] = sec.id;
  ticker = ++ticker_id_counter_;
  */
  client_->reqMktData(ticker, *c, "", false, false, TagValueListSPtr{});
  tickers_[ticker] = &sec;
}

void IB::Subscribe(const opentrade::Security& sec) noexcept {
  if (!subs_.insert(sec.id).second) return;
  if (!client_->isConnected()) return;
  tp_.AddTask([&sec, this]() { Subscribe2(sec); });
}

// https://interactivebrokers.github.io/tws-api/tick_types.html
void IB::tickPrice(TickerId tickerId, TickType field, double price,
                   const TickAttrib& attribs) {
  if (price < 0) return;
  auto sec = tickers_[tickerId];
  if (!sec) return;
  auto sec_id = sec->id;
  switch (field) {
    case 1:  // Bid Price
      UpdateBidPrice(sec_id, price);
      if (sec->type == opentrade::kForexPair) {
        UpdateMidAsLastPrice(sec_id);
      }
      break;

    case 2:  // Ask Price
      UpdateAskPrice(sec_id, price);
      if (sec->type == opentrade::kForexPair) {
        UpdateMidAsLastPrice(sec_id);
      }
      break;

    case 4:  // Last Price
      UpdateLastPrice(sec_id, price);
      break;

    case 6:   // High Price
    case 7:   // Low Price
    case 9:   // Close Price
    case 14:  // Open
    default:
      break;
  }
}

void IB::tickSize(TickerId tickerId, TickType field, int size) {
  if (size < 0) return;
  auto sec = tickers_[tickerId];
  if (!sec) return;
  auto sec_id = sec->id;
  switch (field) {
    case 0:  // Bid Size
      UpdateBidSize(sec_id, size);
      break;

    case 3:  // Ask Size
      UpdateAskSize(sec_id, size);
      break;

    case 5:  // Last Size
      UpdateLastSize(sec_id, size);
      break;

    case 8:  // Volume
    default:
      break;
  }
}

extern "C" {
opentrade::Adapter* create() { return new IB{}; }
}
