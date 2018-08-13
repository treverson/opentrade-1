#include "order.h"

#include <boost/filesystem.hpp>
#include <boost/iostreams/device/mapped_file.hpp>
#include <cstring>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <sstream>

#include "algo.h"
#include "connection.h"
#include "exchange_connectivity.h"
#include "logger.h"
#include "position.h"
#include "server.h"
#include "task_pool.h"

namespace fs = boost::filesystem;

namespace opentrade {

static TaskPool kReadTaskPool;
TaskPool kWriteTaskPool;

static auto kPath = fs::path(".") / "store" / "confirmations";

void GlobalOrderBook::Initialize() {
  auto& self = Instance();
  self.of_.open(kPath.c_str(), std::ofstream::app);
  if (!self.of_.good()) {
    LOG_FATAL("Failed to write file: " << kPath.c_str() << ": "
                                       << strerror(errno));
  }
  self.LoadStore();
  LOG_INFO("Got last maximum client order id: " << self.order_id_counter_);
  time_t t = time(NULL);
  struct tm now;
  localtime_r(&t, &now);
  auto secs = now.tm_hour * 3600 + now.tm_min * 60 + now.tm_sec;
  auto min_counter = now.tm_wday * 1e7 + secs * 50;
  self.order_id_counter_ = self.order_id_counter_ + 1e5;
  if (self.order_id_counter_ < min_counter) {
    self.order_id_counter_ = min_counter;
  }
  LOG_INFO("New client order id starts from " << self.order_id_counter_);
  self.seq_counter_ += 1000;
}

inline void GlobalOrderBook::UpdateOrder(Confirmation::Ptr cm) {
  switch (cm->exec_type) {
    case kUnconfirmedNew:
    case kUnconfirmedCancel:
      orders_.emplace(cm->order->id, cm->order);
      break;
    case kPartiallyFilled:
    case kFilled:
      if (cm->exec_trans_type == kTransNew) {
        auto ord = cm->order;
        ord->avg_px =
            (ord->avg_px * ord->cum_qty + cm->last_px * cm->last_shares) /
            (ord->cum_qty + cm->last_shares);
        ord->cum_qty += cm->last_shares;
        ord->leaves_qty -= cm->last_shares;
        if (ord->cum_qty >= ord->qty)
          ord->status = kFilled;
        else if (ord->IsLive())
          ord->status = kPartiallyFilled;
      } else if (cm->exec_trans_type == kTransCancel) {
        auto ord = cm->order;
        if (ord->cum_qty <= cm->last_shares) {
          ord->avg_px = 0;
          ord->cum_qty = 0;
        } else {
          ord->avg_px =
              (ord->avg_px * ord->cum_qty - cm->last_px * cm->last_shares) /
              (ord->cum_qty - cm->last_shares);
          ord->cum_qty -= cm->last_shares;
        }
      } else {
        // to-do
      }
      break;
    case kNew:
    case kPendingNew:
    case kPendingCancel:
      cm->order->status = cm->exec_type;
      break;
    case kRiskRejected:
    case kCanceled:
    case kRejected:
    case kExpired:
    case kCalculated:
    case kDoneForDay:
      cm->leaves_qty = cm->order->leaves_qty;
      cm->order->leaves_qty = 0;
      cm->order->status = cm->exec_type;
      break;
    default:
      break;
  }
}

void GlobalOrderBook::Handle(Confirmation::Ptr cm, bool offline) {
  if (cm->order->id <= 0) {  // risk rejected
    assert(!offline);
    Server::Publish(cm);
    return;
  }
  UpdateOrder(cm);
  PositionManager::Instance().Handle(cm, offline);
  if (cm->order->inst) AlgoManager::Instance().Handle(cm);
  if (offline) return;
  kWriteTaskPool.AddTask([this, cm]() {
    cm->seq = ++seq_counter_;
    Server::Publish(cm);
    std::stringstream ss;
    auto ord = cm->order;
    switch (cm->exec_type) {
      case kNew:
        ss << ord->id << ' ' << cm->transaction_time << ' ' << cm->order_id;
        break;
      case kPartiallyFilled:
      case kFilled:
        ss << std::setprecision(15) << ord->id << ' ' << cm->transaction_time
           << ' ' << cm->last_shares << ' ' << cm->last_px << ' '
           << static_cast<char>(cm->exec_trans_type) << ' ' << cm->exec_id;
        break;
      case kPendingNew:
      case kPendingCancel:
      case kCancelRejected:
      case kCanceled:
      case kRejected:
      case kExpired:
      case kCalculated:
      case kDoneForDay:
        ss << ord->id << ' ' << cm->transaction_time << ' ' << cm->text;
        break;
      case kUnconfirmedNew: {
        ss << std::setprecision(15) << ord->id << ' ' << cm->transaction_time
           << ' ' << ord->algo_id << ' ' << ord->qty << ' ' << ord->price << ' '
           << ord->stop_price << ' ' << static_cast<char>(ord->side) << ' '
           << static_cast<char>(ord->type) << ' ' << static_cast<char>(ord->tif)
           << ' ' << ord->sec->id << ' ' << ord->user->id << ' '
           << ord->broker_account->id;
      } break;
      case kUnconfirmedCancel:
        ss << ord->id << ' ' << cm->transaction_time << ' ' << ord->orig_id;
        break;
      case kRiskRejected:
        ss << ord->id << ' ' << cm->text;
        break;
      default:
        break;
    }
    auto str = ss.str();
    if (str.empty()) return;
    // excluding length and seq and exec_type and ending '\0\n'
    of_.write(reinterpret_cast<const char*>(&cm->seq), sizeof(cm->seq));
    uint16_t n = str.size();
    of_.write(reinterpret_cast<const char*>(&n), sizeof(n));
    of_.write(reinterpret_cast<const char*>(&ord->sub_account->id),
              sizeof(ord->sub_account->id));
    of_ << static_cast<char>(cm->exec_type);
    of_ << str << '\0' << std::endl;
  });
}

void GlobalOrderBook::LoadStore(uint32_t seq0, Connection* conn) {
  if (!fs::file_size(kPath)) return;
  boost::iostreams::mapped_file_source m(kPath.string());
  auto p = m.data();
  auto p_end = p + m.size();
  auto ln = 0;
  while (p + 6 < p_end) {
    ln++;
    auto seq = *reinterpret_cast<const uint32_t*>(p);
    if (!conn) seq_counter_ = seq;
    p += 4;
    auto n = *reinterpret_cast<const uint16_t*>(p);
    if (p + n + 5 + sizeof(SubAccount::IdType) > p_end) break;
    p += 2;
    auto sub_account_id = *reinterpret_cast<const SubAccount::IdType*>(p);
    p += sizeof(SubAccount::IdType);
    auto exec_type = static_cast<opentrade::OrderStatus>(*p);
    p += 1;
    auto body = p;
    p += n + 2;  // body + '\0' + '\n'
    if (seq <= seq0) continue;
    if (conn) {
      assert(conn->user_);
      if (!conn->user_->is_admin &&
          conn->user_->sub_accounts->find(sub_account_id) ==
              conn->user_->sub_accounts->end())
        continue;
    }
    switch (exec_type) {
      case kNew: {
        uint32_t id;
        int64_t tm;
        char id_str[n];
        *id_str = 0;
        if (sscanf(body, "%u %ld %[^\1]s", &id, &tm, id_str) < 2) {
          LOG_ERROR("Failed to parse confirmation line #" << ln);
          continue;
        }
        if (conn) {
          Confirmation cm{};
          cm.seq = seq;
          Order ord{};
          ord.id = id;
          cm.order = &ord;
          cm.exec_type = exec_type;
          cm.transaction_time = tm;
          cm.order_id = id_str;
          conn->Send(cm, true);
          continue;
        }
        auto ord = Get(id);
        if (!ord) {
          LOG_ERROR("Unknown order id " << id << " on confirmation line #"
                                        << ln);
        }
        auto cm = std::make_shared<Confirmation>();
        cm->exec_type = exec_type;
        cm->order = ord;
        cm->transaction_time = tm;
        cm->order_id = id_str;
        Handle(cm, true);
      } break;
      case kPartiallyFilled:
      case kFilled: {
        uint32_t id;
        int64_t tm;
        double last_shares;
        double last_px;
        char exec_trans_type;
        char exec_id[n];
        if (sscanf(body, "%u %ld %lf %lf %c %[^\1]s", &id, &tm, &last_shares,
                   &last_px, &exec_trans_type, exec_id) < 6) {
          LOG_ERROR("Failed to parse confirmation line #" << ln);
          continue;
        }
        if (conn) {
          Confirmation cm{};
          cm.seq = seq;
          Order ord{};
          ord.id = id;
          cm.order = &ord;
          cm.exec_type = exec_type;
          cm.transaction_time = tm;
          cm.last_shares = last_shares;
          cm.last_px = last_px;
          cm.exec_trans_type =
              static_cast<opentrade::ExecTransType>(exec_trans_type);
          cm.exec_id = exec_id;
          conn->Send(cm, true);
          continue;
        }
        if (IsDupExecId(exec_id)) {  // not only double check, but also insert
                                     // into exec_ids_
          LOG_ERROR("Duplicate exec id " << exec_id << " on confirmation line #"
                                         << ln);
          continue;
        }
        auto ord = Get(id);
        if (!ord) {
          LOG_ERROR("Unknown order id " << id << " on confirmation line #"
                                        << ln);
          continue;
        }
        auto cm = std::make_shared<Confirmation>();
        cm->exec_type = exec_type;
        cm->order = ord;
        cm->transaction_time = tm;
        cm->last_shares = last_shares;
        cm->last_px = last_px;
        cm->exec_trans_type =
            static_cast<opentrade::ExecTransType>(exec_trans_type);
        cm->exec_id = exec_id;
        Handle(cm, true);
      } break;
      case kPendingNew:
      case kPendingCancel:
      case kCancelRejected:
      case kCanceled:
      case kRejected:
      case kExpired:
      case kCalculated:
      case kDoneForDay: {
        uint32_t id;
        int64_t tm;
        char text[n];
        *text = 0;
        if (sscanf(body, "%u %ld %[^\1]s", &id, &tm, text) < 2) {
          LOG_ERROR("Failed to parse confirmation line #" << ln);
          continue;
        }
        if (conn) {
          Confirmation cm{};
          cm.seq = seq;
          Order ord{};
          ord.id = id;
          cm.order = &ord;
          cm.exec_type = exec_type;
          cm.transaction_time = tm;
          cm.text = text;
          conn->Send(cm, true);
          continue;
        }
        auto ord = Get(id);
        if (!ord) {
          LOG_ERROR("Unknown order id " << id << " on confirmation line #"
                                        << ln);
          continue;
        }
        auto cm = std::make_shared<Confirmation>();
        cm->exec_type = exec_type;
        cm->order = ord;
        cm->transaction_time = tm;
        cm->text = text;
        Handle(cm, true);
      } break;
      case kUnconfirmedNew: {
        uint32_t id;
        int64_t tm;
        uint32_t algo_id;
        double qty;
        double price;
        double stop_price;
        char side;
        char type;
        char tif;
        uint32_t sec_id;
        uint32_t user_id;
        uint32_t broker_account_id;
        if (sscanf(body, "%u %ld %u %lf %lf %lf %c %c %c %u %u %u", &id, &tm,
                   &algo_id, &qty, &price, &stop_price, &side, &type, &tif,
                   &sec_id, &user_id, &broker_account_id) < 12) {
          LOG_ERROR("Failed to parse confirmation line #" << ln);
          continue;
        }
        if (conn) {
          Confirmation cm{};
          cm.seq = seq;
          Order ord{};
          ord.id = id;
          ord.algo_id = algo_id;
          ord.qty = qty;
          ord.price = price;
          ord.stop_price = stop_price;
          ord.side = static_cast<opentrade::OrderSide>(side);
          ord.type = static_cast<opentrade::OrderType>(type);
          ord.tif = static_cast<opentrade::TimeInForce>(tif);
          Security sec{};
          sec.id = sec_id;
          ord.sec = &sec;
          User user;
          user.id = user_id;
          ord.user = &user;
          SubAccount sub_account;
          sub_account.id = sub_account_id;
          ord.sub_account = &sub_account;
          BrokerAccount broker_account;
          broker_account.id = broker_account_id;
          ord.broker_account = &broker_account;
          cm.order = &ord;
          cm.exec_type = exec_type;
          cm.transaction_time = tm;
          conn->Send(cm, true);
          continue;
        }
        auto sec = SecurityManager::Instance().GetSecurity(sec_id);
        if (!sec) {
          LOG_ERROR("Unknown security id " << sec_id
                                           << " on confirmation line #" << ln);
          continue;
        }
        auto user = AccountManager::Instance().GetUser(user_id);
        if (!user) {
          LOG_ERROR("Unknown user id " << user_id << " on confirmation line #"
                                       << ln);
          continue;
        }
        auto sub_account =
            AccountManager::Instance().GetSubAccount(sub_account_id);
        if (!sub_account) {
          LOG_ERROR("Unknown sub account id "
                    << sub_account_id << " on confirmation line #" << ln);
          continue;
        }
        auto broker_account =
            AccountManager::Instance().GetBrokerAccount(broker_account_id);
        if (!sub_account) {
          LOG_ERROR("Unknown broker account id "
                    << broker_account_id << " on confirmation line #" << ln);
          continue;
        }
        auto ord = new Order{};
        ord->id = id;
        ord->algo_id = algo_id;
        ord->qty = qty;
        ord->leaves_qty = qty;
        ord->price = price;
        ord->stop_price = stop_price;
        ord->side = static_cast<opentrade::OrderSide>(side);
        ord->type = static_cast<opentrade::OrderType>(type);
        ord->tif = static_cast<opentrade::TimeInForce>(tif);
        ord->sec = sec;
        ord->user = user;
        ord->sub_account = sub_account;
        ord->broker_account = broker_account;
        ord->tm = tm;
        auto cm = std::make_shared<Confirmation>();
        cm->exec_type = exec_type;
        cm->order = ord;
        cm->transaction_time = tm;
        Handle(cm, true);
        if (id > order_id_counter_) order_id_counter_ = id;
      } break;
      case kUnconfirmedCancel: {
        uint32_t id;
        int64_t tm;
        uint32_t orig_id;
        if (sscanf(body, "%u %ld %u", &id, &tm, &orig_id) < 3) {
          LOG_ERROR("Failed to parse confirmation line #" << ln);
          continue;
        }
        if (conn) {
          Confirmation cm{};
          cm.seq = seq;
          Order ord{};
          ord.id = id;
          ord.orig_id = orig_id;
          cm.order = &ord;
          cm.exec_type = exec_type;
          cm.transaction_time = tm;
          conn->Send(cm, true);
          continue;
        }
        auto orig_ord = Get(orig_id);
        if (!orig_ord) {
          LOG_ERROR("Unknown orig_id " << orig_id << " on confirmation line #"
                                       << ln);
          continue;
        }
        auto cancel_order = new Order(*orig_ord);
        cancel_order->id = id;
        cancel_order->orig_id = orig_id;
        cancel_order->status = kUnconfirmedCancel;
        cancel_order->tm = tm;
        auto cm = std::make_shared<Confirmation>();
        cm->exec_type = exec_type;
        cm->order = cancel_order;
        cm->transaction_time = tm;
        if (id > order_id_counter_) order_id_counter_ = id;
        Handle(cm, true);
      } break;
      case kRiskRejected: {
        uint32_t id;
        char text[n];
        *text = 0;
        if (sscanf(body, "%u %[^\1]s", &id, text) < 1) {
          LOG_ERROR("Failed to parse confirmation line #" << ln);
          continue;
        }
        if (conn) {
          assert(id > 0);
          Confirmation cm{};
          cm.seq = seq;
          Order ord{};
          ord.id = id;
          cm.order = &ord;
          cm.exec_type = exec_type;
          cm.text = text;
          conn->Send(cm, true);
          continue;
        }
        auto ord = Get(id);
        if (!ord) {
          LOG_ERROR("Unknown order id " << id << " on confirmation line #"
                                        << ln);
          continue;
        }
        auto cm = std::make_shared<Confirmation>();
        cm->exec_type = exec_type;
        cm->order = ord;
        cm->text = text;
        Handle(cm, true);
      } break;
      default:
        break;
    }
  }
  if (!conn && p != p_end) {
    LOG_FATAL("Corrupted confirmation file: " << kPath.c_str()
                                              << ", please fix it first");
  }
}

void GlobalOrderBook::Cancel() {
  for (auto& pair : orders_) {
    auto ord = pair.second;
    if (ord->IsLive()) ExchangeConnectivityManager::Instance().Cancel(*ord);
  }
}

}  // namespace opentrade
