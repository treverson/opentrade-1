#include <tbb/concurrent_unordered_set.h>
#include <boost/unordered_map.hpp>
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <ctime>
#include <fstream>
#include <memory>
#include <thread>
#include <unordered_map>
#include <vector>

#define throw(...)
#include <quickfix/NullStore.h>
#include <quickfix/Session.h>
#include <quickfix/ThreadedSocketAcceptor.h>
#undef throw

#include "core/logger.h"
#include "core/market_data.h"
#include "core/security.h"
#include "core/task_pool.h"
#include "fix/filelog.h"

using Security = opentrade::Security;

static thread_local boost::uuids::random_generator kUuidGen;

class SimServer : public opentrade::MarketDataAdapter, public FIX::Application {
 public:
  void Start() noexcept override;
  void Subscribe(const Security& sec) noexcept override;
  void onCreate(const FIX::SessionID& session_id) override {
    if (!session_) session_ = FIX::Session::lookupSession(session_id);
  }
  void fromApp(const FIX::Message& msg,
               const FIX::SessionID& session_id) override;
  void onLogon(const FIX::SessionID& session_id) override {}
  void onLogout(const FIX::SessionID& session_id) override {}
  void toApp(FIX::Message& msg, const FIX::SessionID& session_id) override {}
  void toAdmin(FIX::Message& msg, const FIX::SessionID& id) override {}
  void fromAdmin(const FIX::Message&, const FIX::SessionID&) override {}

 private:
  std::unique_ptr<FIX::SessionSettings> fix_settings_;
  std::unique_ptr<FIX::MessageStoreFactory> fix_store_factory_;
  std::unique_ptr<FIX::LogFactory> fix_log_factory_;
  std::unique_ptr<FIX::ThreadedSocketAcceptor> threaded_socket_acceptor_;
  FIX::Session* session_ = nullptr;
  struct OrderTuple {
    double px = 0;
    double leaves = 0;
    bool is_buy = false;
    FIX::Message resp;
  };
  std::unordered_map<Security::IdType,
                     std::unordered_map<std::string, OrderTuple>>
      active_orders_;
  boost::unordered_map<std::pair<std::string, std::string>, const Security*>
      sec_of_name_;
  opentrade::TaskPool tp_;
  tbb::concurrent_unordered_set<Security::IdType> subs_;
  tbb::concurrent_unordered_set<std::string> used_ids_;
};

void SimServer::Start() noexcept {
  auto bbgid_file = config("bbgid_file");
  if (bbgid_file.empty()) {
    LOG_FATAL(name() << ": bbgid_file not given");
  }

  auto ticks_file = config("ticks_file");
  if (ticks_file.empty()) {
    LOG_FATAL(name() << ": ticks_file not given");
  }

  std::unordered_map<std::string, const Security*> sec_map;
  for (auto& pair : opentrade::SecurityManager::Instance().securities()) {
    sec_map[pair.second->bbgid] = pair.second;
    sec_of_name_[std::make_pair(std::string(pair.second->symbol),
                                std::string(pair.second->exchange->name))] =
        pair.second;
  }

  std::string line;
  std::vector<const Security*> secs;
  std::ifstream ifs(bbgid_file.c_str());
  if (!ifs.good()) {
    LOG_FATAL(name() << ": Can not open " << bbgid_file);
  }
  while (std::getline(ifs, line)) {
    auto sec = sec_map[line];
    secs.push_back(sec);
    if (!sec) {
      LOG_ERROR(name() << ": Unknown bbgid " << line);
      continue;
    }
  }

  if (!std::ifstream(ticks_file.c_str()).good()) {
    LOG_FATAL(name() << ": Can not open " << ticks_file);
  }
  auto config_file = config("config_file");
  if (config_file.empty()) LOG_FATAL(name() << ": config_file not given");
  if (!std::ifstream(config_file.c_str()).good())
    LOG_FATAL(name() << ": Faield to open: " << config_file);

  fix_settings_.reset(new FIX::SessionSettings(config_file));
  fix_store_factory_.reset(new FIX::NullStoreFactory());
  fix_log_factory_.reset(new FIX::AsyncFileLogFactory(*fix_settings_));
  threaded_socket_acceptor_.reset(new FIX::ThreadedSocketAcceptor(
      *this, *fix_store_factory_, *fix_settings_, *fix_log_factory_));
  threaded_socket_acceptor_->start();

  connected_ = 1;

  std::thread thread([=]() {
    while (true) {
      struct tm tm;
      auto t = time(nullptr);
      gmtime_r(&t, &tm);
      auto n = tm.tm_hour * 3600 + tm.tm_min * 60 + tm.tm_sec;
      auto t0 = t - n;
      std::ifstream ifs(ticks_file.c_str());
      std::string line;
      LOG_DEBUG(name() << ": Start to play back");
      auto skip = 0l;
      while (std::getline(ifs, line)) {
        if (skip-- > 0) continue;
        uint32_t hms;
        uint32_t i;
        char type;
        double px;
        double qty;
        if (sscanf(line.c_str(), "%u %u %c %lf %lf", &hms, &i, &type, &px,
                   &qty) != 5)
          continue;
        if (i >= secs.size()) continue;
        t = t0 + hms / 10000 * 3600 + hms % 10000 / 100 * 60 + hms % 100;
        auto now = time(nullptr);
        if (t < now - 3) {
          skip = 1000;
          continue;
        }
        if (now < t) {
          LOG_DEBUG(name() << ": " << hms);
          std::this_thread::sleep_for(std::chrono::seconds(t - now));
        }
        auto sec = secs[i];
        if (!sec) continue;
        switch (type) {
          case 'T': {
            Update(sec->id, px, qty);
            if (!qty && sec->type == opentrade::kForexPair) qty = 1e12;
            if (px > 0 && qty > 0) {
              tp_.AddTask([sec, px, qty, this]() {
                auto size = qty;
                auto& actives = active_orders_[sec->id];
                if (actives.empty()) return;
                auto it = actives.begin();
                while (it != actives.end() && size > 0) {
                  auto& tuple = it->second;
                  auto ok = (tuple.is_buy && px <= tuple.px) ||
                            (!tuple.is_buy && px >= tuple.px);
                  if (!ok) {
                    it++;
                    continue;
                  }
                  auto n = std::min(size, tuple.leaves);
                  size -= n;
                  tuple.leaves -= n;
                  assert(size >= 0);
                  assert(tuple.leaves >= 0);
                  auto& resp = tuple.resp;
                  resp.setField(FIX::ExecTransType('0'));
                  resp.setField(FIX::ExecType(
                      tuple.leaves <= 0 ? FIX::ExecType_FILL
                                        : FIX::ExecType_PARTIAL_FILL));
                  resp.setField(FIX::OrdStatus(
                      tuple.leaves <= 0 ? FIX::ExecType_FILL
                                        : FIX::ExecType_PARTIAL_FILL));
                  resp.setField(FIX::LastShares(n));
                  resp.setField(FIX::LastPx(tuple.px));
                  auto eid = boost::uuids::to_string(kUuidGen());
                  resp.setField(FIX::ExecID(eid));
                  session_->send(resp);
                  if (tuple.leaves <= 0)
                    it = actives.erase(it);
                  else
                    it++;
                }
              });
            }
          } break;
          case 'A':
            if (*sec->exchange->name == 'U') qty *= 100;
            Update(sec->id, px, qty, false);
            break;
          case 'B':
            if (*sec->exchange->name == 'U') qty *= 100;
            Update(sec->id, px, qty, true);
            break;
          default:
            break;
        }
      }
      for (auto& pair : *md_) pair.second = opentrade::MarketData{};
    }
  });
  thread.detach();
}

void SimServer::Subscribe(const Security& sec) noexcept {
  subs_.insert(sec.id);
}

void SimServer::fromApp(const FIX::Message& msg,
                        const FIX::SessionID& session_id) {
  const std::string& msgType = msg.getHeader().getField(FIX::FIELD::MsgType);
  if (msgType == "D") {  // new order
    auto resp = msg;
    resp.getHeader().setField(FIX::MsgType("8"));
    resp.setField(FIX::TransactTime(FIX::UTCTIMESTAMP()));
    auto symbol = msg.getField(FIX::FIELD::Symbol);
    auto exchange = msg.getField(FIX::FIELD::ExDestination);
    auto it = sec_of_name_.find(std::make_pair(symbol, exchange));
    if (it == sec_of_name_.end()) {
      resp.setField(FIX::ExecType(FIX::ExecType_REJECTED));
      resp.setField(FIX::OrdStatus(FIX::ExecType_REJECTED));
      resp.setField(FIX::Text("unknown security"));
      session_->send(resp);
      return;
    }
    auto& sec = *it->second;
    if (!sec.IsInTradePeriod()) {
      resp.setField(FIX::ExecType(FIX::ExecType_REJECTED));
      resp.setField(FIX::OrdStatus(FIX::ExecType_REJECTED));
      resp.setField(FIX::Text("Not in trading period"));
      session_->send(resp);
      return;
    }
    auto qty = atof(msg.getField(FIX::FIELD::OrderQty).c_str());
    if (qty <= 0) {
      resp.setField(FIX::ExecType(FIX::ExecType_REJECTED));
      resp.setField(FIX::OrdStatus(FIX::ExecType_REJECTED));
      resp.setField(FIX::Text("invalid OrderQty"));
      session_->send(resp);
      return;
    }
    auto px = 0.;
    if (msg.isSetField(FIX::FIELD::Price))
      px = atof(msg.getField(FIX::FIELD::Price).c_str());
    FIX::OrdType type;
    msg.getField(type);
    if (px <= 0 && type != FIX::OrdType_MARKET) {
      resp.setField(FIX::ExecType(FIX::ExecType_REJECTED));
      resp.setField(FIX::OrdStatus(FIX::ExecType_REJECTED));
      resp.setField(FIX::Text("invalid price"));
      session_->send(resp);
      return;
    }
    resp.setField(FIX::ExecType(FIX::ExecType_PENDING_NEW));
    resp.setField(FIX::OrdStatus(FIX::ExecType_PENDING_NEW));
    session_->send(resp);
    auto clordid = msg.getField(FIX::FIELD::ClOrdID);
    if (used_ids_.find(clordid) != used_ids_.end()) {
      resp.setField(FIX::ExecType(FIX::ExecType_REJECTED));
      resp.setField(FIX::OrdStatus(FIX::ExecType_REJECTED));
      resp.setField(FIX::Text("duplicate ClOrdID"));
      session_->send(resp);
      return;
    }
    used_ids_.insert(clordid);
    resp.setField(FIX::FIELD::OrderID, "SIM-" + clordid);
    resp.setField(FIX::ExecType(FIX::ExecType_NEW));
    resp.setField(FIX::OrdStatus(FIX::ExecType_NEW));
    session_->send(resp);
    FIX::Side side;
    msg.getField(side);
    auto is_buy = side == FIX::Side_BUY;
    if (type == FIX::OrdType_MARKET) {
      auto q = opentrade::MarketDataManager::Instance().Get(sec).quote();
      auto qty_q = is_buy ? q.ask_size : q.bid_size;
      auto px_q = is_buy ? q.ask_price : q.bid_price;
      if (!qty_q && sec.type == opentrade::kForexPair) qty_q = 1e12;
      if (qty_q > 0 && px_q > 0) {
        if (qty_q > qty) qty_q = qty;
        resp.setField(FIX::ExecTransType('0'));
        resp.setField(FIX::ExecType(qty_q == qty ? FIX::ExecType_FILL
                                                 : FIX::ExecType_PARTIAL_FILL));
        resp.setField(FIX::OrdStatus(
            qty_q == qty ? FIX::ExecType_FILL : FIX::ExecType_PARTIAL_FILL));
        resp.setField(FIX::LastShares(qty_q));
        resp.setField(FIX::LastPx(px_q));
        auto eid = boost::uuids::to_string(kUuidGen());
        resp.setField(FIX::ExecID(eid));
        session_->send(resp);
        if (qty_q >= qty) return;
      }
      resp.setField(FIX::ExecType(FIX::ExecType_CANCELLED));
      resp.setField(FIX::OrdStatus(FIX::ExecType_CANCELLED));
      resp.setField(FIX::Text("no quote"));
      session_->send(resp);
      return;
    }
    tp_.AddTask([=, &sec]() {
      auto resp = msg;
      resp.getHeader().setField(FIX::MsgType("8"));
      resp.setField(FIX::TransactTime(FIX::UTCTIMESTAMP()));
      OrderTuple ord{px, qty, is_buy, resp};
      auto q = opentrade::MarketDataManager::Instance().Get(sec).quote();
      auto qty_q = is_buy ? q.ask_size : q.bid_size;
      auto px_q = is_buy ? q.ask_price : q.bid_price;
      if (!qty_q && sec.type == opentrade::kForexPair) qty_q = 1e12;
      if (qty_q > 0 && px_q > 0) {
        if ((is_buy && px >= px_q) || (!is_buy && px <= px_q)) {
          if (qty_q > qty) qty_q = qty;
          resp.setField(FIX::ExecTransType('0'));
          resp.setField(FIX::ExecType(
              qty_q == qty ? FIX::ExecType_FILL : FIX::ExecType_PARTIAL_FILL));
          resp.setField(FIX::OrdStatus(
              qty_q == qty ? FIX::ExecType_FILL : FIX::ExecType_PARTIAL_FILL));
          resp.setField(FIX::LastShares(qty_q));
          resp.setField(FIX::LastPx(px_q));
          auto eid = boost::uuids::to_string(kUuidGen());
          resp.setField(FIX::ExecID(eid));
          session_->send(resp);
          ord.leaves -= qty_q;
          assert(ord.leaves >= 0);
          if (ord.leaves <= 0) return;
        }
      }
      FIX::TimeInForce tif;
      if (msg.isSetField(FIX::FIELD::TimeInForce)) msg.getField(tif);
      if (tif == FIX::TimeInForce_IMMEDIATE_OR_CANCEL) {
        resp.setField(FIX::ExecType(FIX::ExecType_CANCELLED));
        resp.setField(FIX::OrdStatus(FIX::ExecType_CANCELLED));
        resp.setField(FIX::Text("no quote"));
        session_->send(resp);
        return;
      }
      active_orders_[sec.id][clordid] = ord;
    });
  } else if (msgType == "F") {
    tp_.AddTask([=]() {
      auto resp = msg;
      resp.getHeader().setField(FIX::MsgType("9"));
      resp.setField(FIX::TransactTime(FIX::UTCTIMESTAMP()));
      resp.setField(
          FIX::CxlRejResponseTo(FIX::CxlRejResponseTo_ORDER_CANCEL_REQUEST));
      auto symbol = msg.getField(FIX::FIELD::Symbol);
      auto exchange = msg.getField(FIX::FIELD::ExDestination);
      auto it0 = sec_of_name_.find(std::make_pair(symbol, exchange));
      if (it0 == sec_of_name_.end()) {
        resp.setField(FIX::Text("unknown security"));
        session_->send(resp);
        return;
      }
      auto& actives = active_orders_[it0->second->id];
      auto clordid = msg.getField(FIX::FIELD::ClOrdID);
      if (used_ids_.find(clordid) != used_ids_.end()) {
        resp.setField(FIX::Text("duplicate ClOrdID"));
        session_->send(resp);
        return;
      }
      used_ids_.insert(clordid);
      auto orig = msg.getField(FIX::FIELD::OrigClOrdID);
      auto it = actives.find(orig);
      if (it == actives.end()) {
        resp.setField(FIX::Text("inactive"));
        session_->send(resp);
        return;
      }
      resp = msg;
      resp.getHeader().setField(FIX::MsgType("8"));
      resp.setField(FIX::TransactTime(FIX::UTCTIMESTAMP()));
      resp.setField(FIX::ExecType(FIX::ExecType_CANCELLED));
      resp.setField(FIX::OrdStatus(FIX::ExecType_CANCELLED));
      session_->send(resp);
      actives.erase(it);
    });
  }
}

extern "C" {
opentrade::Adapter* create() { return new SimServer{}; }
}
