#include "position.h"

#include <postgresql/soci-postgresql.h>
#include <boost/date_time/posix_time/posix_time.hpp>
#include <boost/filesystem.hpp>
#include <fstream>
#include <mutex>

#include "database.h"
#include "logger.h"
#include "task_pool.h"

namespace fs = boost::filesystem;
namespace pt = boost::posix_time;

namespace opentrade {

inline void HandlePnl(double qty, double price, double multiplier,
                      Position* p) {
  const auto qty0 = p->qty;
  auto& realized_pnl = p->realized_pnl;
  auto& avg_price = p->avg_price;
  if ((qty0 > 0) && (qty < 0)) {  // sell trade to cover position
    if (qty0 > -qty) {
      realized_pnl += (price - avg_price) * -qty * multiplier;
    } else {
      realized_pnl += (price - avg_price) * qty0 * multiplier;
      avg_price = price;
    }
  } else if ((qty0 < 0) && (qty > 0)) {  // buy trade to cover position
    if (-qty0 > qty) {
      realized_pnl += (avg_price - price) * qty * multiplier;
    } else {
      realized_pnl += (avg_price - price) * -qty0 * multiplier;
      avg_price = price;
    }
  } else {  // open position
    avg_price = (qty0 * avg_price + qty * price) / (qty0 + qty);
  }
}

inline void Position::HandleTrade(bool is_buy, double qty, double price,
                                  double price0, double multiplier,
                                  bool is_bust, bool is_otc) {
  assert(qty > 0);
  PositionValue::HandleTrade(is_buy, qty, price, price0, multiplier, is_bust,
                             is_otc);
  if (!is_buy) qty = -qty;
  if (is_otc) {
    // do nothing
  } else if (!is_bust) {
    if (qty > 0) {
      total_outstanding_buy_qty -= qty;
      total_bought_qty += qty;
    } else {
      total_outstanding_sell_qty -= -qty;
      total_sold_qty += -qty;
    }
  } else {
    if (qty > 0) {
      total_bought_qty -= qty;
    } else {
      total_sold_qty -= -qty;
    }
  }

  if (is_bust) qty = -qty;
  HandlePnl(qty, price, multiplier, this);
  this->qty += qty;
}

inline void Position::HandleFinish(bool is_buy, double leaves_qty,
                                   double price0, double multiplier) {
  assert(leaves_qty);
  if (is_buy) {
    total_outstanding_buy_qty -= leaves_qty;
  } else {
    total_outstanding_sell_qty -= leaves_qty;
  }
  PositionValue::HandleFinish(is_buy, leaves_qty, price0, multiplier);
}

inline void Position::HandleNew(bool is_buy, double qty, double price,
                                double multiplier) {
  assert(qty > 0);
  if (is_buy) {
    total_outstanding_buy_qty -= qty;
  } else {
    total_outstanding_sell_qty -= qty;
  }
  PositionValue::HandleNew(is_buy, qty, price, multiplier);
}

void PositionManager::Initialize() {
  Instance().sql_ = Database::Session();

  auto& self = Instance();
  auto sql = Database::Session();

  auto tm = pt::to_tm(pt::second_clock::universal_time());
  auto path = fs::path(".") / "store" / "session";
  std::ifstream ifs(path.c_str());
  char buf[256] = {0};
  if (ifs.good()) {
    ifs.read(buf, sizeof(buf) - 1);
    auto ptime = pt::time_from_string(buf);
    tm = pt::to_tm(ptime);
    strftime(buf, sizeof(buf), "%Y-%m-%d %H:%M:%S", &tm);
    self.session_ = buf;
  } else {
    std::ofstream ofs(path.c_str(), std::ofstream::trunc);
    if (!ofs.good()) {
      LOG_FATAL("failed to write file '" << path << "' : " << strerror(errno));
    }
    strftime(buf, sizeof(buf), "%Y-%m-%d %H:%M:%S", &tm);
    self.session_ = buf;
    ofs.write(buf, strlen(buf));
    LOG_INFO("Created new session");
  }
  LOG_INFO("Session time: " << buf << " UTC");
  LOG_INFO("Loading BOD from database");

  auto query = R"(
    select distinct on (sub_account_id, security_id)
      sub_account_id, broker_account_id, security_id,
      qty, avg_price, realized_pnl, tm
    from position
    where tm < :tm
    order by sub_account_id, security_id, id desc
  )";
  soci::rowset<soci::row> st = (sql->prepare << query, soci::use(tm));
  for (auto it = st.begin(); it != st.end(); ++it) {
    Position p{};
    auto i = 0;
    auto sub_account_id = Database::GetValue(*it, i++, 0);
    auto broker_account_id = Database::GetValue(*it, i++, 0);
    auto security_id = Database::GetValue(*it, i++, 0);
    auto sec = SecurityManager::Instance().Get(security_id);
    if (!sec) continue;
    p.qty = Database::GetValue(*it, i++, 0.);
    p.avg_price = Database::GetValue(*it, i++, 0.);
    p.realized_pnl = Database::GetValue(*it, i++, 0.);
    Bod bod{};
    bod.qty = p.qty;
    bod.avg_price = p.avg_price;
    bod.realized_pnl = p.realized_pnl;
    bod.broker_account_id = broker_account_id;
    tm = Database::GetValue(*it, i++, tm);
    bod.tm = mktime(&tm);
    self.bods_.emplace(std::make_pair(sub_account_id, security_id), bod);
    self.sub_positions_.emplace(std::make_pair(sub_account_id, security_id), p);
    auto& p2 =
        self.broker_positions_[std::make_pair(broker_account_id, security_id)];
    p2.realized_pnl += p.realized_pnl;
    HandlePnl(p.qty, p.avg_price, sec->multiplier * sec->rate, &p2);
    p2.qty += p.qty;
  }
}

static TaskPool kDatabaseTaskPool;

void PositionManager::Handle(Confirmation::Ptr cm, bool offline) {
  auto ord = cm->order;
  auto sec = ord->sec;
  auto multiplier = sec->rate * sec->multiplier;
  bool is_buy = ord->IsBuy();
  auto is_otc = ord->type == kOTC;
  assert(cm && ord->id > 0);
  static std::mutex kMutex;
  std::lock_guard<std::mutex> lock(kMutex);
  switch (cm->exec_type) {
    case kPartiallyFilled:
    case kFilled: {
      bool is_bust;
      if (cm->exec_trans_type == kTransNew)
        is_bust = false;
      else if (cm->exec_trans_type == kTransCancel)
        is_bust = true;
      else
        return;
      auto qty = cm->last_shares;
      auto px = cm->last_px;
      auto px0 = ord->price;
      auto& pos = sub_positions_[std::make_pair(ord->sub_account->id, sec->id)];
      pos.HandleTrade(is_buy, qty, px, px0, multiplier, is_bust, is_otc);
      broker_positions_[std::make_pair(ord->broker_account->id, sec->id)]
          .HandleTrade(is_buy, qty, px, px0, multiplier, is_bust, is_otc);
      user_positions_[std::make_pair(ord->user->id, sec->id)].HandleTrade(
          is_buy, qty, px, px0, multiplier, is_bust, is_otc);
      const_cast<SubAccount*>(ord->sub_account)
          ->position_value.HandleTrade(is_buy, qty, px, px0, multiplier,
                                       is_bust, is_otc);
      const_cast<BrokerAccount*>(ord->broker_account)
          ->position_value.HandleTrade(is_buy, qty, px, px0, multiplier,
                                       is_bust, is_otc);
      const_cast<User*>(ord->user)->position_value.HandleTrade(
          is_buy, qty, px, px0, multiplier, is_bust, is_otc);
      if (offline) return;
      kDatabaseTaskPool.AddTask([this, pos, cm]() {
        try {
          static User::IdType user_id;
          static SubAccount::IdType sub_account_id;
          static Security::IdType security_id;
          static BrokerAccount::IdType broker_account_id;
          static double qty;
          static double avg_price;
          static double realized_pnl;
          static std::string desc;
          static const char* cmd = R"(
            insert into position(user_id, sub_account_id, security_id, 
            broker_account_id, qty, avg_price, realized_pnl, tm, "desc") 
            values(:user_id, :sub_account_id, :security_id, :broker_account_id,
            :qty, :avg_price, :realized_pnl, now() at time zone 'utc', :desc)
        )";
          static soci::statement st =
              (this->sql_->prepare << cmd, soci::use(user_id),
               soci::use(sub_account_id), soci::use(security_id),
               soci::use(broker_account_id), soci::use(qty),
               soci::use(avg_price), soci::use(realized_pnl), soci::use(desc));
          auto ord = cm->order;
          user_id = ord->user->id;
          sub_account_id = ord->sub_account->id;
          security_id = ord->sec->id;
          broker_account_id = ord->broker_account->id;
          qty = pos.qty;
          avg_price = pos.avg_price;
          realized_pnl = pos.realized_pnl;
          static std::stringstream os;
          os.str("");
          os << std::setprecision(15) << "tm=" << cm->transaction_time
             << ",qty=" << cm->last_shares << ",px=" << cm->last_px
             << ",side=" << static_cast<char>(ord->side)
             << ",type=" << static_cast<char>(ord->type) << ",id=" << ord->id;
          if (cm->exec_trans_type == kTransCancel) os << ",bust=1";
          desc = os.str();
          st.execute(true);
        } catch (const soci::postgresql_soci_error& e) {
          LOG_FATAL("Trying update position to database: \n"
                    << e.sqlstate() << ' ' << e.what());
        } catch (const soci::soci_error& e) {
          LOG_FATAL("Trying update position to database: \n" << e.what());
        }
      });
    } break;
    case kUnconfirmedNew:
      if (!is_otc) {
        auto qty = ord->qty;
        auto px = ord->price;
        sub_positions_[std::make_pair(ord->sub_account->id, sec->id)].HandleNew(
            is_buy, qty, px, multiplier);
        broker_positions_[std::make_pair(ord->broker_account->id, sec->id)]
            .HandleNew(is_buy, qty, px, multiplier);
        user_positions_[std::make_pair(ord->user->id, sec->id)].HandleNew(
            is_buy, qty, px, multiplier);
        const_cast<SubAccount*>(ord->sub_account)
            ->position_value.HandleNew(is_buy, qty, px, multiplier);
        const_cast<BrokerAccount*>(ord->broker_account)
            ->position_value.HandleNew(is_buy, qty, px, multiplier);
        const_cast<User*>(ord->user)->position_value.HandleNew(is_buy, qty, px,
                                                               multiplier);
      }
      break;
    case kRiskRejected:
    case kCanceled:
    case kRejected:
    case kExpired:
    case kCalculated:
    case kDoneForDay: {
      auto qty = cm->leaves_qty;
      auto px = ord->price;
      sub_positions_[std::make_pair(ord->sub_account->id, sec->id)]
          .HandleFinish(is_buy, qty, px, multiplier);
      broker_positions_[std::make_pair(ord->broker_account->id, sec->id)]
          .HandleFinish(is_buy, qty, px, multiplier);
      user_positions_[std::make_pair(ord->user->id, sec->id)].HandleFinish(
          is_buy, qty, px, multiplier);
      const_cast<SubAccount*>(ord->sub_account)
          ->position_value.HandleFinish(is_buy, qty, px, multiplier);
      const_cast<BrokerAccount*>(ord->broker_account)
          ->position_value.HandleFinish(is_buy, qty, px, multiplier);
      const_cast<User*>(ord->user)->position_value.HandleFinish(is_buy, qty, px,
                                                                multiplier);
    } break;
    default:
      break;
  }
}

static TaskPool kPnlTaskPool;

void PositionManager::UpdatePnl() {
  auto tm = time(nullptr);
  std::map<SubAccount::IdType, std::pair<double, double>> pnls;
  auto& sm = SecurityManager::Instance();
  for (auto& pair : sub_positions_) {
    auto acc = pair.first.first;
    auto sec_id = pair.first.second;
    auto& pos = pair.second;
    auto& pnl = pnls[acc];
    pnl.first += pos.realized_pnl;
    if (!pos.qty && !pos.unrealized_pnl) continue;
    auto sec = sm.Get(sec_id);
    if (!sec) continue;
    auto px = sec->CurrentPrice();
    if (!px) continue;
    pos.unrealized_pnl = pos.qty * (px - pos.avg_price);
    pnl.second += pos.unrealized_pnl;
  }
  for (auto& pair : pnls) {
    auto& pnl = pnls_[pair.first];
    if (std::abs(pnl.realized - pair.second.first) < 1 &&
        std::abs(pnl.unrealized - pair.second.second) < 1)
      continue;
    pnl.realized = pair.second.first;
    pnl.unrealized = pair.second.second;
    if (!pnl.of) {
      auto path =
          fs::path(".") / "store" / ("pnl-" + std::to_string(pair.first));
      pnl.of = new std::ofstream(path.c_str(), std::ofstream::app);
    }
    (*pnl.of) << tm << ' ' << pnl.realized << ' ' << pnl.unrealized
              << std::endl;
  }

  kPnlTaskPool.AddTask([this]() { this->UpdatePnl(); }, pt::seconds(5));
}

}  // namespace opentrade
