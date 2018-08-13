#ifndef OPENTRADE_POSITION_H_
#define OPENTRADE_POSITION_H_

#include <soci.h>
#include <tbb/concurrent_unordered_map.h>
#include <boost/unordered_map.hpp>
#include <fstream>
#include <string>

#include "account.h"
#include "common.h"
#include "order.h"
#include "security.h"
#include "utility.h"

namespace opentrade {

struct Position : public PositionValue {
  double qty = 0;
  double avg_price = 0;
  double unrealized_pnl = 0;
  double realized_pnl = 0;

  double total_bought_qty = 0;
  double total_sold_qty = 0;
  double total_outstanding_buy_qty = 0;
  double total_outstanding_sell_qty = 0;

  void HandleNew(bool is_buy, double qty, double price, double multiplier);
  void HandleTrade(bool is_buy, double qty, double price, double price0,
                   double multiplier, bool is_bust, bool is_otc);
  void HandleFinish(bool is_buy, double leaves_qty, double price0,
                    double multiplier);
};

struct Bod {
  double qty = 0;
  double avg_price = 0;
  double realized_pnl = 0;
  time_t tm = 0;
  BrokerAccount::IdType broker_account_id = 0;
};

class PositionManager : public Singleton<PositionManager> {
 public:
  static void Initialize();
  auto session() { return session_; }
  void Handle(Confirmation::Ptr cm, bool offline);
  const Position& Get(const SubAccount& acc, const Security& sec) const {
    return FindInMap(sub_positions_, std::make_pair(acc.id, sec.id));
  }
  const Position& Get(const BrokerAccount& acc, const Security& sec) const {
    return FindInMap(broker_positions_, std::make_pair(acc.id, sec.id));
  }
  const Position& Get(const User& user, const Security& sec) const {
    return FindInMap(user_positions_, std::make_pair(user.id, sec.id));
  }
  void UpdatePnl();

 private:
  // holding the sql session exclusively for position update
  std::unique_ptr<soci::session> sql_;
  boost::unordered_map<std::pair<SubAccount::IdType, Security::IdType>, Bod>
      bods_;
  tbb::concurrent_unordered_map<std::pair<SubAccount::IdType, Security::IdType>,
                                Position>
      sub_positions_;
  tbb::concurrent_unordered_map<
      std::pair<BrokerAccount::IdType, Security::IdType>, Position>
      broker_positions_;
  tbb::concurrent_unordered_map<std::pair<User::IdType, Security::IdType>,
                                Position>
      user_positions_;
  struct Pnl {
    double realized = 0;
    double unrealized = 0;
    std::ofstream* of = nullptr;
  };
  tbb::concurrent_unordered_map<SubAccount::IdType, Pnl> pnls_;
  std::string session_;
  friend class RiskMananger;
  friend class Connection;
};

}  // namespace opentrade

#endif  // OPENTRADE_POSITION_H_
