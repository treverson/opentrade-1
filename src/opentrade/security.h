#ifndef OPENTRADE_SECURITY_H_
#define OPENTRADE_SECURITY_H_

#include <tbb/concurrent_unordered_map.h>
#include <string>

#include "common.h"
#include "utility.h"

namespace opentrade {

struct Exchange {
  typedef uint16_t IdType;
  IdType id = 0;
  const char* name = "";
  const char* mic = "";
  const char* bb_name = "";
  const char* ib_name = "";
  const char* country = "";
  const char* tz = "";
  bool odd_lot_allowed = false;
  int utc_time_offset = 0;
  const char* desc = "";
  struct TickSizeTuple {
    double lower_bound = 0;
    double upper_bound = 0;
    double value = 0;
    bool operator<(const TickSizeTuple& b) const {
      return lower_bound < b.lower_bound;
    }
  };
  typedef std::vector<TickSizeTuple> TickSizeTable;
  double GetTickSize(double ref) const;
  const TickSizeTable* tick_size_table = nullptr;
  int trade_start = 0;  // seconds since midnight
  int trade_end = 0;
  int break_start = 0;
  int break_end = 0;

  int GetTime() const {  // seconds since midnight in exchange time zone
    return GetUtcSinceMidNight(utc_time_offset);
  }
  bool IsInTradePeriod() const {
    auto t = GetTime();
    return (break_start <= 0 || (t > break_start && t < break_end)) &&
           (trade_start <= 0 || (t > trade_start && t < trade_end));
  }
};

// follow IB
// https://interactivebrokers.github.io/tws-api/classIBApi_1_1Contract.html
inline static const std::string kStock = "STK";
inline static const std::string kForexPair = "CASH";
inline static const std::string kCommodity = "CMDTY";
inline static const std::string kFuture = "FUT";
inline static const std::string kOption = "OPT";
inline static const std::string kIndex = "IND";
inline static const std::string kFutureOption = "FOP";
inline static const std::string kCombo = "BAG";
inline static const std::string KWarrant = "WAR";
inline static const std::string kBond = "BOND";
inline static const std::string kMutualFund = "FUND";
inline static const std::string kNews = "NEWS";

struct Security {
  typedef uint32_t IdType;
  IdType id = 0;
  const char* symbol = "";
  const char* local_symbol = "";
  const char* type = "";
  const char* currency = "";
  const char* bbgid = "";
  const char* cusip = "";
  const char* isin = "";
  const char* sedol = "";
  const Exchange* exchange = nullptr;
  const Security* underlying = nullptr;
  double rate = 1;
  double multiplier = 1;
  double tick_size = 0;
  double close_price = 0;
  double adv20 = 0;
  double market_cap = 0;
  int lot_size = 0;
  int sector = 0;
  int industry_group = 0;
  int industry = 0;
  int sub_industry = 0;
  double strike_price = 0;
  int maturity_date = 0;
  bool put_or_call = 0;
  char opt_attribute = 0;

  double CurrentPrice() const;
  double GetTickSize(double px) const {
    if (tick_size > 0) return tick_size;
    return exchange->GetTickSize(px);
  }
  bool IsInTradePeriod() const { return exchange->IsInTradePeriod(); }
};

class SecurityManager : public Singleton<SecurityManager> {
 public:
  static void Initialize();
  const char* check_sum() const { return check_sum_; }
  const Security* GetSecurity(Security::IdType id) {
    return FindInMap(securities_, id);
  }
  const Exchange* GetExchange(Exchange::IdType id) {
    return FindInMap(exchanges_, id);
  }
  const Exchange* GetExchange(const std::string& name) {
    return FindInMap(exchange_of_name_, name);
  }

  typedef tbb::concurrent_unordered_map<Security::IdType, Security*>
      SecurityMap;
  const SecurityMap& securities() const { return securities_; }
  void LoadFromDatabase();

 protected:
  void UpdateCheckSum();

 private:
  tbb::concurrent_unordered_map<Exchange::IdType, Exchange*> exchanges_;
  tbb::concurrent_unordered_map<std::string, Exchange*> exchange_of_name_;
  SecurityMap securities_;
  const char* check_sum_;
};

}  // namespace opentrade

#endif  // OPENTRADE_SECURITY_H_
