#ifndef OPENTRADE_COMMON_H_
#define OPENTRADE_COMMON_H_

#include <tbb/atomic.h>
#include <cassert>
#include <string>

namespace opentrade {

static inline const std::string kEmptyStr;

template <typename V>
class Singleton {
 public:
  static V& Instance() {
    static V kInstance;
    return kInstance;
  }

 protected:
  Singleton() {}
};

struct Limits {
  double msg_rate = 0;               // per second
  double msg_rate_per_security = 0;  // per security per second
  double order_qty = 0;
  double order_value = 0;
  double value = 0;  // per security
  double turnover = 0;
  double total_value = 0;
  double total_turnover = 0;
};

struct Throttle {
  tbb::atomic<int> n = 0;
  int operator()(int tm) const {
    if (tm != this->tm) return 0;
    return n;
  }
  int tm = 0;

  void Update(int tm2) {
    if (tm2 != tm) {
      n = 0;
      tm2 = tm;
    } else {
      n++;
    }
  }
};

struct PositionValue {
  double total_bought = 0;
  double total_sold = 0;
  double total_outstanding_buy = 0;
  double total_outstanding_sell = 0;

  void HandleNew(bool is_buy, double qty, double price, double multiplier);
  void HandleTrade(bool is_buy, double qty, double price, double price0,
                   double multiplier, bool is_bust, bool is_otc);
  void HandleFinish(bool is_buy, double leaves_qty, double price0,
                    double multiplier);
};

inline void PositionValue::HandleNew(bool is_buy, double qty, double price,
                                     double multiplier) {
  assert(qty > 0);
  auto value = qty * price * multiplier;
  if (is_buy) {
    total_outstanding_buy += value;
  } else {
    total_outstanding_sell += value;
  }
}

inline void PositionValue::HandleTrade(bool is_buy, double qty, double price,
                                       double price0, double multiplier,
                                       bool is_bust, bool is_otc) {
  assert(qty > 0);
  if (!is_buy) qty = -qty;
  auto value = qty * price * multiplier;
  if (is_otc) {
    // do nothing
  } else if (!is_bust) {
    auto value0 = qty * price0 * multiplier;
    if (value > 0) {
      total_outstanding_buy -= value0;
      total_bought += value;
    } else {
      total_outstanding_sell -= -value0;
      total_sold += -value;
    }
  } else {
    if (value > 0) {
      total_bought -= value;
    } else {
      total_sold -= -value;
    }
  }
}

inline void PositionValue::HandleFinish(bool is_buy, double leaves_qty,
                                        double price0, double multiplier) {
  assert(leaves_qty);
  auto value = leaves_qty * price0 * multiplier;
  if (is_buy) {
    total_outstanding_buy -= value;
  } else {
    total_outstanding_sell -= value;
  }
}

}  // namespace opentrade

#endif  // OPENTRADE_COMMON_H_
