#ifndef OPENTRADE_RISK_H_
#define OPENTRADE_RISK_H_

#include <string>

#include "order.h"
#include "utility.h"

namespace opentrade {

inline thread_local std::string kRiskError;

class RiskManager : public Singleton<RiskManager> {
 public:
  bool Check(const Order& ord);
  bool CheckMsgRate(const Order& ord);
};

}  // namespace opentrade

#endif  // OPENTRADE_RISK_H_
