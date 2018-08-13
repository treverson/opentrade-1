#ifndef OPENTRADE_ACCOUNT_H_
#define OPENTRADE_ACCOUNT_H_

#include <tbb/concurrent_unordered_map.h>
#include <string>
#include <unordered_map>

#include "common.h"
#include "security.h"
#include "utility.h"

namespace opentrade {

class ExchangeConnectivityAdapter;

struct AccountBase {
  Limits limits;
  Throttle throttle_in_sec;
  tbb::concurrent_unordered_map<Security::IdType, Throttle>
      throttle_per_security_in_sec;
  PositionValue position_value;
};

struct BrokerAccount : public AccountBase {
  typedef uint16_t IdType;
  IdType id = 0;
  const char* name = "";
  const char* adapter_name = "";
  ExchangeConnectivityAdapter* adapter = nullptr;
  typedef std::unordered_map<std::string, std::string> StrMap;
  const StrMap* params = new StrMap();

  void set_params(const std::string& params);
};

struct SubAccount : public AccountBase {
  typedef uint16_t IdType;
  IdType id = 0;
  const char* name = "";
  typedef std::unordered_map<Exchange::IdType, BrokerAccount*> BrokerAccountMap;
  const BrokerAccountMap* broker_accounts = new BrokerAccountMap();
};

struct User : public AccountBase {
  typedef uint16_t IdType;
  IdType id = 0;
  const char* name = "";
  const char* password = "";
  bool is_admin = false;
  bool is_disabled = false;
  typedef std::unordered_map<SubAccount::IdType, SubAccount*> SubAccountMap;
  const SubAccountMap* sub_accounts = new SubAccountMap();
};

struct UserSubAccountMap {
  User::IdType user_id = 0;
  SubAccount::IdType sub_account_id = 0;
};

struct SubAccountBrokerAccountMap {
  SubAccount::IdType sub_account_id = 0;
  Exchange::IdType exchange_id = 0;
  BrokerAccount::IdType broker_account_id = 0;
};

class AccountManager : public Singleton<AccountManager> {
 public:
  static void Initialize();
  const User* GetUser(const std::string& name) {
    return FindInMap(user_of_name_, name);
  }
  const User* GetUser(User::IdType id) { return FindInMap(users_, id); }
  const SubAccount* GetSubAccount(SubAccount::IdType id) {
    return FindInMap(sub_accounts_, id);
  }
  const SubAccount* GetSubAccount(const std::string& name) {
    return FindInMap(sub_account_of_name_, name);
  }
  const BrokerAccount* GetBrokerAccount(BrokerAccount::IdType id) {
    return FindInMap(broker_accounts_, id);
  }

 private:
  tbb::concurrent_unordered_map<User::IdType, User*> users_;
  tbb::concurrent_unordered_map<std::string, User*> user_of_name_;
  tbb::concurrent_unordered_map<SubAccount::IdType, SubAccount*> sub_accounts_;
  tbb::concurrent_unordered_map<std::string, SubAccount*> sub_account_of_name_;
  tbb::concurrent_unordered_map<BrokerAccount::IdType, BrokerAccount*>
      broker_accounts_;
  friend class Connection;
};

}  // namespace opentrade

#endif  // OPENTRADE_ACCOUNT_H_
