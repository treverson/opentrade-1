#ifndef OPENTRADE_ALGO_H_
#define OPENTRADE_ALGO_H_

#include <tbb/atomic.h>
#include <tbb/concurrent_unordered_map.h>
#include <atomic>
#include <boost/asio.hpp>
#include <boost/unordered_map.hpp>
#include <boost/unordered_set.hpp>
#include <fstream>
#include <list>
#include <mutex>
#include <set>
#include <thread>
#include <tuple>
#include <unordered_map>
#include <variant>
#include <vector>

#include "adapter.h"
#include "market_data.h"
#include "order.h"
#include "security.h"
#include "utility.h"

namespace opentrade {

class Instrument;

typedef std::tuple<DataSrc::IdType, const Security*, const SubAccount*,
                   OrderSide, double>
    SecurityTuple;

struct ParamDef {
  typedef std::variant<std::string, const char*, bool, int64_t, int32_t, double,
                       SecurityTuple>
      ValueScalar;
  typedef std::vector<ValueScalar> ValueVector;
  typedef std::variant<std::string, const char*, bool, int64_t, int32_t, double,
                       SecurityTuple, ValueVector>
      Value;
  std::string name;
  Value default_value;
  bool required = false;
  double min_value = 0;
  double max_value = 0;
  int precision = 0;
};

typedef std::vector<ParamDef> ParamDefs;

class Algo : public Adapter {
 public:
  typedef uint32_t IdType;
  typedef std::unordered_map<std::string, ParamDef::Value> ParamMap;

  Instrument* Subscribe(const Security& sec, DataSrc::IdType src = 0);
  void Stop();
  void SetTimeout(std::function<void()> func, uint32_t milliseconds);
  Order* Place(const Contract& contract, Instrument* inst);
  bool Cancel(const Order& ord);

  virtual std::string OnStart(const ParamMap& params) noexcept = 0;
  virtual void OnStop() noexcept = 0;
  virtual void OnMarketTrade(const Instrument& inst, const MarketData& md,
                             const MarketData& md0) noexcept = 0;
  virtual void OnMarketQuote(const Instrument& inst, const MarketData& md,
                             const MarketData& md0) noexcept = 0;
  virtual void OnConfirmation(const Confirmation& cm) noexcept = 0;
  virtual const ParamDefs& GetParamDefs() noexcept = 0;

  // called only once when app starts, you can load global data shared by all
  // algo instance here.
  void Start() noexcept override {}

  bool is_active() const { return is_active_; }
  IdType id() const { return id_; }
  const std::string& token() const { return token_; }
  const User& user() const { return *user_; }

 private:
  const User* user_ = nullptr;
  bool is_active_ = true;
  IdType id_ = 0;
  std::string token_;
  std::set<Instrument*> instruments_;
  friend class AlgoManager;
};

class Instrument {
 public:
  typedef std::set<Order*> Orders;
  Instrument(Algo* algo, const Security& sec, DataSrc::IdType src)
      : algo_(algo), sec_(sec), src_(src) {}
  Algo& algo() { return *algo_; }
  const Security& sec() const { return sec_; }
  DataSrc::IdType src() const { return src_; }
  const MarketData& md() const { return *md_; }
  const Orders& active_orders() const { return active_orders_; }
  double bought_qty() const { return bought_qty_; }
  double sold_qty() const { return sold_qty_; }
  double outstanding_buy_qty() const { return outstanding_buy_qty_; }
  double outstanding_sell_qty() const { return outstanding_sell_qty_; }
  double net_qty() const { return bought_qty_ - sold_qty_; }
  double total_qty() const { return bought_qty_ + sold_qty_; }
  double net_outstanding_qty() const {
    return outstanding_buy_qty_ - outstanding_sell_qty_;
  }
  double total_outstanding_qty() const {
    return outstanding_buy_qty_ + outstanding_sell_qty_;
  }
  double total_exposure() const {
    return total_qty() + total_outstanding_qty();
  }

 private:
  Algo* algo_ = nullptr;
  const Security& sec_;
  const MarketData* md_ = nullptr;
  const DataSrc::IdType src_;
  Orders active_orders_;
  double bought_qty_ = 0;
  double sold_qty_ = 0;
  double outstanding_buy_qty_ = 0;
  double outstanding_sell_qty_ = 0;
  friend class AlgoManager;
  friend class Algo;
};

class AlgoRunner {
 public:
  void operator()();

 private:
  boost::unordered_map<std::pair<DataSrc::IdType, Security::IdType>,
                       std::pair<MarketData, std::list<Instrument*>>>
      instruments_;
  tbb::concurrent_unordered_map<std::pair<DataSrc::IdType, Security::IdType>,
                                tbb::atomic<uint32_t>>
      md_refs_;
  std::thread::id tid_;
  boost::unordered_set<std::pair<DataSrc::IdType, Security::IdType>> dirties_;
  std::mutex mutex_;
  typedef std::lock_guard<std::mutex> LockGuard;
  friend class AlgoManager;
};

class Connection;

class AlgoManager : public AdapterManager<Algo>, public Singleton<AlgoManager> {
 public:
  static void Initialize();
  Algo* Spawn(std::shared_ptr<Algo::ParamMap> params, const std::string& name,
              const User& user, const std::string& params_raw,
              const std::string& token);
  void Run(int nthreads);
  void Update(DataSrc::IdType src, Security::IdType id);
  void Stop();
  void Stop(Security::IdType id);
  void Stop(const std::string& token);
  void Handle(Confirmation::Ptr cm);
  void SetTimeout(Algo::IdType id, std::function<void()> func,
                  uint32_t milliseconds);
  bool IsSubscribed(DataSrc::IdType src, Security::IdType id) {
    return md_refs_[std::make_pair(src, id)] > 0;
  }
  void Register(Instrument* inst);
  void Persist(const Algo& algo, const std::string& status,
               const std::string& body);
  void LoadStore(uint32_t seq0 = 0, Connection* conn = nullptr);
  Algo* Get(const std::string& token) {
    return FindInMap(algo_of_token_, token);
  }

 private:
  std::atomic<Algo::IdType> algo_id_counter_ = 0;
  tbb::concurrent_unordered_map<Algo::IdType, Algo*> algos_;
  tbb::concurrent_unordered_map<std::string, Algo*> algo_of_token_;
  tbb::concurrent_unordered_map<std::pair<DataSrc::IdType, Security::IdType>,
                                tbb::atomic<uint32_t>>
      md_refs_;
  AlgoRunner* runners_ = nullptr;
  std::vector<std::thread> threads_;
  boost::asio::io_service io_service_;
  std::unique_ptr<boost::asio::io_service::work> work_;
  std::vector<boost::asio::strand> strands_;
  std::ofstream of_;
  uint32_t seq_counter_ = 0;
  friend class AlgoRunner;
};

}  // namespace opentrade

#endif  // OPENTRADE_ALGO_H_
