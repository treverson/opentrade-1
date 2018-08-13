#ifndef OPENTRADE_MARKET_DATA_H_
#define OPENTRADE_MARKET_DATA_H_

#include <tbb/concurrent_unordered_map.h>
#include <map>
#include <string>

#include "adapter.h"
#include "security.h"
#include "utility.h"

namespace opentrade {

struct MarketData {
  time_t tm = 0;
  struct Trade {
    double open = 0;
    double high = 0;
    double low = 0;
    double close = 0;
    double qty = 0;
    double vwap = 0;
    double volume = 0;

    bool operator!=(const Trade& b) const {
      return volume != b.volume || close != b.close || high != b.high ||
             low != b.low;
    }
  };

  struct Quote {
    double ask_price = 0;
    double ask_size = 0;
    double bid_price = 0;
    double bid_size = 0;

    bool operator!=(const Quote& b) const {
      return ask_price != b.ask_price || ask_size != b.ask_size ||
             bid_price != b.bid_price || bid_size != b.bid_size;
    }
  };

  static const size_t kDepthSize = 5;
  typedef Quote Depth[kDepthSize];

  const Quote& quote() const { return depth[0]; }

  Trade trade;
  Depth depth;
};

struct DataSrc {
  typedef uint32_t IdType;

  static constexpr IdType GetId(const char* src) {
    if (!src || !*src) return 0;
    return strlen(src) == 1 ? *src : (GetId(src + 1) << 8) + *src;
  }

  static const char* GetStr(IdType id) {
    static thread_local char str[5];
    auto i = 0u;
    for (i = 0u; i < 4 && id; ++i) {
      str[i] = id & 0xFF;
      id >>= 8;
    }
    str[i] = 0;
    return str;
  }
};

class MarketDataAdapter : public virtual NetworkAdapter {
 public:
  typedef tbb::concurrent_unordered_map<Security::IdType, MarketData>
      MarketDataMap;
  virtual void Subscribe(const Security& sec) noexcept = 0;
  DataSrc::IdType src() const { return src_; }
  void Update(Security::IdType id, const MarketData::Quote& q,
              uint32_t level = 0);
  void Update(Security::IdType id, double price, double size, bool is_bid,
              uint32_t level = 0);
  void Update(Security::IdType id, double last_price, double last_qty);
  void UpdateMidAsLastPrice(Security::IdType id);
  void UpdateAskPrice(Security::IdType id, double v);
  void UpdateAskSize(Security::IdType id, double v);
  void UpdateBidPrice(Security::IdType id, double v);
  void UpdateBidSize(Security::IdType id, double v);
  void UpdateLastPrice(Security::IdType id, double v);
  void UpdateLastSize(Security::IdType id, double v);

 protected:
  MarketDataMap* md_;

 private:
  DataSrc::IdType src_ = 0;
  friend class MarketDataManager;
};

class MarketDataManager : public AdapterManager<MarketDataAdapter>,
                          public Singleton<MarketDataManager> {
 public:
  MarketDataAdapter* Subscribe(const Security& sec, DataSrc::IdType src);
  void Add(MarketDataAdapter* adapter);
  const MarketData& Get(Security::IdType id, DataSrc::IdType src = 0);
  const MarketData& Get(const Security& sec, DataSrc::IdType src = 0);

 private:
  MarketDataAdapter* GetRoute(const Security& sec, DataSrc::IdType src);

 private:
  std::map<DataSrc::IdType, MarketDataAdapter::MarketDataMap> md_of_src_;
  MarketDataAdapter* default_;
  std::map<std::pair<DataSrc::IdType, Exchange::IdType>,
           std::vector<MarketDataAdapter*>>
      routes_;
};

}  // namespace opentrade

#endif  // OPENTRADE_MARKET_DATA_H_
