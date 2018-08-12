#include "market_data.h"

#include "algo.h"
#include "logger.h"
#include "utility.h"

namespace opentrade {

inline MarketDataAdapter* MarketDataManager::GetRoute(const Security& sec,
                                                      DataSrc::IdType src) {
  auto it = routes_.find(std::make_pair(src, sec.exchange->id));
  return it == routes_.end() ? default_
                             : it->second[sec.id % it->second.size()];
}

MarketDataAdapter* MarketDataManager::Subscribe(const Security& sec,
                                                DataSrc::IdType src) {
  auto adapter = GetRoute(sec, src);
  adapter->Subscribe(sec);
  return adapter;
}

const MarketData& MarketDataManager::Get(const Security& sec,
                                         DataSrc::IdType src) {
  auto adapter = GetRoute(sec, src);
  auto md = adapter->md_;
  auto it = md->find(sec.id);
  if (it == md->end()) {
    adapter->Subscribe(sec);
    return (*md)[sec.id];
  }
  return it->second;
}

const MarketData& MarketDataManager::Get(Security::IdType id,
                                         DataSrc::IdType src) {
  auto it = md_of_src_.find(src);
  static const MarketData kMd{};
  if (it == md_of_src_.end()) return kMd;
  return it->second[id];
}

void MarketDataManager::Add(MarketDataAdapter* adapter) {
  AdapterManager<MarketDataAdapter>::Add(adapter);

  if (!default_) default_ = adapter;
  auto src = adapter->config("src");
  if (!src.empty()) {
    LOG_INFO(adapter->name() << " src=" << src)
  }
  if (src.size() > 4) {
    LOG_FATAL("Invalid market data src: " << src << ", maximum length is 4");
  }
  auto src_id = DataSrc::GetId(src.c_str());
  auto markets = adapter->config("markets");
  if (markets.empty()) markets = adapter->config("exchanges");
  adapter->md_ = &md_of_src_[src_id];
  adapter->src_ = src_id;
  for (auto& tok : Split(markets, ",;")) {
    auto orig = tok;
    boost::to_upper(tok);
    boost::algorithm::trim(tok);
    auto e = SecurityManager::Instance().GetExchange(tok);
    if (!e) {
      LOG_WARN("Unknown market name: " << orig << ", ignored");
      continue;
    }
    routes_[std::make_pair(src_id, e->id)].push_back(adapter);
  }
}

void MarketDataAdapter::Update(Security::IdType id, const MarketData::Quote& q,
                               uint32_t level) {
  if (level >= 5) return;
  (*md_)[id].depth[level] = q;
  if (level) return;
  auto& x = AlgoManager::Instance();
  if (!x.IsSubscribed(src_, id)) return;
  x.Update(src_, id);
}

void MarketDataAdapter::Update(Security::IdType id, double price, double size,
                               bool is_bid, uint32_t level) {
  if (level >= 5) return;
  auto& q = (*md_)[id].depth[level];
  if (is_bid) {
    q.bid_price = price;
    q.bid_size = size;
  } else {
    q.ask_price = price;
    q.ask_size = size;
  }
  if (level) return;
  auto& x = AlgoManager::Instance();
  if (!x.IsSubscribed(src_, id)) return;
  x.Update(src_, id);
}

static inline void UpdatePx(double px, MarketData::Trade* t) {
  if (!t->open) t->open = px;
  if (px > t->high) t->high = px;
  if (px < t->low || !t->low) t->low = px;
  t->close = px;
}

static inline void UpdateVolume(double qty, MarketData::Trade* t) {
  t->vwap = (t->volume * t->vwap + t->close * qty) / (t->volume + qty);
  t->volume += qty;
  t->qty = qty;
}

void MarketDataAdapter::Update(Security::IdType id, double last_price,
                               double last_qty) {
  auto& md = (*md_)[id];
  md.tm = time(nullptr);
  auto& t = md.trade;
  if (last_price > 0) UpdatePx(last_price, &t);
  if (last_qty > 0) UpdateVolume(last_qty, &t);
  auto& x = AlgoManager::Instance();
  if (!x.IsSubscribed(src_, id)) return;
  x.Update(src_, id);
}

void MarketDataAdapter::UpdateAskPrice(Security::IdType id, double v) {
  auto& md = (*md_)[id];
  md.tm = time(nullptr);
  md.depth[0].ask_price = v;
  auto& x = AlgoManager::Instance();
  if (!x.IsSubscribed(src_, id)) return;
  x.Update(src_, id);
}

void MarketDataAdapter::UpdateAskSize(Security::IdType id, double v) {
  auto& md = (*md_)[id];
  md.tm = time(nullptr);
  md.depth[0].ask_size = v;
  auto& x = AlgoManager::Instance();
  if (!x.IsSubscribed(src_, id)) return;
  x.Update(src_, id);
}

void MarketDataAdapter::UpdateBidPrice(Security::IdType id, double v) {
  auto& md = (*md_)[id];
  md.tm = time(nullptr);
  md.depth[0].bid_price = v;
  auto& x = AlgoManager::Instance();
  if (!x.IsSubscribed(src_, id)) return;
  x.Update(src_, id);
}

void MarketDataAdapter::UpdateBidSize(Security::IdType id, double v) {
  auto& md = (*md_)[id];
  md.tm = time(nullptr);
  md.depth[0].bid_size = v;
  auto& x = AlgoManager::Instance();
  if (!x.IsSubscribed(src_, id)) return;
  x.Update(src_, id);
}

void MarketDataAdapter::UpdateLastPrice(Security::IdType id, double v) {
  if (v <= 0) return;
  auto& md = (*md_)[id];
  md.tm = time(nullptr);
  UpdatePx(v, &md.trade);
  auto& x = AlgoManager::Instance();
  if (!x.IsSubscribed(src_, id)) return;
  x.Update(src_, id);
}

void MarketDataAdapter::UpdateLastSize(Security::IdType id, double v) {
  if (v <= 0) return;
  auto& md = (*md_)[id];
  md.tm = time(nullptr);
  UpdateVolume(v, &md.trade);
  auto& x = AlgoManager::Instance();
  if (!x.IsSubscribed(src_, id)) return;
  x.Update(src_, id);
}

void MarketDataAdapter::UpdateMidAsLastPrice(Security::IdType id) {
  auto& md = (*md_)[id];
  auto& q = md.quote();
  auto& t = md.trade;
  if (q.ask_price > q.bid_price && q.bid_price > 0) {
    auto px = (q.ask_price + q.bid_price) / 2;
    UpdatePx(px, &t);
    md.tm = time(nullptr);
    auto& x = AlgoManager::Instance();
    if (!x.IsSubscribed(src_, id)) return;
    x.Update(src_, id);
  }
}

}  // namespace opentrade
