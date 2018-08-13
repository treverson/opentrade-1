#include "twap.h"

#include <opentrade/logger.h>

namespace opentrade {

std::string TWAP::OnStart(const ParamMap& params) noexcept {
  SecurityTuple st{};
  st = GetParam(params, "Security", st);
  auto src = std::get<0>(st);
  auto sec = std::get<1>(st);
  acc_ = std::get<2>(st);
  side_ = std::get<3>(st);
  qty_ = std::get<4>(st);
  assert(sec);  // SecurityTuple already verified before onStart
  assert(acc_);
  assert(side_);
  assert(qty_ > 0);

  inst_ = Subscribe(*sec, src);
  auto seconds = GetParam(params, "ValidSeconds", 0);
  if (seconds < 60) return "Too short ValidSeconds, must be >= 60";
  begin_time_ = time(nullptr);
  price_ = GetParam(params, "Price", 0.);
  end_time_ = begin_time_ + seconds;
  min_size_ = GetParam(params, "MinSize", 0);
  if (min_size_ <= 0 && sec->lot_size <= 0) {
    return "MinSize required for security without lot size";
  }
  if (min_size_ > 0 && sec->lot_size > 0) {
    min_size_ = std::round(min_size_ / sec->lot_size) * sec->lot_size;
  }
  max_pov_ = GetParam(params, "MaxPov", 0.0);
  if (max_pov_ > 1) max_pov_ = 1;
  auto agg = GetParam(params, "Aggression", kEmptyStr);
  if (agg == "Low")
    agg_ = kAggLow;
  else if (agg == "Medium")
    agg_ = kAggMedium;
  else if (agg == "High")
    agg_ = kAggHigh;
  else if (agg == "Highest")
    agg_ = kAggHighest;
  else
    return "Invalid aggression, must be in (Low, Medium, High, Highest)";
  Timer();
  LOG_DEBUG('[' << name() << ' ' << id() << "] started");
  return {};
}

void TWAP::OnStop() noexcept {
  LOG_DEBUG('[' << name() << ' ' << id() << "] stopped");
}

void TWAP::OnMarketTrade(const Instrument& inst, const MarketData& md,
                         const MarketData& md0) noexcept {
  auto& t = md.trade;
  LOG_DEBUG(inst.sec().symbol << " trade: " << t.open << ' ' << t.high << ' '
                              << t.low << ' ' << t.close << ' ' << t.qty << ' '
                              << t.vwap << ' ' << t.volume);
  if (initial_volume_ <= 0) initial_volume_ = t.volume;
}

void TWAP::OnMarketQuote(const Instrument& inst, const MarketData& md,
                         const MarketData& md0) noexcept {
  auto& q = md.quote();
  LOG_DEBUG(inst.sec().symbol << " quote: " << q.ask_price << ' ' << q.ask_size
                              << ' ' << q.bid_price << ' ' << q.bid_size);
}

void TWAP::OnConfirmation(const Confirmation& cm) noexcept {
  if (inst_->total_qty() >= qty_) Stop();
}

const ParamDefs& TWAP::GetParamDefs() noexcept {
  static ParamDefs defs{
      {"Security", SecurityTuple{}, true},
      {"Price", 0.0, false, 0, 10000000, 7},
      {"ValidSeconds", 300, true, 60},
      {"MinSize", 0, false, 0, 10000000},
      {"MaxPov", 0.0, false, 0, 1, 2},
      {"Aggression", ParamDef::ValueVector{"Low", "Medium", "High", "Highest"},
       true},
  };
  return defs;
}

void TWAP::Timer() {
  if (!is_active()) return;
  auto now = time(nullptr);
  if (now > end_time_) {
    Stop();
    return;
  }
  SetTimeout([this]() { Timer(); }, 1000);
  if (!inst_->sec().IsInTradePeriod()) return;

  auto md = inst_->md();
  auto bid = md.quote().bid_price;
  auto ask = md.quote().ask_price;
  auto last_px = md.trade.close;
  auto mid_px = 0.;
  if (ask > bid && bid > 0) {
    mid_px = (ask + bid) / 2;
    auto tick_size = inst_->sec().GetTickSize(mid_px);
    if (tick_size > 0) {
      if (IsBuy(side_))
        mid_px = std::ceil(mid_px / tick_size) * tick_size;
      else
        mid_px = std::floor(mid_px / tick_size) * tick_size;
    }
  }

  if (!inst_->active_orders().empty()) {
    for (auto ord : inst_->active_orders()) {
      if (IsBuy(side_)) {
        if (ord->price < bid) {
          Cancel(*ord);
        }
      } else {
        if (ask > 0 && ord->price > ask) {
          Cancel(*ord);
        }
      }
    }
    return;
  }

  if (initial_volume_ > 0 && max_pov_ > 0) {
    if (inst_->total_qty() > max_pov_ * (md.trade.volume - initial_volume_))
      return;
  }

  auto ratio =
      std::min(1., (now - begin_time_ + 1.) / (end_time_ - begin_time_));
  auto expect = qty_ * ratio;
  auto leaves = expect - inst_->total_exposure();
  if (leaves <= 0) return;
  auto total_leaves = qty_ - inst_->total_exposure();
  auto lot_size = std::max(1, inst_->sec().lot_size);
  auto max_qty = inst_->sec().exchange->odd_lot_allowed
                     ? total_leaves
                     : std::floor(total_leaves / lot_size) * lot_size;
  if (max_qty <= 0) return;
  auto would_qty = std::ceil(leaves / lot_size) * lot_size;
  if (would_qty < min_size_) would_qty = min_size_;
  if (would_qty > max_qty) would_qty = max_qty;
  Contract c;
  c.side = side_;
  c.qty = would_qty;
  c.sub_account = acc_;
  switch (agg_) {
    case kAggLow:
      if (IsBuy(side_)) {
        if (bid > 0)
          c.price = bid;
        else if (last_px > 0)
          c.price = last_px;
        else
          return;
      } else {
        if (ask > 0)
          c.price = ask;
        else if (last_px > 0)
          c.price = last_px;
        else
          return;
      }
      break;
    case kAggMedium:
      if (mid_px > 0) {
        c.price = mid_px;
        break;
      }  // else go to kAggHigh
    case kAggHigh:
      if (IsBuy(side_)) {
        if (ask > 0) {
          c.price = ask;
          break;
        }  // else go to kAggHighest
      } else {
        if (bid > 0) {
          c.price = bid;
          break;
        }  // else go to kAggHighest
      }
    case kAggHighest:
    default:
      c.type = kMarket;
      break;
  }
  if (price_ > 0 && ((IsBuy(side_) && c.price > price_) ||
                     (!IsBuy(side_) && c.price < price_)))
    return;
  Place(c, inst_);
}

}  // namespace opentrade

extern "C" {
opentrade::Adapter* create() { return new opentrade::TWAP{}; }
}
