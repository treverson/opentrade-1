#ifndef MD_BPIPE_BPIPE_H_
#define MD_BPIPE_BPIPE_H_

#include <blpapi_name.h>
#include <blpapi_service.h>
#include <blpapi_session.h>
#include <blpapi_sessionoptions.h>
#include <tbb/concurrent_unordered_map.h>
#include <tbb/concurrent_unordered_set.h>
#include <atomic>

#include "opentrade/market_data.h"
#include "opentrade/task_pool.h"

namespace bbg = BloombergLP::blpapi;

class BPIPE : public opentrade::MarketDataAdapter, public bbg::EventHandler {
 public:
  void Start() noexcept override;
  void Reconnect() noexcept override;
  void Subscribe(const opentrade::Security& sec) noexcept override;

 protected:
  bool processEvent(const bbg::Event& evt, bbg::Session* session) override;
  void OnConnect();

  void ProcessSessionStatus(const bbg::Event& evt);
  void ProcessTokenStatus(const bbg::Event& evt);
  void ProcessSubscriptionData(const bbg::Event& evt);
  void ProcessResponse(const bbg::Event& evt);
  void LogEvent(const bbg::Event& evt);
  void Subscribe2(const opentrade::Security& sec);
  void UpdateQuote(const bbg::Message& msg, const opentrade::Security& sec,
                   const bbg::Name& ask_name, const bbg::Name& bid_name,
                   const bbg::Name& ask_sz_name, const bbg::Name& bid_sz_name,
                   int level);

 private:
  bbg::SessionOptions options_;
  bbg::Session* session_ = nullptr;
  bbg::Identity identity_;
  std::atomic<int64_t> ticker_counter_ = 0;
  bbg::Service auth_service_;
  tbb::concurrent_unordered_set<opentrade::Security::IdType> subs_;
  tbb::concurrent_unordered_map<int64_t, const opentrade::Security*> tickers_;
  opentrade::TaskPool tp_;
  int reconnect_interval_ = 5;
  bool depth_ = false;
};

#endif  // MD_BPIPE_BPIPE_H_
