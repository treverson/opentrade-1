#ifndef IB_IB_H_
#define IB_IB_H_

#include "jts/DefaultEWrapper.h"
#include "jts/EClientSocket.h"
#include "jts/EReader.h"
#include "jts/EReaderOSSignal.h"
#include "opentrade/exchange_connectivity.h"
#include "opentrade/market_data.h"
#include "opentrade/security.h"
#include "opentrade/task_pool.h"

#include <tbb/concurrent_unordered_map.h>
#include <tbb/concurrent_unordered_set.h>
#include <atomic>
#include <fstream>
#include <string>

class IB : public opentrade::ExchangeConnectivityAdapter,
           public opentrade::MarketDataAdapter,
           public DefaultEWrapper {
 public:
  IB();
  ~IB();
  void Start() noexcept override;
  void Reconnect() noexcept override;
  std::string Place(const opentrade::Order& ord) noexcept override;
  std::string Cancel(const opentrade::Order& ord) noexcept override;
  bool connected() const noexcept override {
    return 1 == connected_ && next_valid_id_ > 0 && client_->isConnected();
  }
  void Subscribe(const opentrade::Security& sec) noexcept override;

 protected:
  void orderStatus(OrderId orderId, const std::string& status, double filled,
                   double remaining, double avgFillPrice, int permId,
                   int parentId, double lastFillPrice, int clientId,
                   const std::string& whyHeld, double mktCapPrice) override;
  void openOrder(OrderId orderId, const Contract&, const Order&,
                 const OrderState&) override;
  void openOrderEnd() override {}
  void winError(const std::string& str, int lastError) override {}
  void connectionClosed() override;
  void nextValidId(OrderId orderId) override;
  void execDetails(int reqId, const Contract& contract,
                   const Execution& execution) override;
  void execDetailsEnd(int reqId) override {}
  void error(const int id, const int errorCode,
             const std::string& errorString) override;
  void tickPrice(TickerId tickerId, TickType field, double price,
                 const TickAttrib& attribs) override;
  void tickSize(TickerId tickerId, TickType field, int size) override;
  void currentTime(int64_t time) override;

  bool Connect(const char* host, unsigned int port, int client_Id);
  void Disconnect();
  void Connect(bool delay = false);
  void Read();
  void Heartbeat();
  void Subscribe2(const opentrade::Security& sec);

  EReaderOSSignal os_signal_ = 2000;  // 2-seconds timeout
  EClientSocket* const client_ = nullptr;
  EReader* reader_ = nullptr;
  TickerId ticker_id_counter_ = 0;

  std::string host_;
  int port_ = 0;
  std::ofstream of_;
  opentrade::TaskPool tp_;
  opentrade::TaskPool io_tp_;
  opentrade::TaskPool reader_tp_;
  int heartbeat_interval_ = 5;
  time_t last_heartbeat_tm_ = 0;
  int client_id_ = 1;
  std::atomic<uint32_t> next_valid_id_ = 0;
  tbb::concurrent_unordered_map<uint32_t, uint32_t> orders_;
  tbb::concurrent_unordered_map<uint32_t, uint32_t> orders2_;
  tbb::concurrent_unordered_map<TickerId, const opentrade::Security*> tickers_;
  tbb::concurrent_unordered_set<opentrade::Security::IdType> subs_;
};

#endif  // IB_IB_H_
