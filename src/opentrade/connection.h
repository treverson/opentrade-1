#ifndef OPENTRADE_CONNECTION_H_
#define OPENTRADE_CONNECTION_H_

#include <boost/asio.hpp>
#include <boost/unordered_map.hpp>
#include <map>
#include <memory>
#include <unordered_map>

#include "account.h"
#include "algo.h"
#include "market_data.h"
#include "order.h"
#include "security.h"

namespace opentrade {

struct Transport {
  typedef std::shared_ptr<Transport> Ptr;
  virtual void Send(const std::string& msg) = 0;
  virtual std::string GetAddress() const = 0;
};

class Connection : public std::enable_shared_from_this<Connection> {
 public:
  typedef std::shared_ptr<Connection> Ptr;
  Connection(Transport::Ptr transport,
             std::shared_ptr<boost::asio::io_service> service);
  ~Connection();
  void OnMessage(const std::string&);
  void Send(Confirmation::Ptr cm);
  void Send(const Algo& algo, const std::string& status,
            const std::string& body, uint32_t seq);
  void Close() { closed_ = true; }

 protected:
  void PublishMarketdata();
  void PublishMarketStatus();
  void Send(const std::string& msg) {
    if (!closed_) transport_->Send(msg);
  }
  void Send(const Confirmation& cm, bool offline);
  void Send(Algo::IdType id, time_t tm, const std::string& token,
            const std::string& name, const std::string& status,
            const std::string& body, uint32_t seq, bool offline);
  std::string GetAddress() const { return transport_->GetAddress(); }

 private:
  Transport::Ptr transport_;
  const User* user_ = nullptr;
  std::unordered_map<Security::IdType, std::pair<MarketData, uint32_t>> subs_;
  boost::asio::strand strand_;
  boost::asio::deadline_timer timer_;
  std::map<std::string, bool> ecs_;
  std::map<std::string, bool> mds_;
  std::map<SubAccount::IdType, std::pair<double, double>> pnls_;
  boost::unordered_map<std::pair<SubAccount::IdType, Security::IdType>,
                       std::pair<double, double>>
      single_pnls_;
  bool sub_pnl_ = false;
  bool closed_ = false;
  friend class Server;
  friend class AlgoManager;
  friend class GlobalOrderBook;
};

}  // namespace opentrade

#endif  // OPENTRADE_CONNECTION_H_
