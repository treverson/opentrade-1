#include "fix/fix.h"
#include "opentrade/order.h"

#define throw(...)
#include <quickfix/fix42/Allocation.h>
#include <quickfix/fix42/ExecutionReport.h>
#include <quickfix/fix42/NewOrderSingle.h>
#include <quickfix/fix42/OrderCancelReject.h>
#include <quickfix/fix42/OrderCancelReplaceRequest.h>
#include <quickfix/fix42/OrderCancelRequest.h>
#include <quickfix/fix42/OrderStatusRequest.h>
#include <quickfix/fix42/Reject.h>
#include <quickfix/fix42/SettlementInstructions.h>
#undef throw

class SimClient : public opentrade::Fix {
 public:
  SimClient() { empty_store_ = true; }

  void onMessage(const FIX42::ExecutionReport& msg, const FIX::SessionID& id) {
    OnExecutionReport(msg, id);
  }

  void onMessage(const FIX42::OrderCancelReject& msg,
                 const FIX::SessionID& id) {
    OnCancelRejected(msg, id);
  }

  std::string SetAndSend(const opentrade::Order& ord, FIX::Message* msg) {
    SetTags(ord, msg);
    msg->setField(FIX::Symbol(ord.sec->symbol));
    msg->setField(FIX::ExDestination(ord.sec->exchange->name));
    if (Send(msg))
      return {};
    else
      return "Failed in FIX::Session::send()";
  }

  std::string Place(const opentrade::Order& ord) noexcept override {
    FIX42::NewOrderSingle msg;
    return SetAndSend(ord, &msg);
  }

  std::string Cancel(const opentrade::Order& ord) noexcept override {
    FIX42::OrderCancelRequest msg;
    return SetAndSend(ord, &msg);
  }
};

extern "C" {
opentrade::Adapter* create() { return new SimClient{}; }
}
