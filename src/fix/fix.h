#ifndef FIX_FIX_H_
#define FIX_FIX_H_

#include <fstream>
#include <memory>
#include <optional>
#define throw(...)
#include <quickfix/MessageCracker.h>
#include <quickfix/NullStore.h>
#include <quickfix/Session.h>
#include <quickfix/ThreadedSocketInitiator.h>
#undef throw

#include "filelog.h"
#include "filestore.h"
#include "opentrade/exchange_connectivity.h"
#include "opentrade/logger.h"
#include "opentrade/utility.h"

namespace opentrade {

class Fix : public FIX::Application,
            public FIX::MessageCracker,
            public ExchangeConnectivityAdapter {
 public:
  void Start() noexcept override {
    auto config_file = config("config_file");
    if (config_file.empty()) LOG_FATAL(name() << ": config_file not given");
    if (!std::ifstream(config_file.c_str()).good())
      LOG_FATAL(name() << ": Faield to open: " << config_file);

    fix_settings_.reset(new FIX::SessionSettings(config_file));
    if (empty_store_)
      fix_store_factory_.reset(new FIX::NullStoreFactory());
    else
      fix_store_factory_.reset(new FIX::AsyncFileStoreFactory(*fix_settings_));
    fix_log_factory_.reset(new FIX::AsyncFileLogFactory(*fix_settings_));
    threaded_socket_initiator_.reset(new FIX::ThreadedSocketInitiator(
        *this, *fix_store_factory_, *fix_settings_, *fix_log_factory_));
    threaded_socket_initiator_->start();
  }

  void onCreate(const FIX::SessionID& session_id) override {
    if (!session_) session_ = FIX::Session::lookupSession(session_id);
  }

  void onLogon(const FIX::SessionID& session_id) override {
    if (session_ != FIX::Session::lookupSession(session_id)) return;
    connected_ = -1;
    // in case frequently reconnected, e.g. seqnum mismatch,
    // OnLogout is called immediately after OnLogon
    tp_.AddTask(
        [=]() {
          if (-1 == connected_) {
            connected_ = 1;
            LOG_INFO(name() << ": Logged-in to " << session_id.toString());
          }
        },
        boost::posix_time::seconds(1));
  }

  void onLogout(const FIX::SessionID& session_id) override {
    if (session_ != FIX::Session::lookupSession(session_id)) return;
    if (connected())
      LOG_INFO(name() << ": Logged-out from " << session_id.toString());
    connected_ = 0;
  }

  void toApp(FIX::Message& msg, const FIX::SessionID& session_id) override {
    if (msg.isSetField(FIX::FIELD::PossDupFlag)) {
      FIX::PossDupFlag flag;
      msg.getHeader().getField(flag);
      if (flag) throw FIX::DoNotSend();
    }
  }

  void fromApp(const FIX::Message& msg,
               const FIX::SessionID& session_id) override {
    crack(msg, session_id);
  }

  void fromAdmin(const FIX::Message&, const FIX::SessionID&) override {}

  void toAdmin(FIX::Message& msg, const FIX::SessionID& id) override {
    FIX::MsgType msg_type;
    msg.getHeader().getField(msg_type);
    if (msg_type == FIX::MsgType_Logon) {
      try {
        auto username = fix_settings_->get(id).getString("Username");
        if (!username.empty())
          msg.getHeader().setField(FIX::Username(username));
      } catch (...) {
      }
      try {
        auto password = fix_settings_->get(id).getString("Password");
        if (!password.empty())
          msg.getHeader().setField(FIX::Password(password));
      } catch (...) {
      }
    }
  }

  void UpdateTm(const FIX::Message& msg) {
    if (msg.isSetField(FIX::FIELD::TransactTime)) {
      auto t = FIX::UtcTimeStampConvertor::convert(
          (msg.getField(FIX::FIELD::TransactTime)));
      transact_time_ = t.getTimeT() * 1000000l + t.getMillisecond() * 1000;
    } else {
      transact_time_ = NowUtcInMicro();
    }
  }

  void OnExecutionReport(const FIX::Message& msg,
                         const FIX::SessionID& session_id) {
    UpdateTm(msg);
    std::string text;
    if (msg.isSetField(FIX::FIELD::Text)) text = msg.getField(FIX::FIELD::Text);
    auto exec_type = msg.getField(FIX::FIELD::ExecType)[0];
    switch (exec_type) {
      case FIX::ExecType_PENDING_NEW:
        OnPendingNew(msg, text);
        break;
      case FIX::ExecType_PENDING_CANCEL:
        OnPendingCancel(msg);
        break;
      case FIX::ExecType_NEW:
      case FIX::ExecType_SUSPENDED:
        OnNew(msg);
        break;
      case FIX::ExecType_PARTIAL_FILL:
      case FIX::ExecType_FILL:
      case FIX::ExecType_TRADE:
        OnFilled(msg, exec_type, exec_type == FIX::ExecType_PARTIAL_FILL);
        break;
      case FIX::ExecType_PENDING_REPLACE:
        break;
      case FIX::ExecType_CANCELED:
        OnCanceled(msg, text);
        break;
      case FIX::ExecType_REPLACED:
        OnReplaced(msg, text);
        break;
      case FIX::ExecType_REJECTED:
        OnRejected(msg, text);
        break;
      case FIX::ExecType_RESTATED:
        break;
      case FIX::ExecType_TRADE_CANCEL:
        // FIX4.4 to-do
        break;
      case FIX::ExecType_TRADE_CORRECT:
        // FIX4.4 to-do
        break;
      default:
        break;
    }
  }

  void OnNew(const FIX::Message& msg) {
    Order::IdType clordid = atol(msg.getField(FIX::FIELD::ClOrdID).c_str());
    std::string order_id;
    if (msg.isSetField(FIX::FIELD::OrderID))
      order_id = msg.getField(FIX::FIELD::OrderID);
    HandleNew(clordid, order_id, transact_time_);
  }

  void OnPendingNew(const FIX::Message& msg, const std::string& text) {
    Order::IdType clordid = atol(msg.getField(FIX::FIELD::ClOrdID).c_str());
    HandlePendingNew(clordid, text, transact_time_);
  }

  void OnFilled(const FIX::Message& msg, char exec_type, bool is_partial) {
    char exec_trans_type = msg.getField(FIX::FIELD::ExecTransType)[0];
    if (exec_trans_type == FIX::ExecTransType_CORRECT) {
      LOG_WARN(name() << ": Ignoring FIX::ExecTransType_CORRECT");
      return;
    }
    auto exec_id = msg.getField(FIX::FIELD::ExecID);
    auto last_shares = atoll(msg.getField(FIX::FIELD::LastShares).c_str());
    auto last_px = atof(msg.getField(FIX::FIELD::LastPx).c_str());
    auto clordid = atol(msg.getField(FIX::FIELD::ClOrdID).c_str());
    HandleFill(clordid, last_shares, last_px, exec_id, transact_time_,
               is_partial, static_cast<ExecTransType>(exec_trans_type));
  }

  void OnCanceled(const FIX::Message& msg, const std::string& text) {
    Order::IdType orig_id = 0;
    if (msg.isSetField(FIX::FIELD::OrigClOrdID))
      orig_id = atol(msg.getField(FIX::FIELD::OrigClOrdID).c_str());
    Order::IdType clordid = 0;
    if (msg.isSetField(FIX::FIELD::ClOrdID))
      clordid = atol(msg.getField(FIX::FIELD::ClOrdID).c_str());
    HandleCanceled(clordid, orig_id, text, transact_time_);
  }

  void OnPendingCancel(const FIX::Message& msg) {
    Order::IdType orig_id = 0;
    if (msg.isSetField(FIX::FIELD::OrigClOrdID))
      orig_id = atol(msg.getField(FIX::FIELD::OrigClOrdID).c_str());
    Order::IdType clordid = 0;
    if (msg.isSetField(FIX::FIELD::ClOrdID))
      clordid = atol(msg.getField(FIX::FIELD::ClOrdID).c_str());
    HandlePendingCancel(clordid, orig_id, transact_time_);
  }

  void OnReplaced(const FIX::Message& msg, const std::string& text) {
    // to-do
  }

  void OnRejected(const FIX::Message& msg, const std::string& text) {
    Order::IdType clordid = atol(msg.getField(FIX::FIELD::ClOrdID).c_str());
    HandleNewRejected(clordid, text, transact_time_);
  }

  // Cancel or Replace msg got rejected
  void OnCancelRejected(const FIX::Message& msg, const FIX::SessionID&) {
    FIX::CxlRejResponseTo rejResponse;
    msg.getField(rejResponse);
    switch (rejResponse) {
      case FIX::CxlRejResponseTo_ORDER_CANCEL_REQUEST:
        break;
      case FIX::CxlRejResponseTo_ORDER_CANCEL_REPLACE_REQUEST:
      default:
        return;  // to-do: replace rejected
        break;
    }

    Order::IdType orig_id = 0;
    if (msg.isSetField(FIX::FIELD::OrigClOrdID))
      orig_id = atol(msg.getField(FIX::FIELD::OrigClOrdID).c_str());
    Order::IdType clordid = 0;
    if (msg.isSetField(FIX::FIELD::ClOrdID))
      clordid = atol(msg.getField(FIX::FIELD::ClOrdID).c_str());
    UpdateTm(msg);
    std::string text;
    if (msg.isSetField(FIX::FIELD::Text)) text = msg.getField(FIX::FIELD::Text);
    HandleCancelRejected(clordid, orig_id, text, transact_time_);
  }

  void SetTags(const Order& ord, FIX::Message* msg) {
    if (!ord.orig_id) {  // not cancel
      if (ord.type != kMarket) {
        msg->setField(FIX::Price(ord.price));
      }
      if (ord.stop_price) msg->setField(FIX::StopPx(ord.stop_price));
      msg->setField(FIX::TimeInForce(ord.tif));
    } else {
      msg->setField(FIX::OrigClOrdID(std::to_string(ord.orig_id)));
    }

    msg->setField(FIX::HandlInst('1'));
    msg->setField(FIX::OrderQty(ord.qty));
    msg->setField(FIX::ClOrdID(std::to_string(ord.id)));
    msg->setField(FIX::Side(ord.side));
    if (ord.side == kShort) msg->setField(FIX::LocateReqd(false));
    msg->setField(FIX::TransactTime());
    msg->setField(FIX::OrdType(ord.type));

    if (ord.sec->type == kOption) {
      msg->setField(FIX::PutOrCall(ord.sec->put_or_call));
      msg->setField(FIX::OptAttribute('A'));
      msg->setField(FIX::StrikePrice(ord.sec->strike_price));
    }
  }

  bool Send(FIX::Message* msg) { return session_->send(*msg); }

 protected:
  std::unique_ptr<FIX::SessionSettings> fix_settings_;
  std::unique_ptr<FIX::MessageStoreFactory> fix_store_factory_;
  std::unique_ptr<FIX::LogFactory> fix_log_factory_;
  std::unique_ptr<FIX::ThreadedSocketInitiator> threaded_socket_initiator_;
  FIX::Session* session_ = nullptr;

  int64_t transact_time_ = 0;
  TaskPool tp_;
  bool empty_store_ = false;
};  // namespace opentrade

}  // namespace opentrade

#endif  // FIX_FIX_H_
