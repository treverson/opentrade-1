#include "bpipe.h"

#include <blpapi_correlationid.h>
#include <blpapi_element.h>
#include <blpapi_event.h>
#include <blpapi_message.h>
#include <blpapi_request.h>
#include <blpapi_subscriptionlist.h>
#include <blpapi_tlsoptions.h>

#include "opentrade/logger.h"

static const bbg::Name kBid("BID");
static const bbg::Name kAsk("ASK");
static const bbg::Name kAskSize("ASK_SIZE");
static const bbg::Name kBidSize("BID_SIZE");
static const bbg::Name kPxLast("PX_LAST");
static const bbg::Name kLastTrade("LAST_TRADE");
static const bbg::Name kSizeLastTrade("SIZE_LAST_TRADE");
static const bbg::Name kBestAsks[] = {
    bbg::Name{"BEST_ASK1"}, bbg::Name{"BEST_ASK2"}, bbg::Name{"BEST_ASK3"},
    bbg::Name{"BEST_ASK4"}, bbg::Name{"BEST_ASK5"}};
static const bbg::Name kBestBids[] = {
    bbg::Name{"BEST_BID1"}, bbg::Name{"BEST_BID2"}, bbg::Name{"BEST_BID3"},
    bbg::Name{"BEST_BID4"}, bbg::Name{"BEST_BID5"}};
static const bbg::Name kBestAskSzs[] = {
    bbg::Name{"BEST_ASK1_SZ"}, bbg::Name{"BEST_ASK2_SZ"},
    bbg::Name{"BEST_ASK3_SZ"}, bbg::Name{"BEST_ASK4_SZ"},
    bbg::Name{"BEST_ASK5_SZ"}};
static const bbg::Name kBestBidSzs[] = {
    bbg::Name{"BEST_BID1_SZ"}, bbg::Name{"BEST_BID2_SZ"},
    bbg::Name{"BEST_BID3_SZ"}, bbg::Name{"BEST_BID4_SZ"},
    bbg::Name{"BEST_BID5_SZ"}};

void BPIPE::Start() noexcept {
  auto logon_type = config("logon_type");
  if (logon_type.empty()) {
    LOG_FATAL(name() << ": logon_type not given");
  }

  auto logon_params = config("logon_params");
  if (logon_params.empty()) {
    LOG_FATAL(name() << ": logon_params not given");
  }

  auto host = config("host");
  if (host.empty()) {
    LOG_FATAL(name() << ": host not given");
  }

  auto port = atoi(config("port").c_str());
  if (!port) {
    LOG_FATAL(name() << ": port not given");
  }

  auto n = atoi(config("reconnect_interval").c_str());
  if (n > 0) reconnect_interval_ = n;
  LOG_INFO(name() << ": reconnect_interval=" << reconnect_interval_ << "s");

  auto hosts = opentrade::Split(host, ",");
  for (auto i = 0u; i < hosts.size(); ++i) {
    LOG_INFO(name() << ": set server " << hosts[i] << ":" << port);
    options_.setServerAddress(hosts[i].c_str(), port, i);
  }
  options_.setAutoRestartOnDisconnection(true);

  depth_ = !!atoi(config("depth").c_str());
  LOG_INFO(name() << ": depth=" << depth_);

  auto params = opentrade::Split(logon_params, ",");
  std::string appName = logon_params;
  if (params.size() >= 3) {
    if (params.size() >= 4) {
      appName = params.front();
      params.erase(params.begin());
    }
    auto tlsOptions = bbg::TlsOptions::createFromFiles(
        params[0].c_str(),   // d_clientCredentials
        params[1].c_str(),   // d_clientCredentialsPassword
        params[2].c_str());  // d_trustMaterial
    LOG_INFO(name() << ": app=" << appName);
    options_.setTlsOptions(tlsOptions);
    LOG_INFO(name() << ": tls-client-credentials=" << params[0]);
    LOG_INFO(name() << ": tls-client-credentials-password=" << params[1]);
    LOG_INFO(name() << ": tls-trust-material=" << params[2]);
  }

  std::string authOptions;
  if (logon_type == "OS_LOGON") {
    authOptions = "AuthenticationType=OS_LOGON";
  } else if (logon_type == "APPLICATION") {
    authOptions = "AuthenticationMode=APPLICATION_ONLY;";
    authOptions += "ApplicationAuthenticationType=APPNAME_AND_KEY;";
    authOptions += "ApplicationName=" + appName;
  } else if (logon_type == "DIRECTORY_SERVICE") {
    authOptions = "AuthenticationType=DIRECTORY_SERVICE;";
    authOptions += "DirSvcPropertyName=" + logon_params;
  } else if (logon_type == "USER_AND_APPLICATION") {
    authOptions =
        "AuthenticationMode=USER_AND_APPLICATION;AuthenticationType=OS_LOGON;"
        "ApplicationAuthenticationType=APPNAME_AND_KEY;ApplicationName=" +
        appName;
  } else {
    LOG_FATAL(
        name() << ": Invalid logon_type, expect on of (OS_LOGON, APPLICATION, "
                  "DIRECTORY_SERVICE, USER_AND_APPLICATION)");
  }

  LOG_INFO(name() << ": Authentication Options = " << authOptions);
  options_.setAuthenticationOptions(authOptions.c_str());
  session_ = new bbg::Session(options_, this);
  session_->startAsync();
}

void BPIPE::Reconnect() noexcept {
  tp_.AddTask([this]() {
    connected_ = 0;
    session_->stop();
    delete session_;  // release fd
    session_ = new bbg::Session(options_, this);
    session_->startAsync();
  });
}

void BPIPE::Subscribe(const opentrade::Security& sec) noexcept {
  if (!subs_.insert(sec.id).second) return;
  if (!connected()) return;
  tp_.AddTask([this, &sec] { Subscribe2(sec); });
}

void BPIPE::Subscribe2(const opentrade::Security& sec) {
  bbg::SubscriptionList sub;
  std::string symbol("//blp/mktdata/bbgid/");
  symbol += sec.bbgid;
  std::string fields = "LAST_TRADE,SIZE_LAST_TRADE,BID,BID_SIZE,ASK,ASK_SIZE,";
  auto depth =
      "BEST_BID1,BEST_BID2,BEST_BID3,BEST_BID4,BEST_BID5,"
      "BEST_BID1_SZ,BEST_BID2_SZ,BEST_BID3_SZ,BEST_BID4_SZ,BEST_BID5_SZ,"
      "BEST_ASK1,BEST_ASK2,BEST_ASK3,BEST_ASK4,BEST_ASK5,"
      "BEST_ASK1_SZ,BEST_ASK2_SZ,BEST_ASK3_SZ,BEST_ASK4_SZ,BEST_ASK5_SZ";
  if (depth_) fields += depth;

  auto ticker = ++ticker_counter_;
  tickers_[ticker] = &sec;
  sub.add(symbol.c_str(), fields.c_str(), "", bbg::CorrelationId(ticker));
  session_->subscribe(sub, identity_);
}

bool BPIPE::processEvent(const bbg::Event& evt, bbg::Session* session) {
  try {
    switch (evt.eventType()) {
      case bbg::Event::SESSION_STATUS:
        ProcessSessionStatus(evt);
        break;
      case bbg::Event::RESPONSE:
      case bbg::Event::PARTIAL_RESPONSE:
      case bbg::Event::AUTHORIZATION_STATUS:
        ProcessResponse(evt);
        break;
      case bbg::Event::SUBSCRIPTION_DATA:
        ProcessSubscriptionData(evt);
        break;
      case bbg::Event::TOKEN_STATUS:
        ProcessTokenStatus(evt);
        break;
      case bbg::Event::SERVICE_STATUS:
      case bbg::Event::TIMEOUT:
      case bbg::Event::RESOLUTION_STATUS:
      case bbg::Event::TOPIC_STATUS:
      case bbg::Event::REQUEST:
      case bbg::Event::SUBSCRIPTION_STATUS:
      case bbg::Event::REQUEST_STATUS:
      case bbg::Event::UNKNOWN:
      case bbg::Event::ADMIN:
      default:
        LogEvent(evt);
        break;
    }
  } catch (const bbg::Exception& e) {
    LOG_ERROR(name() << ": bbg::Exception: " << e.description());
  } catch (const std::exception& e) {
    LOG_ERROR(name() << ": std::exception: " << e.what());
  }
  return true;
}

void BPIPE::ProcessSessionStatus(const bbg::Event& evt) {
  bbg::MessageIterator it(evt);
  while (it.next()) {
    bbg::Message msg = it.message();
    LOG_INFO(name() << ": " << msg.messageType());
    if (msg.messageType() == "SessionStarted") {
      OnConnect();
    } else if (msg.messageType() == "SessionTerminated" ||
               msg.messageType() == "SessionConnectionDown" ||
               msg.messageType() == "SessionStartupFailure") {
      connected_ = 0;
      tp_.AddTask([this]() { Reconnect(); },
                  boost::posix_time::seconds(reconnect_interval_));
    }
  }
  LogEvent(evt);
}

void BPIPE::OnConnect() {
  if (!session_->openService("//blp/apiauth")) return;
  auth_service_ = session_->getService("//blp/apiauth");

  identity_ = session_->createIdentity();
  LOG_INFO(name() << ": Generate token from session");
  session_->generateToken();
}

void BPIPE::ProcessResponse(const bbg::Event& evt) {
  bbg::MessageIterator it(evt);
  while (it.next()) {
    auto msg = it.message();
    auto msg_type = msg.messageType();
    if (msg_type == "AuthorizationSuccess") {
      connected_ = 1;
      for (auto id : subs_) {
        auto sec = opentrade::SecurityManager::Instance().Get(id);
        if (!sec) continue;
        Subscribe2(*sec);
      }
      LOG_INFO(name() << ": Connected");
    } else if (msg_type == "AuthorizationFailure") {
      LOG_ERROR(name() << ": AuthorizationFailure");
    }
  }
  LogEvent(evt);
}

inline void BPIPE::UpdateQuote(const bbg::Message& msg,
                               const opentrade::Security& sec,
                               const bbg::Name& ask_name,
                               const bbg::Name& bid_name,
                               const bbg::Name& ask_sz_name,
                               const bbg::Name& bid_sz_name, int level) {
  auto ask = 0.;
  auto ask_sz = 0.;
  auto bid = 0.;
  auto bid_sz = 0.;
  if (msg.hasElement(ask_name, true)) {
    ask = msg.getElementAsFloat64(ask_name);
    if (msg.hasElement(ask_sz_name, true))
      ask_sz = msg.getElementAsInt64(ask_sz_name);
  }
  if (msg.hasElement(bid_name, true)) {
    bid = msg.getElementAsFloat64(bid_name);
    if (msg.hasElement(bid_sz_name, true))
      bid_sz = msg.getElementAsInt64(bid_sz_name);
  }

  if (ask > 0 && bid > 0) {
    Update(sec.id, opentrade::MarketData::Quote{ask, ask_sz, bid, bid_sz},
           level);
  } else if (ask > 0) {
    Update(sec.id, ask, ask_sz, false, level);
  } else if (bid > 0) {
    Update(sec.id, bid, bid_sz, true, level);
  }
}

void BPIPE::ProcessSubscriptionData(const bbg::Event& evt) {
  bbg::MessageIterator it(evt);
  while (it.next()) {
    auto msg = it.message();
    auto ticker = msg.correlationId().asInteger();
    auto sec = tickers_[ticker];
    if (!sec) continue;
    auto px = 0.;
    auto sz = 0.;
    if (msg.hasElement(kLastTrade, true)) {
      px = msg.getElementAsFloat64(kLastTrade);
      if (msg.hasElement(kSizeLastTrade, true))
        sz = msg.getElementAsInt64(kSizeLastTrade);
    }
    if (px > 0) {
      Update(sec->id, px, sz);
    }
    UpdateQuote(msg, *sec, kAsk, kBid, kAskSize, kBidSize, 0);
    if (depth_) {
      for (auto i = 0; i < 5; ++i) {
        UpdateQuote(msg, *sec, kBestAsks[i], kBestBids[i], kBestAskSzs[i],
                    kBestBidSzs[i], i);
      }
    }
  }
}

void BPIPE::ProcessTokenStatus(const bbg::Event& evt) {
  bbg::MessageIterator it(evt);
  while (it.next()) {
    auto msg = it.message();
    if (msg.messageType() == "TokenGenerationSuccess") {
      LOG_INFO(name() << ": TokenGenerationSuccess");
      auto req = auth_service_.createAuthorizationRequest();
      req.set("token", msg.getElementAsString("token"));
      session_->sendAuthorizationRequest(req, &identity_,
                                         bbg::CorrelationId(++ticker_counter_));
    } else if (msg.messageType() == "TokenGenerationFailure") {
      LOG_ERROR(name() << ": TokenGenerationFailure");
    }
  }
  LogEvent(evt);
}

void BPIPE::LogEvent(const bbg::Event& evt) {
  bbg::MessageIterator it(evt);
  while (it.next()) {
    auto msg = it.message();
    LOG_DEBUG(name() << ": " << evt.eventType() << ": " << msg);
  }
}

extern "C" {
opentrade::Adapter* create() { return new BPIPE{}; }
}
