#include "server.h"

#include <boost/asio.hpp>
#include <boost/filesystem.hpp>
#include <exception>
#include <fstream>
#include <mutex>
#include <thread>

#include "3rd/simple_web_server/server_http.hpp"
#include "3rd/simple_websocket_server/server_ws.hpp"
#include "connection.h"
#include "logger.h"

namespace opentrade {

using WsServer = SimpleWeb::SocketServer<SimpleWeb::WS>;
using HttpServer = SimpleWeb::Server<SimpleWeb::HTTP>;
typedef std::shared_ptr<WsServer::Connection> WsConnPtr;
typedef std::lock_guard<std::mutex> LockGuard;

static HttpServer kHttpServer;
static WsServer kWsServer;
static std::unordered_map<WsConnPtr, Connection::Ptr> kSocketMap;
static std::mutex kMutex;
static auto kIoService = std::make_shared<boost::asio::io_service>();

void close(WsConnPtr connection) {
  LockGuard lock(kMutex);
  auto it = kSocketMap.find(connection);
  if (it == kSocketMap.end()) return;
  it->second->Close();
  kSocketMap.erase(it);
}

struct WsSocketWrapper : public Transport {
  explicit WsSocketWrapper(WsConnPtr ws) : ws_(ws) {}

  std::string GetAddress() const { return ws_->remote_endpoint_address(); }

  void Send(const std::string& msg) override {
    auto send_stream = std::make_shared<WsServer::SendStream>();
    *send_stream << msg;
    ws_->send(send_stream, [](const SimpleWeb::error_code& e) {
      if (e) {
        LOG_DEBUG("GATEWAY Server: Error sending message. "
                  << "Error: " << e << ", error message: " << e.message());
      }
    });
  }

 private:
  WsConnPtr ws_;
};

void Server::Publish(Confirmation::Ptr cm) {
  kIoService->post([cm]() {
    LockGuard lock(kMutex);
    for (auto& pair : kSocketMap) {
      pair.second->Send(cm);
    }
  });
}

void Server::Publish(const Algo& algo, const std::string& status,
                     const std::string& body, uint32_t seq) {
  kIoService->post([&algo, status, body, seq]() {
    LockGuard lock(kMutex);
    for (auto& pair : kSocketMap) {
      pair.second->Send(algo, status, body, seq);
    }
  });
}

static void ServeStatic() {
  kHttpServer.default_resource["GET"] =
      [](std::shared_ptr<HttpServer::Response> response,
         std::shared_ptr<HttpServer::Request> request) {
        try {
          auto web_root_path = boost::filesystem::canonical("web");
          auto path =
              boost::filesystem::canonical(web_root_path / request->path);
          // Check if path is within web_root_path
          if (std::distance(web_root_path.begin(), web_root_path.end()) >
                  std::distance(path.begin(), path.end()) ||
              !std::equal(web_root_path.begin(), web_root_path.end(),
                          path.begin()))
            throw std::invalid_argument("path must be within root path");
          if (boost::filesystem::is_directory(path)) path /= "index.html";

          SimpleWeb::CaseInsensitiveMultimap header;

          // Uncomment the following line to enable Cache-Control
          // header.emplace("Cache-Control", "max-age=86400");

          auto ifs = std::make_shared<std::ifstream>();
          ifs->open(path.string(),
                    std::ifstream::in | std::ios::binary | std::ios::ate);

          if (*ifs) {
            auto length = ifs->tellg();
            ifs->seekg(0, std::ios::beg);

            header.emplace("Content-Length", std::to_string(length));
            response->write(header);

            // Trick to define a recursive function within this scope (for
            // example purposes)
            class FileServer {
             public:
              static void read_and_send(
                  const std::shared_ptr<HttpServer::Response>& response,
                  const std::shared_ptr<std::ifstream>& ifs) {
                // Read and send 128 KB at a time
                static std::vector<char> buffer(
                    131072);  // Safe when server is running on one thread
                std::streamsize read_length;
                if ((read_length =
                         ifs->read(&buffer[0],
                                   static_cast<std::streamsize>(buffer.size()))
                             .gcount()) > 0) {
                  response->write(&buffer[0], read_length);
                  if (read_length ==
                      static_cast<std::streamsize>(buffer.size())) {
                    response->send(
                        [response, ifs](const SimpleWeb::error_code& ec) {
                          if (!ec)
                            read_and_send(response, ifs);
                          else
                            LOG_DEBUG("Http connection interrupted");
                        });
                  }
                }
              }
            };
            FileServer::read_and_send(response, ifs);
          } else {
            throw std::invalid_argument("could not read file");
          }
        } catch (const std::exception& e) {
          response->write(
              SimpleWeb::StatusCode::client_error_bad_request,
              "Could not open path " + request->path + ": " + e.what());
        }
      };
}

void Server::Start(int port, int nthreads) {
  nthreads = std::min(1, nthreads);
  LOG_INFO("web server nthreads=" << nthreads);
  kHttpServer.io_service = kIoService;
  kHttpServer.config.reuse_address = true;
  kHttpServer.config.port = port;
  kWsServer.io_service = kIoService;
  kWsServer.config.reuse_address = true;
  kWsServer.config.port = port + 1;

  auto& endpoint = kWsServer.endpoint["^/ot[/]?$"];

  endpoint.on_message = [](WsConnPtr connection,
                           std::shared_ptr<WsServer::Message> message) {
    Connection::Ptr p;
    {
      LockGuard lock(kMutex);
      p = kSocketMap[connection];
    }
    if (p) p->OnMessage(message->string());
  };

  endpoint.on_open = [](WsConnPtr connection) {
    LOG_DEBUG("Websocket Server: Opened connection "
              << connection->remote_endpoint_address());
    Connection::Ptr p(new Connection(
        std::make_shared<WsSocketWrapper>(connection), kIoService));
    {
      LockGuard lock(kMutex);
      kSocketMap[connection] = p;
    }
  };

  endpoint.on_close = [](WsConnPtr connection, int status,
                         const std::string& /*reason*/) {
    LOG_DEBUG("Websocket Server: Closed connection "
              << connection->remote_endpoint_address() << " with status code "
              << status);
    close(connection);
  };

  endpoint.on_error = [](WsConnPtr connection, const SimpleWeb::error_code& e) {
    LOG_DEBUG("Websocket Server: Error in connection "
              << connection->remote_endpoint_address() << ". "
              << "Error: " << e << ", error message: " << e.message());
    close(connection);
  };

  // to make nginx work
  kHttpServer.on_upgrade = [=](std::unique_ptr<SimpleWeb::HTTP>& socket,
                               std::shared_ptr<typename SimpleWeb::ServerBase<
                                   SimpleWeb::HTTP>::Request>
                                   request) {
    auto connection =
        std::make_shared<SimpleWeb::SocketServer<SimpleWeb::WS>::Connection>(
            std::move(socket));
    connection->method = std::move(request->method);
    connection->path = std::move(request->path);
    connection->query_string = std::move(request->query_string);
    connection->http_version = std::move(request->http_version);
    connection->header = std::move(request->header);
    connection->remote_endpoint = std::move(*request->remote_endpoint);
    kWsServer.upgrade(connection);
  };

  ServeStatic();

  kHttpServer.on_error = [](std::shared_ptr<HttpServer::Request> /*request*/,
                            const SimpleWeb::error_code& /*ec*/) {};

  try {
    kWsServer.start();
    kHttpServer.start();
    LOG_INFO("http://0.0.0.0:" << port << " start to listen");
    LOG_INFO("ws://0.0.0.0:" << port << "/ot"
                             << " start to listen");
    std::vector<std::thread> threads;
    for (auto i = 0; i < nthreads; ++i) {
      threads.emplace_back([]() { kIoService->run(); });
    }
    for (auto& t : threads) t.join();
  } catch (std::runtime_error& e) {
    LOG_ERROR("failed to start web server: " << e.what());
  }
}

void Server::Stop() {
  kHttpServer.stop();
  kWsServer.stop();

  LockGuard lock(kMutex);
  for (auto& pair : kSocketMap) pair.second->Close();
  kSocketMap.clear();
}

}  // namespace opentrade
