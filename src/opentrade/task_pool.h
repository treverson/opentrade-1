#ifndef OPENTRADE_TASK_POOL_H_
#define OPENTRADE_TASK_POOL_H_

#include <boost/asio.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>
#include <thread>
#include <vector>

namespace opentrade {

class TaskPool {
 public:
  explicit TaskPool(size_t nthreads = 1) {
    work_.reset(new boost::asio::io_service::work(service_));
    for (auto i = 0u; i < nthreads; ++i) {
      threads_.emplace_back([this]() { service_.run(); });
    }
  }

  ~TaskPool() {
    if (work_) Stop();
  }

  void Stop(bool wait = true) {
    if (wait) {
      work_.reset();  // if not delete this, io_service::run will never exit
      for (auto& t : threads_) t.join();
      service_.stop();
    } else {
      service_.stop();
      for (auto& t : threads_) t.join();
      work_.reset();
    }
  }

  template <typename T, typename Tm>
  void AddTask(const T& func, Tm t) {
    auto tt = new boost::asio::deadline_timer(service_, t);
    tt->async_wait([=](const boost::system::error_code&) {
      func();
      delete tt;
    });
  }

  template <typename T>
  void AddTask(const T& func) {
    service_.post(func);
  }

  template <typename T, typename Tm, typename Tm2>
  void RepeatTask(const T& func, Tm t, Tm2 interval) {
    addTask(
        [=]() {
          addTask(func, interval);
          func();
        },
        t);
  }

 protected:
  std::vector<std::thread> threads_;
  boost::asio::io_service service_;
  std::unique_ptr<boost::asio::io_service::work> work_;
};

}  // namespace opentrade

#endif  // OPENTRADE_TASK_POOL_H_
