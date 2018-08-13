#ifndef OPENTRADE_UTILITY_H_
#define OPENTRADE_UTILITY_H_

#include <sys/time.h>
#include <boost/algorithm/string.hpp>
#include <cstring>
#include <ctime>
#include <iostream>
#include <optional>
#include <variant>
#include <vector>

namespace opentrade {

template <typename V>
const typename V::mapped_type& FindInMap(const V& map,
                                         const typename V::key_type& key) {
  static const typename V::mapped_type kValue{};
  auto it = map.find(key);
  if (it == map.end()) return kValue;
  return it->second;
}

template <typename M, typename V>
std::optional<V> GetParam(const M& var_map, const std::string& name) {
  auto it = var_map.find(name);
  if (it == var_map.end()) return {};
  if (auto pval = std::get_if<V>(&it->second)) return *pval;
  return {};
}

template <typename M, typename V>
inline V GetParam(const M& var_map, const std::string& name, V default_value) {
  return GetParam<M, V>(var_map, name).value_or(default_value);
}

template <typename M>
inline int GetParam(const M& var_map, const std::string& name,
                    int default_value) {
  return GetParam<M, int64_t>(var_map, name).value_or(default_value);
}

template <typename M>
inline std::string GetParam(const M& var_map, const std::string& name,
                            const char* default_value) {
  return GetParam<M, std::string>(var_map, name).value_or(default_value);
}

static inline int64_t NowUtcInMicro() {
  struct timeval now;
  auto rc = gettimeofday(&now, nullptr);
  if (rc)
    return time(nullptr) * 1000000lu;
  else
    return now.tv_sec * 1000000lu + now.tv_usec;
}

static inline const char* GetNowStr() {
  struct timeval tp;
  gettimeofday(&tp, NULL);
  struct tm tm_info;
  localtime_r(&tp.tv_sec, &tm_info);
  char buf[256];
  strftime(buf, 26, "%Y-%m-%d %H:%M:%S", &tm_info);
  static thread_local char out[256];
  snprintf(out, sizeof(out), "%s.%06ld", buf, tp.tv_usec);
  return out;
}

// not thread-safe and highly compatibility, but work well on linux
static inline int GetUtcTimeOffset(const char* tz) {
  auto orig_tz = getenv("TZ");
  setenv("TZ", tz, 1);
  tzset();
  struct tm tm;
  auto t = time(nullptr);
  localtime_r(&t, &tm);
  if (orig_tz)
    setenv("TZ", orig_tz, 1);
  else
    unsetenv("TZ");
  tzset();
  return tm.tm_gmtoff;
}

static const int kSecondsOneDay = 3600 * 24;

static inline int GetUtcSinceMidNight(int tm_gmtoff) {
  time_t rawtime;
  time(&rawtime);
  struct tm tm_info;
  gmtime_r(&rawtime, &tm_info);
  auto n = tm_info.tm_hour * 3600 + tm_info.tm_min * 60 + tm_info.tm_sec;
  return (n + kSecondsOneDay + tm_gmtoff) % kSecondsOneDay;
}

static inline decltype(auto) Split(const std::string& str, const char* sep,
                                   bool compact = true,
                                   bool remove_empty = true) {
  std::vector<std::string> out;
  boost::split(out, str, boost::is_any_of(sep),
               compact ? boost::token_compress_on : boost::token_compress_off);
  if (remove_empty) {
    out.erase(std::remove_if(out.begin(), out.end(),
                             [](auto x) { return x.empty(); }),
              out.end());
  }
  return out;
}

}  // namespace opentrade

#endif  // OPENTRADE_UTILITY_H_
