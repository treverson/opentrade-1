#ifndef OPENTRADE_DATABASE_H_
#define OPENTRADE_DATABASE_H_

#include <soci.h>
#include <cstring>
#include <memory>
#include <string>

namespace opentrade {

class Database {
 public:
  static void Initialize(const std::string& url, uint8_t pool_size,
                         bool create_tables);
  static auto Session() { return std::make_unique<soci::session>(*pool_); }

  template <typename T>
  static T GetValue(soci::row const& row, int index, T default_value) {
    if (row.get_indicator(index) != soci::i_null)
      return row.get<T>(index);
    else
      return default_value;
  }

  static const char* GetValue(soci::row const& row, int index,
                              const char* default_value) {
    if (row.get_indicator(index) != soci::i_null)
      return strdup(row.get<std::string>(index).c_str());
    else
      return default_value;
  }

 private:
  inline static soci::connection_pool* pool_ = nullptr;
};

}  // namespace opentrade

#endif  // OPENTRADE_DATABASE_H_
