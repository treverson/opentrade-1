#ifndef FIX_FILELOG_H_
#define FIX_FILELOG_H_

#define private protected
#define throw(...)
#include <quickfix/FileLog.h>
#undef private
#undef throw

#include "opentrade/task_pool.h"

namespace FIX {

class AsyncFileLog : public FileLog {
 public:
  explicit AsyncFileLog(const std::string& path) : FileLog(path) {}
  AsyncFileLog(const std::string& path, const SessionID& sessionID)
      : FileLog(path, sessionID) {}
  AsyncFileLog(const std::string& path, const std::string& backupPath,
               const SessionID& sessionID)
      : FileLog(path, backupPath, sessionID) {}

  void appendTime(const std::string& value, std::string& newvalue) {
    struct timespec t;
    clock_gettime(CLOCK_REALTIME, &t);
    char buf[64];
    int n = snprintf(buf, sizeof(buf), "0=%ld.%ld\1", t.tv_sec, t.tv_nsec);
    if (n < 0) n = 0;
    newvalue.reserve(value.length() + n + 1);
    newvalue.append(value);
    if (n > 0) newvalue.append(buf);
  }

  void onIncoming(const std::string& value) override {
    std::string newvalue;
    appendTime(value, newvalue);
    pool_.AddTask([=]() { this->FileLog::onIncoming(newvalue); });
  }
  void onOutgoing(const std::string& value) override {
    std::string newvalue;
    appendTime(value, newvalue);
    pool_.AddTask([=]() { this->FileLog::onOutgoing(newvalue); });
  }
  void onEvent(const std::string& value) override {
    pool_.AddTask([=]() { this->FileLog::onEvent(value); });
  }
  void clear() override {
    pool_.AddTask([=]() { this->FileLog::clear(); });
  }
  void backup() override {
    pool_.AddTask([=]() { this->FileLog::backup(); });
  }

 private:
  opentrade::TaskPool pool_;
};

class AsyncFileLogFactory : public FileLogFactory {
 public:
  explicit AsyncFileLogFactory(const SessionSettings& settings)
      : FileLogFactory(settings) {}
  explicit AsyncFileLogFactory(const std::string& path)
      : FileLogFactory(path) {}
  AsyncFileLogFactory(const std::string& path, const std::string& backupPath)
      : FileLogFactory(path, backupPath) {}

 public:
  Log* create() {
    m_globalLogCount++;
    if (m_globalLogCount > 1) return m_globalLog;

    try {
      if (m_path.size()) return new AsyncFileLog(m_path);
      std::string path;
      std::string backupPath;

      Dictionary settings = m_settings.get();
      path = settings.getString(FILE_LOG_PATH);
      backupPath = path;
      if (settings.has(FILE_LOG_BACKUP_PATH))
        backupPath = settings.getString(FILE_LOG_BACKUP_PATH);

      return m_globalLog = new AsyncFileLog(path);
    } catch (ConfigError&) {
      m_globalLogCount--;
      throw;
    }
  }

  Log* create(const SessionID& s) {
    if (m_path.size() && m_backupPath.size())
      return new AsyncFileLog(m_path, m_backupPath, s);
    if (m_path.size()) return new AsyncFileLog(m_path, s);

    std::string path;
    Dictionary settings = m_settings.get(s);
    path = settings.getString(FILE_LOG_PATH);
    return new AsyncFileLog(path, s);
  }
};

}  // namespace FIX

#endif  // FIX_FILELOG_H_
