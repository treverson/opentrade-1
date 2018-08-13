#ifndef FIX_FILESTORE_H_
#define FIX_FILESTORE_H_

#define private protected
#define throw(...)
#include <quickfix/FileStore.h>
#undef private
#undef throw
#include <mutex>

#include "opentrade/task_pool.h"

namespace FIX {

class AsyncFileStore : public FileStore {
 public:
  AsyncFileStore(std::string path, const SessionID& s) : FileStore(path, s) {}

  bool set(int seq, const std::string& msg) override {
    pool_.AddTask([=]() { set_(seq, msg); });
    return true;
  }
  void get(int begin, int end,
           std::vector<std::string>& result) const override {
    std::scoped_lock<std::mutex> lock(m_);
    return FileStore::get(begin, end, result);
  }

  void setNextSenderMsgSeqNum(int value) override {
    m_cache.setNextSenderMsgSeqNum(value);
    pool_.AddTask([this]() { setSeqNum_(); });
  }
  void setNextTargetMsgSeqNum(int value) override {
    m_cache.setNextTargetMsgSeqNum(value);
    pool_.AddTask([this]() { setSeqNum_(); });
  }
  void incrNextSenderMsgSeqNum() override {
    m_cache.incrNextSenderMsgSeqNum();
    pool_.AddTask([this]() { setSeqNum_(); });
  }
  void incrNextTargetMsgSeqNum() override {
    m_cache.incrNextTargetMsgSeqNum();
    pool_.AddTask([this]() { setSeqNum_(); });
  }

  void reset() override {
    std::scoped_lock<std::mutex> lock(m_);
    FileStore::reset();
  }
  void refresh() override {
    std::scoped_lock<std::mutex> lock(m_);
    FileStore::refresh();
  }

 private:
  void set_(int seq, const std::string& msg) {
    std::scoped_lock<std::mutex> lock(m_);
    try {
      FileStore::set(seq, msg);
    } catch (const IOException& e) {
      std::cerr << e.what() << std::endl;
    }
  }

  void setSeqNum_() {
    std::scoped_lock<std::mutex> lock(m_);
    FileStore::setSeqNum();
  }

 private:
  opentrade::TaskPool pool_;
  mutable std::mutex m_;
};

class AsyncFileStoreFactory : public FileStoreFactory {
 public:
  explicit AsyncFileStoreFactory(const SessionSettings& settings)
      : FileStoreFactory(settings) {}
  explicit AsyncFileStoreFactory(const std::string& path)
      : FileStoreFactory(path) {}

  MessageStore* create(const SessionID& s) override {
    if (m_path.size()) return new AsyncFileStore(m_path, s);

    std::string path;
    Dictionary settings = m_settings.get(s);
    path = settings.getString(FILE_STORE_PATH);
    return new AsyncFileStore(path, s);
  }
};

}  // namespace FIX

#endif  // FIX_FILESTORE_H_
