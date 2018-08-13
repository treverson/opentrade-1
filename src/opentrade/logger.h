#ifndef OPENTRADE_LOGGER_H_
#define OPENTRADE_LOGGER_H_

#include <log4cxx/basicconfigurator.h>
#include <log4cxx/logger.h>
#include <log4cxx/propertyconfigurator.h>
#include <unistd.h>

namespace opentrade {

class Logger {
 public:
  static auto Get(const std::string& name) {
    return log4cxx::Logger::getLogger(name);
  }

  static void Initialize(const std::string& name,
                         const std::string& config_file) {
    Logger::config_file = config_file;
    if (!config_file.empty())
      log4cxx::PropertyConfigurator::configure(config_file);
    else
      log4cxx::BasicConfigurator::configure();
    if (!name.empty()) logger = Get(name);
  }

  inline static auto logger = log4cxx::Logger::getRootLogger();
  inline static std::string config_file;
};

inline const char* kDefaultLogConf = R"(
log4j.rootLogger=debug, stdout_debug
log4j.logger.opentrade=debug, stdout, opentrade
log4j.additivity.opentrade=false # not inherit appender from parent
log4j.logger.sql=debug, sql
log4j.additivity.sql=false # not inherit appender from parent

log4j.appender.stdout_debug.Threshold=debug
log4j.appender.stdout_debug=org.apache.log4j.ConsoleAppender
log4j.appender.stdout_debug.layout=org.apache.log4j.PatternLayout
log4j.appender.stdout_debug.layout.ConversionPattern=%d{yyyy-MM-dd HH:mm:ss,SSS} %5p - %m%n

log4j.appender.stdout.Threshold=info
log4j.appender.stdout=org.apache.log4j.ConsoleAppender
log4j.appender.stdout.layout=org.apache.log4j.PatternLayout
log4j.appender.stdout.layout.ConversionPattern=%d{yyyy-MM-dd HH:mm:ss,SSS} %5p - %m%n

log4j.appender.opentrade=org.apache.log4j.RollingFileAppender
log4j.appender.opentrade.rollingPolicy=org.apache.log4j.rolling.TimeBasedRollingPolicy
log4j.appender.opentrade.rollingPolicy.FileNamePattern=logs/opentrade.%d{yyyy-MM-dd}.log
log4j.appender.opentrade.rollingPolicy.ActiveFileName=logs/opentrade.log
log4j.appender.opentrade.File=logs/opentrade.log
log4j.appender.opentrade.Append=true
log4j.appender.opentrade.MaxFileSize=100MB
log4j.appender.opentrade.MaxBackupIndex=10
log4j.appender.opentrade.layout=org.apache.log4j.PatternLayout
log4j.appender.opentrade.layout.ConversionPattern=%d{yyyy-MM-dd HH:mm:ss,SSS} %5p - %m%n

log4j.appender.sql=org.apache.log4j.RollingFileAppender
log4j.appender.sql.rollingPolicy=org.apache.log4j.rolling.TimeBasedRollingPolicy
log4j.appender.sql.rollingPolicy.FileNamePattern=logs/sql.%d{yyyy-MM-dd}.log
log4j.appender.sql.rollingPolicy.ActiveFileName=logs/sql.log
log4j.appender.sql.File=logs/sql.log
log4j.appender.sql.Append=true
log4j.appender.sql.MaxFileSize=100MB
log4j.appender.sql.MaxBackupIndex=10
log4j.appender.sql.layout=org.apache.log4j.PatternLayout
log4j.appender.sql.layout.ConversionPattern=%d{yyyy-MM-dd HH:mm:ss,SSS} %5p - %m%n
  )";

}  // namespace opentrade

#define LOG_TRACE(msg) LOG4CXX_TRACE(opentrade::Logger::logger, msg)
#define LOG_DEBUG(msg) LOG4CXX_DEBUG(opentrade::Logger::logger, msg)
#define LOG_INFO(msg) LOG4CXX_INFO(opentrade::Logger::logger, msg)
#define LOG_WARN(msg) LOG4CXX_WARN(opentrade::Logger::logger, msg)
#define LOG_ERROR(msg) LOG4CXX_ERROR(opentrade::Logger::logger, msg)
#define LOG_FATAL(msg)                                             \
  {                                                                \
    LOG4CXX_FATAL(opentrade::Logger::logger, msg);                 \
    /* to-do: safe exit */                                         \
    if (system(("kill -9 " + std::to_string(getpid())).c_str())) { \
    }                                                              \
  }

#endif  // OPENTRADE_LOGGER_H_
