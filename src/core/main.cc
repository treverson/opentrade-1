#include <boost/algorithm/string.hpp>
#include <boost/filesystem.hpp>
#include <boost/program_options.hpp>
#include <boost/property_tree/ini_parser.hpp>
#include <boost/property_tree/ptree.hpp>
#include <fstream>
#include <iostream>

#include "account.h"
#include "algo.h"
#include "database.h"
#include "exchange_connectivity.h"
#include "logger.h"
#include "market_data.h"
#include "position.h"
#include "security.h"
#include "server.h"

namespace bpo = boost::program_options;
namespace fs = boost::filesystem;
using opentrade::AlgoManager;
using opentrade::ExchangeConnectivityManager;
using opentrade::MarketDataManager;
using opentrade::PositionManager;

int main(int argc, char *argv[]) {
  std::string config_file_path;
  std::string log_config_file_path;
  std::string db_url;
  uint16_t db_pool_size;
  bool disable_rms;
  bool db_create_tables;
  int io_threads;
  int algo_threads;
  int port;
  try {
    bpo::options_description config("Configuration");
    config.add_options()("help,h", "produce help message")(
        "config_file,c",
        bpo::value<std::string>(&config_file_path)
            ->default_value("opentrade.conf"),
        "config file path")("log_config_file,l",
                            bpo::value<std::string>(&log_config_file_path)
                                ->default_value("log.conf"),
                            "log4cxx config file path")(
        "db_url", bpo::value<std::string>(&db_url), "database connection url")(
        "db_create_tables",
        bpo::value<bool>(&db_create_tables)->default_value(false),
        "create database tables")(
        "db_pool_size", bpo::value<uint16_t>(&db_pool_size)->default_value(4),
        "database connection pool size")(
        "port", bpo::value<int>(&port)->default_value(9111), "listen port")(
        "io_threads", bpo::value<int>(&io_threads)->default_value(1),
        "number of web server io threads")(
        "algo_threads", bpo::value<int>(&algo_threads)->default_value(1),
        "number of algo threads")(
        "disable_rms", bpo::value<bool>(&disable_rms)->default_value(false),
        "whether disable rms");

    bpo::options_description config_file_options;
    config_file_options.add(config);
    bpo::variables_map vm;

    bpo::store(bpo::parse_command_line(argc, argv, config), vm);
    bpo::notify(vm);
    if (vm.count("help")) {
      std::cerr << config << std::endl;
      return 1;
    }

    std::ifstream ifs(config_file_path.c_str());
    if (ifs) {
      bpo::store(parse_config_file(ifs, config_file_options, true), vm);
    } else {
      std::cerr << config_file_path << " not found" << std::endl;
      return 1;
    }

    bpo::store(bpo::parse_command_line(argc, argv, config), vm);
    bpo::notify(vm);  // make command line option higher priority
  } catch (bpo::error &e) {
    std::cerr << "Bad Options: " << e.what() << std::endl;
    return 1;
  }

  if (!std::ifstream(log_config_file_path.c_str()).good()) {
    std::ofstream(log_config_file_path)
        .write(opentrade::kDefaultLogConf, strlen(opentrade::kDefaultLogConf));
  }

  opentrade::Logger::Initialize("opentrade", log_config_file_path);

  auto path = fs::path(".") / "store";
  if (!fs::exists(path)) {
    fs::create_directory(path);
  }

  if (db_url.empty()) {
    LOG_ERROR("db_url not configured");
    return 1;
  }

  opentrade::Database::Initialize(db_url, db_pool_size, db_create_tables);
  opentrade::SecurityManager::Initialize();
  AlgoManager::Initialize();

  boost::property_tree::ptree prop_tree;
  boost::property_tree::ini_parser::read_ini(config_file_path, prop_tree);
  for (auto &section : prop_tree) {
    if (!section.second.size()) continue;
    auto section_name = section.first;
    opentrade::Adapter::StrMap params;
    for (auto &item : section.second) {
      auto name = item.first;
      boost::to_lower(name);
      params[name] = item.second.data();
    }
    auto sofile = params["sofile"];
    if (sofile.empty()) continue;
    params.erase("sofile");
    auto adapter = opentrade::Adapter::Load(sofile);
    adapter->set_name(section_name);
    adapter->set_config(params);
    if (adapter->GetVersion() != opentrade::kApiVersion) {
      LOG_FATAL("Version mismatch");
    }
    if (section_name.find("md_") == 0) {
      auto md = dynamic_cast<opentrade::MarketDataAdapter *>(adapter);
      if (!md) LOG_FATAL("Failed to create MarketDataAdapter");
      MarketDataManager::Instance().Add(md);
    } else if (section_name.find("ec_") == 0) {
      auto ec = dynamic_cast<opentrade::ExchangeConnectivityAdapter *>(adapter);
      if (!ec) LOG_FATAL("Failed to create ExchangeConnectivityAdapter");
      ExchangeConnectivityManager::Instance().Add(ec);
    } else {
      auto algo = dynamic_cast<opentrade::Algo *>(adapter);
      if (!algo) LOG_FATAL("Failed to create Algo");
      AlgoManager::Instance().Add(algo);
    }
  }

  opentrade::AccountManager::Initialize();
  PositionManager::Initialize();
  opentrade::GlobalOrderBook::Initialize();
  for (auto &p : MarketDataManager::Instance().adapters()) {
    p.second->Start();
  }
  for (auto &p : ExchangeConnectivityManager::Instance().adapters()) {
    p.second->Start();
  }
  for (auto &p : AlgoManager::Instance().adapters()) {
    p.second->Start();
  }

  PositionManager::Instance().UpdatePnl();
  AlgoManager::Instance().Run(algo_threads);
  opentrade::Server::Start(port, io_threads);

  return 0;
}
