#include "database.h"

#include <postgresql/soci-postgresql.h>

#include "logger.h"
#include "security.h"

namespace opentrade {

class SqlLog : public std::ostream, std::streambuf {
 public:
  SqlLog() : std::ostream(this) {}

  int overflow(int c) {
    if (c == '\r') return 0;
    if (c == '\n') {
      buf_[offset_] = 0;
      LOG4CXX_DEBUG(logger_, buf_);
      offset_ = 0;
    } else if (offset_ + 1 < sizeof(buf_)) {
      buf_[offset_++] = c;
    }
    return 0;
  }

 private:
  char buf_[256];
  uint32_t offset_ = 0u;
  log4cxx::LoggerPtr logger_ = opentrade::Logger::Get("sql");
};

static const char create_tables_sql[] = R"(
  create sequence if not exists exchange_id_seq start with 100;
  create table if not exists exchange(
    id int2 default nextval('exchange_id_seq') not null,
    "name" varchar(50) not null,
    "mic" char(4),
    "country" char(2),
    "ib_name" varchar(50),
    "bb_name" varchar(50),
    "tz" varchar(20),
    "desc" varchar(1000),
    odd_lot_allowed boolean,
    trade_period int4, -- e.g. 09301500 => 09:30 - 15:00
    break_period int4, -- e.g. 11301300 => 11:30 - 13:00
    tick_size_table varchar(5000),
    primary key(id)
  );
  create unique index if not exists exchange_name_index on exchange("name");

  create sequence if not exists security_id_seq start with 10000;
  create table if not exists security(
    id int4 default nextval('security_id_seq') not null,
    symbol varchar(50) not null,
    local_symbol varchar(50),
    type varchar(12) not null,
    currency char(3),
    bbgid varchar(30),
    cusip varchar(30),
    isin varchar(30),
    sedol varchar(30),
    rate float8,
    multiplier float8,
    tick_size float8,
    lot_size int4,
    close_price float8,
    adv20 float8,
    market_cap float8,
    sector int4,
    industry_group int4,
    industry int4,
    sub_industry int4,
    put_or_call boolean,
    opt_attribute char(1),
    maturity_date int4, -- e.g. 20201230 => 2020-12-30
    strike_price float8,
    exchange_id int2 references exchange(id), -- on update cascade on delete cascade,
    underlying_id int4 references security(id), -- on update cascade on delete cascade,
    name varchar(100),
    primary key(id)
  );
  create unique index if not exists security_symbol_exchange_index on security(symbol, exchange_id);

  create sequence if not exists user_id_seq start with 100;
  create table if not exists "user"(
    id int2 default nextval('user_id_seq') not null,
    "name" varchar(50) not null,
    password varchar(50) not null,
    is_admin boolean,
    is_disabled boolean,
    limits varchar(1000),
    primary key(id)
  );
  do $$
  begin
  if not exists(
    select 1 from "user" where "name" = 'admin'
  ) then
    insert into "user"(id, "name", password, is_admin)
    values(1, 'admin', 'a94a8fe5ccb19ba61c4c0873d391e987982fbbd3', true); -- passwd='test'
    insert into "user"("name", password)
    values('test', 'a94a8fe5ccb19ba61c4c0873d391e987982fbbd3'); -- passwd='test'
  end if;
  end $$;
  create unique index if not exists user_name_index on "user"("name");
  
  create sequence if not exists sub_account_id_seq start with 100;
  create table if not exists sub_account(
    id int2 default nextval('sub_account_id_seq') not null,
    "name" varchar(50) not null,
    limits varchar(1000),
    primary key(id)
  );
  create unique index if not exists sub_account_name_index on sub_account("name");

  create table if not exists user_sub_account_map(
    user_id int2 references "user"(id), -- on update cascade on delete cascade,
    sub_account_id int2 references sub_account(id), -- on update cascade on delete cascade,
    primary key(user_id, sub_account_id)
  );

  create sequence if not exists broker_account_id_seq start with 100;
  create table if not exists broker_account(
    id int2 default nextval('broker_account_id_seq') not null,
    "name" varchar(50) not null,
    adapter varchar(50) not null,
    params varchar(1000),
    limits varchar(1000),
    primary key(id)
  );
  create unique index if not exists broker_account_name_index on broker_account("name");

  create table if not exists sub_account_broker_account_map(
    sub_account_id int2 references sub_account(id), -- on update cascade on delete cascade,
    exchange_id int2 references exchange(id), -- on update cascade on delete cascade,
    broker_account_id int2 references broker_account(id), -- on update cascade on delete cascade,
    primary key(sub_account_id, exchange_id)
  );

  create sequence if not exists position_id_seq start with 100;
  create table if not exists position(
    id bigserial,
    user_id int2 references "user"(id), -- on update cascade on delete cascade,
    sub_account_id int2 references sub_account(id), -- on update cascade on delete cascade,
    broker_account_id int2 references broker_account(id), -- on update cascade on delete cascade,
    security_id int2 references security(id), -- on update cascade on delete cascade,
    tm timestamp not null,
    qty float8 not null,
    avg_price float8 not null,
    realized_pnl float8 not null,
    "desc" varchar(1000),
    primary key(id)
  );
  create index if not exists position__index on position(sub_account_id, security_id, id desc);
)";

void Database::Initialize(const std::string& url, uint8_t pool_size,
                          bool create_tables) {
  if (pool_size < 2) pool_size = 2;
  pool_ = new soci::connection_pool(pool_size);
  static SqlLog log;
  for (auto i = 0u; i < pool_size; ++i) {
    auto& sql = pool_->at(i);
    sql.open(soci::postgresql, url);
    sql.set_log_stream(&log);
  }
  if (create_tables) *Session() << create_tables_sql;
}

}  // namespace opentrade
