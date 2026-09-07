#include <my_global.h>
#include <mysql/plugin.h>
#include "ha_parquet.h"
#include "sql_class.h"

handlerton *parquet_hton;

//one lock shared by every parquet table until day 9's real locking work
static THR_LOCK parquet_lock;

static handler *parquet_create_handler(handlerton *hton, TABLE_SHARE *table,
                                       MEM_ROOT *mem_root)
{
  return new (mem_root) ha_parquet(hton, table);
}

static int parquet_init_func(void *p)
{
  parquet_hton= (handlerton *)p;
  parquet_hton->create= parquet_create_handler;
  thr_lock_init(&parquet_lock);
  return 0;
}

ha_parquet::ha_parquet(handlerton *hton, TABLE_SHARE *table_arg)
  :handler(hton, table_arg)
{}

ulonglong ha_parquet::table_flags() const
{
  return HA_BINLOG_STMT_CAPABLE;
}

ulong ha_parquet::index_flags(uint idx, uint part, bool all_parts) const
{
  //no indexes yet, parquet files are scanned end to end
  return 0;
}

int ha_parquet::open(const char *name, int mode, uint test_if_locked)
{
  thr_lock_data_init(&parquet_lock, &lock, NULL);
  return 0;
}

int ha_parquet::close(void)
{
  return 0;
}

int ha_parquet::create(const char *name, TABLE *table_arg, HA_CREATE_INFO *create_info)
{
  //no real data path yet (day 2 goal is just that CREATE TABLE succeeds)
  return 0;
}

int ha_parquet::delete_table(const char *name)
{
  return 0;
}

int ha_parquet::write_row(const uchar *buf)
{
  return HA_ERR_WRONG_COMMAND;
}

int ha_parquet::update_row(const uchar *old_data, const uchar *new_data)
{
  return HA_ERR_WRONG_COMMAND;
}

int ha_parquet::delete_row(const uchar *buf)
{
  return HA_ERR_WRONG_COMMAND;
}

int ha_parquet::rnd_init(bool scan)
{
  return HA_ERR_WRONG_COMMAND;
}

int ha_parquet::rnd_next(uchar *buf)
{
  return HA_ERR_WRONG_COMMAND;
}

int ha_parquet::rnd_pos(uchar *buf, uchar *pos)
{
  return HA_ERR_WRONG_COMMAND;
}

void ha_parquet::position(const uchar *record)
{}

int ha_parquet::info(uint flag)
{
  return 0;
}

int ha_parquet::external_lock(THD *thd, int lock_type)
{
  return 0;
}

THR_LOCK_DATA **ha_parquet::store_lock(THD *thd, THR_LOCK_DATA **to,
                                       enum thr_lock_type lock_type)
{
  if (lock_type != TL_IGNORE && lock.type == TL_UNLOCK)
    lock.type= lock_type;
  *to++= &lock;
  return to;
}

const Item *ha_parquet::cond_push(const Item *cond)
{
  //stage 2: translate cond into a duckdb WHERE clause. for now, tell
  //mariadb we didn't consume it so it still filters rows itself.
  return cond;
}

void ha_parquet::cond_pop()
{}

struct st_mysql_storage_engine parquet_storage_engine=
{ MYSQL_HANDLERTON_INTERFACE_VERSION };

maria_declare_plugin(parquet)
{
  MYSQL_STORAGE_ENGINE_PLUGIN,
  &parquet_storage_engine,
  "PARQUET",
  "Caleb Goodman",
  "Parquet storage engine backed by DuckDB, S3, and Apache Iceberg",
  PLUGIN_LICENSE_GPL,
  parquet_init_func,
  NULL,
  0x0001,
  NULL,
  NULL,
  "0.1",
  MariaDB_PLUGIN_MATURITY_EXPERIMENTAL
}
maria_declare_plugin_end;
