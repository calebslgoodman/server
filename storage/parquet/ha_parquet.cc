#include <my_global.h>
#include <mysql/plugin.h>
#include "ha_parquet.h"
#include "sql_class.h"
#include "field.h"
#include "parquet_schema.h"

#include <sys/stat.h>
#include <cstdio>
#include <map>
#include <mutex>
#include <vector>

handlerton *parquet_hton;

//one lock shared by every parquet table until day 9's real locking work
static THR_LOCK parquet_lock;

//write_row buffers into an in-memory duckdb table (per open table name);
//external_lock's unlock flushes that buffer out to a real .parquet file
//on disk and remembers its path here so rnd_init can read it back.
static duckdb::DuckDB *parquet_db;
static std::mutex parquet_files_mutex;
static std::map<std::string, std::vector<std::string>> parquet_files;
static std::map<std::string, uint64_t> parquet_file_counter;

static std::string ParquetFileDir(const std::string &table_path)
{
  return table_path + "_parquet";
}

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
  parquet_db= new duckdb::DuckDB(nullptr);
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
  duckdb_table_name= name;
  con.reset(new duckdb::Connection(*parquet_db));
  return 0;
}

int ha_parquet::close(void)
{
  scan_result.reset();
  con.reset();
  return 0;
}

int ha_parquet::create(const char *name, TABLE *table_arg, HA_CREATE_INFO *create_info)
{
  std::string sql, error;
  if (!parquet::BuildDuckDBCreateTableSql(name, table_arg, &sql, &error))
    return HA_ERR_UNSUPPORTED;

  if (mkdir(ParquetFileDir(name).c_str(), 0777) != 0 && errno != EEXIST)
    return HA_ERR_INTERNAL_ERROR;

  try
  {
    duckdb::Connection ddl_con(*parquet_db);
    auto result= ddl_con.Query(sql);
    if (result->HasError())
      return HA_ERR_INTERNAL_ERROR;
  }
  catch (const std::exception &e)
  {
    return HA_ERR_INTERNAL_ERROR;
  }

  std::lock_guard<std::mutex> guard(parquet_files_mutex);
  parquet_files[name].clear();
  parquet_file_counter[name]= 0;
  return 0;
}

int ha_parquet::delete_table(const char *name)
{
  try
  {
    duckdb::Connection ddl_con(*parquet_db);
    ddl_con.Query("DROP TABLE IF EXISTS " + parquet::QuoteIdentifier(name));
  }
  catch (const std::exception &e)
  {
    return HA_ERR_INTERNAL_ERROR;
  }

  std::lock_guard<std::mutex> guard(parquet_files_mutex);
  for (const auto &f : parquet_files[name])
    std::remove(f.c_str());
  parquet_files.erase(name);
  parquet_file_counter.erase(name);
  rmdir(ParquetFileDir(name).c_str());
  return 0;
}

int ha_parquet::write_row(const uchar *buf)
{
  std::string error;
  MY_BITMAP *org_bitmap= dbug_tmp_use_all_columns(table, &table->read_set);

  try
  {
    duckdb::Appender appender(*con, duckdb_table_name);
    appender.BeginRow();
    for (Field **field= table->field; *field; field++)
    {
      if (!parquet::AppendMariaDBFieldToDuckDBAppender(*field, &appender, &error))
      {
        dbug_tmp_restore_column_map(&table->read_set, org_bitmap);
        return HA_ERR_UNSUPPORTED;
      }
    }
    appender.EndRow();
    appender.Close();
  }
  catch (const std::exception &e)
  {
    dbug_tmp_restore_column_map(&table->read_set, org_bitmap);
    return HA_ERR_INTERNAL_ERROR;
  }

  dbug_tmp_restore_column_map(&table->read_set, org_bitmap);
  return 0;
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
  std::string sql= "SELECT * FROM " + parquet::QuoteIdentifier(duckdb_table_name);
  {
    std::lock_guard<std::mutex> guard(parquet_files_mutex);
    const auto &files= parquet_files[duckdb_table_name];
    if (!files.empty())
      sql+= " UNION ALL SELECT * FROM " + parquet::BuildDuckDBReadParquetSql(files);
  }

  try
  {
    scan_result= con->Query(sql);
    if (scan_result->HasError())
      return HA_ERR_INTERNAL_ERROR;
  }
  catch (const std::exception &e)
  {
    return HA_ERR_INTERNAL_ERROR;
  }
  current_row= 0;
  return 0;
}

int ha_parquet::rnd_next(uchar *buf)
{
  if (!scan_result || current_row >= scan_result->RowCount())
    return HA_ERR_END_OF_FILE;

  std::string error;
  MY_BITMAP *org_bitmap= dbug_tmp_use_all_columns(table, &table->write_set);
  uint col= 0;

  try
  {
    for (Field **field= table->field; *field; field++, col++)
    {
      duckdb::Value value= scan_result->GetValue(col, current_row);
      if (!parquet::StoreDuckDBValueInMariaDBField(*field, value, &error))
      {
        dbug_tmp_restore_column_map(&table->write_set, org_bitmap);
        return HA_ERR_UNSUPPORTED;
      }
    }
  }
  catch (const std::exception &e)
  {
    dbug_tmp_restore_column_map(&table->write_set, org_bitmap);
    return HA_ERR_INTERNAL_ERROR;
  }

  dbug_tmp_restore_column_map(&table->write_set, org_bitmap);
  current_row++;
  return 0;
}

int ha_parquet::rnd_end()
{
  scan_result.reset();
  current_row= 0;
  return 0;
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
  //flush the write buffer to a real parquet file once mariadb is done
  //with this table for the statement (see extra_docs/MariaDB Locks
  //Info.docx: "for unlock, export remaining row data as parquet file")
  if (lock_type != F_UNLCK)
    return 0;

  try
  {
    auto count_result= con->Query(
        "SELECT COUNT(*) FROM " + parquet::QuoteIdentifier(duckdb_table_name));
    if (count_result->HasError())
      return HA_ERR_INTERNAL_ERROR;
    if (count_result->GetValue<int64_t>(0, 0) == 0)
      return 0;

    std::string file_path;
    {
      std::lock_guard<std::mutex> guard(parquet_files_mutex);
      file_path= ParquetFileDir(duckdb_table_name) + "/data_" +
                 std::to_string(parquet_file_counter[duckdb_table_name]++) +
                 ".parquet";
    }

    auto copy_result= con->Query(parquet::BuildDuckDBCopyToParquetSql(
        parquet::QuoteIdentifier(duckdb_table_name), file_path));
    if (copy_result->HasError())
      return HA_ERR_INTERNAL_ERROR;

    con->Query("DELETE FROM " + parquet::QuoteIdentifier(duckdb_table_name));

    std::lock_guard<std::mutex> guard(parquet_files_mutex);
    parquet_files[duckdb_table_name].push_back(file_path);
  }
  catch (const std::exception &e)
  {
    return HA_ERR_INTERNAL_ERROR;
  }
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
