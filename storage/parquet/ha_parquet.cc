#include <my_global.h>
#include <mysql/plugin.h>
#include "ha_parquet.h"
#include "sql_class.h"
#include "field.h"
#include "log.h"
#include "parquet_schema.h"
#include "parquet_object_store.h"

#include <sys/stat.h>
#include <chrono>
#include <cstdio>
#include <map>
#include <mutex>
#include <vector>

handlerton *parquet_hton;

//one lock shared by every parquet table until day 9's real locking work
static THR_LOCK parquet_lock;

//write_row buffers into an in-memory duckdb table (per open table name);
//external_lock's unlock flushes that buffer out to a real .parquet file
//(and, if configured, uploads it to s3) once it's held enough rows or
//gone stale for long enough. flushed file paths are remembered here so
//rnd_init can read them back.
static duckdb::DuckDB *parquet_db;
static std::mutex parquet_files_mutex;
static std::map<std::string, std::vector<std::string>> parquet_files;
static std::map<std::string, uint64_t> parquet_file_counter;
static std::map<std::string, uint64_t> parquet_buffered_rows;
static std::map<std::string, std::chrono::steady_clock::time_point> parquet_last_flush;

static char *parquet_s3_endpoint;
static char *parquet_s3_bucket;
static char *parquet_s3_region;
static char *parquet_s3_access_key;
static char *parquet_s3_secret_key;
static unsigned long parquet_write_buffer_max_rows;
static unsigned long parquet_write_buffer_flush_interval_ms;

static std::string ParquetFileDir(const std::string &table_path)
{
  return table_path + "_parquet";
}

static parquet::ObjectStoreConfig CurrentS3Config()
{
  return {parquet_s3_endpoint, parquet_s3_bucket, parquet_s3_region,
          parquet_s3_access_key, parquet_s3_secret_key};
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
  parquet_buffered_rows[name]= 0;
  parquet_last_flush[name]= std::chrono::steady_clock::now();
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
  parquet_buffered_rows.erase(name);
  parquet_last_flush.erase(name);
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

  std::lock_guard<std::mutex> guard(parquet_files_mutex);
  parquet_buffered_rows[duckdb_table_name]++;
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
  //mariadb calls this to release the table at the end of a statement (see
  //extra_docs/MariaDB Locks Info.docx). that's our hook to check whether
  //the write buffer has earned a flush yet -- not every unlock flushes,
  //only once row-count or staleness thresholds are crossed (day 5;
  //day 3/4 flushed unconditionally on every unlock instead).
  if (lock_type != F_UNLCK)
    return 0;

  try
  {
    bool should_flush;
    {
      std::lock_guard<std::mutex> guard(parquet_files_mutex);
      uint64_t rows= parquet_buffered_rows[duckdb_table_name];
      if (rows == 0)
        return 0;
      auto elapsed= std::chrono::steady_clock::now() -
                    parquet_last_flush[duckdb_table_name];
      auto elapsed_ms=
          std::chrono::duration_cast<std::chrono::milliseconds>(elapsed).count();
      should_flush= rows >= parquet_write_buffer_max_rows ||
                    elapsed_ms >= (int64_t)parquet_write_buffer_flush_interval_ms;
    }
    if (!should_flush)
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

    parquet::ObjectStoreConfig s3_config= CurrentS3Config();
    if (s3_config.enabled())
    {
      std::string upload_error;
      if (!parquet::UploadFileToS3(file_path, file_path, s3_config, &upload_error))
      {
        //local file is still valid and still tracked below -- staying
        //local-only on a failed upload beats losing the write entirely.
        //real retry/alerting is day 9 hardening work.
        sql_print_warning("parquet: s3 upload of %s failed: %s",
                          file_path.c_str(), upload_error.c_str());
      }
    }

    std::lock_guard<std::mutex> guard(parquet_files_mutex);
    parquet_files[duckdb_table_name].push_back(file_path);
    parquet_buffered_rows[duckdb_table_name]= 0;
    parquet_last_flush[duckdb_table_name]= std::chrono::steady_clock::now();
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

static MYSQL_SYSVAR_STR(s3_endpoint, parquet_s3_endpoint,
  PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_MEMALLOC,
  "S3-compatible endpoint (e.g. http://127.0.0.1:9000 for a local minio). "
  "Empty disables S3 upload; flushed files stay local-only.",
  NULL, NULL, "");

static MYSQL_SYSVAR_STR(s3_bucket, parquet_s3_bucket,
  PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_MEMALLOC,
  "S3 bucket that flushed parquet files get uploaded to.",
  NULL, NULL, "");

static MYSQL_SYSVAR_STR(s3_region, parquet_s3_region,
  PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_MEMALLOC,
  "S3 region used for sigv4 request signing.",
  NULL, NULL, "us-east-1");

static MYSQL_SYSVAR_STR(s3_access_key, parquet_s3_access_key,
  PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_MEMALLOC,
  "S3 access key id.",
  NULL, NULL, "");

static MYSQL_SYSVAR_STR(s3_secret_key, parquet_s3_secret_key,
  PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_MEMALLOC,
  "S3 secret access key.",
  NULL, NULL, "");

static MYSQL_SYSVAR_ULONG(write_buffer_max_rows, parquet_write_buffer_max_rows,
  PLUGIN_VAR_RQCMDARG,
  "Flush a table's write buffer to a parquet file once it holds this many rows.",
  NULL, NULL, 100000, 1, ULONG_MAX, 1);

static MYSQL_SYSVAR_ULONG(write_buffer_flush_interval_ms,
  parquet_write_buffer_flush_interval_ms, PLUGIN_VAR_RQCMDARG,
  "Flush a table's write buffer if it has held rows for at least this "
  "many milliseconds, even below the row-count threshold.",
  NULL, NULL, 30000, 100, ULONG_MAX, 1);

static struct st_mysql_sys_var *parquet_system_variables[]=
{
  MYSQL_SYSVAR(s3_endpoint),
  MYSQL_SYSVAR(s3_bucket),
  MYSQL_SYSVAR(s3_region),
  MYSQL_SYSVAR(s3_access_key),
  MYSQL_SYSVAR(s3_secret_key),
  MYSQL_SYSVAR(write_buffer_max_rows),
  MYSQL_SYSVAR(write_buffer_flush_interval_ms),
  NULL
};

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
  parquet_system_variables,
  "0.1",
  MariaDB_PLUGIN_MATURITY_EXPERIMENTAL
}
maria_declare_plugin_end;
