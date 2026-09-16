#include <my_global.h>
#include <mysql/plugin.h>
#include "ha_parquet.h"
#include "sql_class.h"
#include "field.h"
#include "log.h"
#include "parquet_schema.h"
#include "parquet_object_store.h"
#include "parquet_catalog.h"
#include "parquet_iceberg.h"

#include <sys/stat.h>
#include <chrono>
#include <cstdio>
#include <map>
#include <mutex>
#include <set>
#include <vector>

handlerton *parquet_hton;

//one lock shared by every parquet table until day 9's real locking work
static THR_LOCK parquet_lock;

//write_row buffers into an in-memory duckdb table (per open table name).
//parquet_commit flushes that buffer out to a real .parquet file (and, if
//configured, uploads it to s3) once it's held enough rows or gone stale
//for long enough -- but only once mariadb confirms the statement that
//wrote those rows actually succeeded. parquet_rollback discards just
//that statement's rows otherwise. flushed file paths are remembered
//here so rnd_init can read them back.
static duckdb::DuckDB *parquet_db;
static std::mutex parquet_files_mutex;
static std::map<std::string, std::vector<parquet::CatalogDataFile>> parquet_files;
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

//empty base uri (the default) disables iceberg registration entirely,
//same "off unless configured" pattern as the s3 sysvars above.
static char *parquet_catalog_base_uri;
static char *parquet_catalog_warehouse;

//splits a mariadb table path like ".../testdb/t1" into ("testdb", "t1").
static bool SplitTableIdent(const std::string &name, std::string *db, std::string *table)
{
  size_t last_slash= name.find_last_of('/');
  if (last_slash == std::string::npos || last_slash == 0)
    return false;
  *table= name.substr(last_slash + 1);
  size_t second_last= name.find_last_of('/', last_slash - 1);
  *db= second_last == std::string::npos
          ? name.substr(0, last_slash)
          : name.substr(second_last + 1, last_slash - second_last - 1);
  return !db->empty() && !table->empty();
}

//registers name as a real iceberg table in the rest catalog (lakekeeper),
//so a real snapshot-bearing table exists there, not just something we
//track in our own in-process maps. no-op if no catalog is configured.
static bool RegisterTableWithCatalog(const std::string &name, TABLE *table_arg,
                                     std::string *error)
{
  if (parquet_catalog_base_uri[0] == '\0' || parquet_catalog_warehouse[0] == '\0')
    return true;

  std::string db_name, table_ident;
  if (!SplitTableIdent(name, &db_name, &table_ident))
  {
    *error= "could not derive a namespace/table name from '" + name + "'";
    return false;
  }

  parquet::CatalogClientConfig cat_config;
  cat_config.base_uri= parquet_catalog_base_uri;
  cat_config.warehouse= parquet_catalog_warehouse;
  parquet::ParquetCatalogClient catalog(cat_config);

  parquet::CatalogStatus status= catalog.BootstrapConfig();
  if (!status.ok())
  {
    *error= "catalog bootstrap failed: " + status.message;
    return false;
  }

  parquet::CatalogNamespaceIdent ns;
  ns.parts= {db_name};
  status= catalog.EnsureNamespace(ns);
  if (!status.ok())
  {
    *error= "EnsureNamespace failed: " + status.message;
    return false;
  }

  std::string schema_json;
  if (!parquet::BuildIcebergSchemaJson(table_arg, 0, &schema_json, error))
    return false;

  parquet::CatalogCreateTableRequest request;
  request.ident.namespace_ident= ns;
  request.ident.table_name= table_ident;
  request.schema_json= schema_json;
  parquet::CatalogLoadTableResult result;
  status= catalog.CreateTable(request, &result);
  //a stale catalog entry from an earlier drop/recreate cycle (day 7 doesn't
  //clean up the catalog on DROP TABLE yet) shouldn't block re-creating the
  //mariadb-side table.
  if (!status.ok() && status.code != parquet::CatalogStatusCode::kConflict)
  {
    *error= "CreateTable failed: " + status.message;
    return false;
  }
  return true;
}

//per-thd "mailbox" (see extra_docs/MariaDB System Design.docx) tracking
//which tables this statement/transaction wrote to, so the handlerton
//commit/rollback hooks -- which only get a THD*, not a handler -- know
//what to flush or clean up. stage 1 scope: trans_register_ha is always
//called with all=false (statement-level only, matching Ayush's team's
//own staging), so an explicit multi-statement BEGIN...ROLLBACK can't
//undo earlier statements in the same transaction that already
//committed -- only the statement currently in flight. real
//transaction-level (all=true) atomicity is stage 3/2pc territory.
struct ParquetTxnState
{
  std::set<std::string> tables;
};

static std::string ParquetFileDir(const std::string &table_path)
{
  return table_path + "_parquet";
}

static parquet::ObjectStoreConfig CurrentS3Config()
{
  return {parquet_s3_endpoint, parquet_s3_bucket, parquet_s3_region,
          parquet_s3_access_key, parquet_s3_secret_key};
}

//commits new_file into table_name's iceberg table as a new snapshot:
//loads the table's current state from the catalog, builds a manifest +
//manifest-list covering the previously-committed files (from
//parquet_files, which does not yet include new_file at this point --
//see the call site) plus new_file, uploads those two avro files to s3
//next to the data file, then commits. on success, fills in new_file's
//added_in_snapshot_id/added_in_sequence_number so a later commit that
//rewrites the manifest again encodes this file correctly as "existing".
static bool CommitFlushedFileToIceberg(const std::string &table_name,
                                       parquet::CatalogDataFile *new_file,
                                       std::string *error)
{
  std::string db_name, table_ident;
  if (!SplitTableIdent(table_name, &db_name, &table_ident))
  {
    *error= "could not derive a namespace/table name from '" + table_name + "'";
    return false;
  }

  parquet::CatalogClientConfig cat_config;
  cat_config.base_uri= parquet_catalog_base_uri;
  cat_config.warehouse= parquet_catalog_warehouse;
  parquet::ParquetCatalogClient catalog(cat_config);

  parquet::CatalogStatus status= catalog.BootstrapConfig();
  if (!status.ok())
  {
    *error= "catalog bootstrap failed: " + status.message;
    return false;
  }

  parquet::CatalogTableIdent ident;
  ident.namespace_ident.parts= {db_name};
  ident.table_name= table_ident;

  parquet::CatalogLoadTableResult load_result;
  status= catalog.LoadTable(ident, &load_result);
  if (!status.ok())
  {
    *error= "LoadTable failed: " + status.message;
    return false;
  }

  std::vector<parquet::CatalogDataFile> existing_files;
  {
    std::lock_guard<std::mutex> guard(parquet_files_mutex);
    existing_files= parquet_files[table_name];
  }

  parquet::IcebergCommitArtifacts artifacts;
  if (!parquet::BuildIcebergCommitArtifacts(ident, load_result, existing_files, *new_file,
                                            parquet_s3_bucket, ParquetFileDir(table_name),
                                            &artifacts, error))
    return false;

  parquet::ObjectStoreConfig s3_config= CurrentS3Config();
  if (!parquet::UploadFileToS3(artifacts.manifest_local_path, artifacts.manifest_key,
                               s3_config, error) ||
      !parquet::UploadFileToS3(artifacts.manifest_list_local_path,
                               artifacts.manifest_list_key, s3_config, error))
    return false;

  parquet::CatalogCommitRequest commit_request;
  commit_request.ident= ident;
  commit_request.commit_request_json= artifacts.commit_request_json;
  parquet::CatalogLoadTableResult commit_result;
  status= catalog.CommitTable(commit_request, &commit_result);
  if (!status.ok())
  {
    *error= "CommitTable failed: " + status.message;
    return false;
  }

  //day 9: prove we can read the committed snapshot's manifest-list
  //location back out of the table metadata json lakekeeper just
  //returned, independent of the local path we already know we just
  //uploaded it to.
  sql_print_information(
      "parquet: %s committed snapshot %s, manifest-list at %s",
      table_name.c_str(), commit_result.current_snapshot_id.c_str(),
      commit_result.current_snapshot_manifest_list.c_str());

  new_file->added_in_snapshot_id= artifacts.snapshot_id;
  new_file->added_in_sequence_number= artifacts.sequence_number;
  return true;
}

//day 10: resolves table_name's actual data file list purely by reading
//iceberg's own metadata -- LoadTable for the current snapshot's
//manifest-list location, download + decode that (avro), download +
//decode every manifest it points at (avro) -- and logs it, entirely
//independent of our own parquet_files bookkeeping. reads still go
//through parquet_files for now; wiring this into rnd_init is day 11.
static void LogActiveFilesFromIceberg(const std::string &table_name)
{
  std::string db_name, table_ident;
  if (!SplitTableIdent(table_name, &db_name, &table_ident))
    return;

  parquet::CatalogClientConfig cat_config;
  cat_config.base_uri= parquet_catalog_base_uri;
  cat_config.warehouse= parquet_catalog_warehouse;
  parquet::ParquetCatalogClient catalog(cat_config);

  parquet::CatalogStatus status= catalog.BootstrapConfig();
  if (!status.ok())
  {
    sql_print_warning("parquet: iceberg bootstrap for %s failed: %s",
                      table_name.c_str(), status.message.c_str());
    return;
  }

  parquet::CatalogTableIdent ident;
  ident.namespace_ident.parts= {db_name};
  ident.table_name= table_ident;
  parquet::CatalogLoadTableResult load_result;
  status= catalog.LoadTable(ident, &load_result);
  if (!status.ok())
  {
    sql_print_warning("parquet: iceberg LoadTable for %s failed: %s",
                      table_name.c_str(), status.message.c_str());
    return;
  }

  std::vector<parquet::CatalogDataFile> files;
  std::string error;
  if (!parquet::ResolveActiveDataFilesFromIceberg(load_result, CurrentS3Config(),
                                                  ParquetFileDir(table_name), &files,
                                                  &error))
  {
    sql_print_warning("parquet: resolving %s's files from iceberg failed: %s",
                      table_name.c_str(), error.c_str());
    return;
  }

  std::string file_list;
  for (const auto &f : files)
    file_list+= (file_list.empty() ? "" : ", ") + f.path;
  sql_print_information("parquet: %s has %zu active file(s) per iceberg: %s",
                        table_name.c_str(), files.size(), file_list.c_str());
}

//flushes table_name's write buffer to a new local parquet file (and
//uploads it to s3, if configured) if it has crossed the row-count or
//staleness threshold. no-op otherwise. called from parquet_commit once
//mariadb confirms the writing statement succeeded.
static void FlushIfThresholdMet(const std::string &table_name)
{
  try
  {
    bool should_flush;
    uint64_t rows;
    {
      std::lock_guard<std::mutex> guard(parquet_files_mutex);
      rows= parquet_buffered_rows[table_name];
      if (rows == 0)
        return;
      auto elapsed= std::chrono::steady_clock::now() - parquet_last_flush[table_name];
      auto elapsed_ms=
          std::chrono::duration_cast<std::chrono::milliseconds>(elapsed).count();
      should_flush= rows >= parquet_write_buffer_max_rows ||
                    elapsed_ms >= (int64_t)parquet_write_buffer_flush_interval_ms;
    }
    if (!should_flush)
      return;

    std::string file_path;
    {
      std::lock_guard<std::mutex> guard(parquet_files_mutex);
      file_path= ParquetFileDir(table_name) + "/data_" +
                 std::to_string(parquet_file_counter[table_name]++) + ".parquet";
    }

    duckdb::Connection flush_con(*parquet_db);
    std::string real_columns= "(SELECT * EXCLUDE (" PARQUET_TXN_ID_COLUMN ") FROM " +
                              parquet::QuoteIdentifier(table_name) + ")";
    auto copy_result=
        flush_con.Query(parquet::BuildDuckDBCopyToParquetSql(real_columns, file_path));
    if (copy_result->HasError())
    {
      sql_print_warning("parquet: flush of %s failed: %s", table_name.c_str(),
                        copy_result->GetError().c_str());
      return;
    }

    flush_con.Query("DELETE FROM " + parquet::QuoteIdentifier(table_name));

    struct stat file_stat;
    parquet::CatalogDataFile new_file;
    new_file.path= file_path;
    new_file.record_count= rows;
    new_file.file_size_bytes= stat(file_path.c_str(), &file_stat) == 0
                                  ? (uint64_t)file_stat.st_size
                                  : 0;

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

    //day 8: commit this file into the iceberg table as a new snapshot,
    //if a catalog is configured. needs s3 too -- the manifest/
    //manifest-list have to point at real s3:// locations, and there's
    //no catalog-configured-but-no-s3 case worth supporting.
    if (parquet_catalog_base_uri[0] != '\0' && parquet_catalog_warehouse[0] != '\0' &&
        s3_config.enabled())
    {
      std::string commit_error;
      if (!CommitFlushedFileToIceberg(table_name, &new_file, &commit_error))
        sql_print_warning("parquet: iceberg commit for %s failed: %s",
                          file_path.c_str(), commit_error.c_str());
    }

    std::lock_guard<std::mutex> guard(parquet_files_mutex);
    parquet_files[table_name].push_back(new_file);
    parquet_buffered_rows[table_name]= 0;
    parquet_last_flush[table_name]= std::chrono::steady_clock::now();
  }
  catch (const std::exception &e)
  {
    sql_print_warning("parquet: flush of %s failed: %s", table_name.c_str(), e.what());
  }
}

static int parquet_commit(THD *thd, bool all)
{
  ParquetTxnState *txn= (ParquetTxnState *)thd_get_ha_data(thd, parquet_hton);
  if (!txn)
    return 0;

  for (const auto &table_name : txn->tables)
    FlushIfThresholdMet(table_name);

  delete txn;
  thd_set_ha_data(thd, parquet_hton, NULL);
  return 0;
}

static int parquet_rollback(THD *thd, bool all)
{
  ParquetTxnState *txn= (ParquetTxnState *)thd_get_ha_data(thd, parquet_hton);
  if (!txn)
    return 0;

  for (const auto &table_name : txn->tables)
  {
    try
    {
      duckdb::Connection rollback_con(*parquet_db);
      rollback_con.Query("DELETE FROM " + parquet::QuoteIdentifier(table_name) +
                         " WHERE " PARQUET_TXN_ID_COLUMN " = " +
                         std::to_string((int64_t)thd->query_id));

      auto count_result=
          rollback_con.Query("SELECT COUNT(*) FROM " + parquet::QuoteIdentifier(table_name));
      std::lock_guard<std::mutex> guard(parquet_files_mutex);
      if (!count_result->HasError())
        parquet_buffered_rows[table_name]= count_result->GetValue<int64_t>(0, 0);
    }
    catch (const std::exception &e)
    {
      sql_print_warning("parquet: rollback cleanup of %s failed: %s",
                        table_name.c_str(), e.what());
    }
  }

  delete txn;
  thd_set_ha_data(thd, parquet_hton, NULL);
  return 0;
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
  parquet_hton->commit= parquet_commit;
  parquet_hton->rollback= parquet_rollback;
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

  if (parquet_catalog_base_uri[0] != '\0' && parquet_catalog_warehouse[0] != '\0' &&
      CurrentS3Config().enabled())
    LogActiveFilesFromIceberg(name);

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

  //register a real iceberg table with the rest catalog, if one is
  //configured -- day 7. unlike the s3 upload step, this failing means
  //CREATE TABLE fails: the whole point of today is that the table
  //genuinely exists in the catalog, not just in our own bookkeeping.
  if (!RegisterTableWithCatalog(name, table_arg, &error))
  {
    sql_print_warning("parquet: iceberg catalog registration failed for %s: %s",
                      name, error.c_str());
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
    std::remove(f.path.c_str());
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
    appender.Append<int64_t>((int64_t)ha_thd()->query_id);
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
  std::string sql= "SELECT * EXCLUDE (" PARQUET_TXN_ID_COLUMN ") FROM " +
                    parquet::QuoteIdentifier(duckdb_table_name);
  {
    std::lock_guard<std::mutex> guard(parquet_files_mutex);
    const auto &files= parquet_files[duckdb_table_name];
    if (!files.empty())
    {
      std::vector<std::string> paths;
      for (const auto &f : files)
        paths.push_back(f.path);
      sql+= " UNION ALL SELECT * FROM " + parquet::BuildDuckDBReadParquetSql(paths);
    }
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
  //day 4/5 used to flush from here on unlock -- but unlock fires whether
  //the statement succeeded or failed, so a statement that errored out
  //partway would still get its rows written to a parquet file as if it
  //had succeeded. real commit/rollback (parquet_commit/parquet_rollback
  //below) only fire on the outcome mariadb actually reached, so flushing
  //moved there. this hook now just registers with the transaction
  //coordinator and records that duckdb_table_name was touched, so those
  //hooks -- which only get a THD*, not a handler -- know what to do.
  if (lock_type == F_UNLCK)
    return 0;

  trans_register_ha(thd, false, parquet_hton, 0);

  ParquetTxnState *txn= (ParquetTxnState *)thd_get_ha_data(thd, parquet_hton);
  if (!txn)
  {
    txn= new ParquetTxnState();
    thd_set_ha_data(thd, parquet_hton, txn);
  }
  txn->tables.insert(duckdb_table_name);
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

static MYSQL_SYSVAR_STR(catalog_base_uri, parquet_catalog_base_uri,
  PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_MEMALLOC,
  "Iceberg REST catalog base uri (e.g. http://127.0.0.1:8181/catalog for "
  "a local lakekeeper). Empty disables iceberg registration.",
  NULL, NULL, "");

static MYSQL_SYSVAR_STR(catalog_warehouse, parquet_catalog_warehouse,
  PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_MEMALLOC,
  "Name of the warehouse to register tables under in the rest catalog.",
  NULL, NULL, "");

static struct st_mysql_sys_var *parquet_system_variables[]=
{
  MYSQL_SYSVAR(s3_endpoint),
  MYSQL_SYSVAR(s3_bucket),
  MYSQL_SYSVAR(s3_region),
  MYSQL_SYSVAR(s3_access_key),
  MYSQL_SYSVAR(s3_secret_key),
  MYSQL_SYSVAR(write_buffer_max_rows),
  MYSQL_SYSVAR(write_buffer_flush_interval_ms),
  MYSQL_SYSVAR(catalog_base_uri),
  MYSQL_SYSVAR(catalog_warehouse),
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
