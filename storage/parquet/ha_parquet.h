#ifndef HA_PARQUET_INCLUDED
#define HA_PARQUET_INCLUDED

#define MYSQL_SERVER 1

#include "handler.h"
#include "thr_lock.h"
#include "my_base.h"
#include "duckdb.hpp"

#include <memory>
#include <string>

class ha_parquet final : public handler
{
public:
  ha_parquet(handlerton *hton, TABLE_SHARE *table_arg);
  ~ha_parquet() override = default;

  ulonglong table_flags() const override;
  ulong index_flags(uint idx, uint part, bool all_parts) const override;

  int open(const char *name, int mode, uint test_if_locked) override;
  int close(void) override;
  int create(const char *name, TABLE *table_arg, HA_CREATE_INFO *create_info) override;
  int delete_table(const char *name) override;

  int write_row(const uchar *buf) override;
  int update_row(const uchar *old_data, const uchar *new_data) override;
  int delete_row(const uchar *buf) override;
  int rnd_init(bool scan) override;
  int rnd_next(uchar *buf) override;
  int rnd_end() override;
  int rnd_pos(uchar *buf, uchar *pos) override;
  void position(const uchar *record) override;
  int info(uint flag) override;

  int external_lock(THD *thd, int lock_type) override;

  THR_LOCK_DATA **store_lock(THD *thd, THR_LOCK_DATA **to,
                             enum thr_lock_type lock_type) override;
  const Item *cond_push(const Item *cond) override;
  void cond_pop() override;

private:
  THR_LOCK_DATA lock;
  std::string parquet_file_path;

  //per-handler connection into the shared in-memory duckdb instance
  std::unique_ptr<duckdb::Connection> con;
  std::string duckdb_table_name;

  std::unique_ptr<duckdb::MaterializedQueryResult> scan_result;
  size_t current_row = 0;
};

#endif
