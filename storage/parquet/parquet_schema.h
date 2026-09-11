#ifndef PARQUET_SCHEMA_INCLUDED
#define PARQUET_SCHEMA_INCLUDED

#define MYSQL_SERVER 1

#include "duckdb.hpp"

#include <string>
#include <vector>

class Field;
struct TABLE;
typedef unsigned char uchar;

//hidden column on the duckdb write buffer holding the mariadb statement
//(THD::query_id) that inserted each row, so a rollback can delete just
//that statement's rows without touching other buffered data. excluded
//from every read/flush via "SELECT * EXCLUDE (PARQUET_TXN_ID_COLUMN)".
#define PARQUET_TXN_ID_COLUMN "_parquet_txn_id"

namespace parquet
{

std::string QuoteIdentifier(const std::string &identifier);

//builds a duckdb list literal, e.g. ['a', 'b'], quoting/escaping each value.
std::string BuildDuckDBStringList(const std::vector<std::string> &values);

//builds "read_parquet([...])" over the given file paths.
std::string BuildDuckDBReadParquetSql(const std::vector<std::string> &paths);

//builds "COPY <table_or_query> TO '<path>' (FORMAT PARQUET)".
std::string BuildDuckDBCopyToParquetSql(const std::string &table_or_query,
                                        const std::string &path);

//maps a mariadb column to the duckdb type used to store it. returns
//false and fills *error if the field's type isn't supported yet.
bool MariaDBFieldToDuckDBType(Field *field, std::string *duckdb_type, std::string *error);

//builds "CREATE TABLE IF NOT EXISTS <name> (...)" from a mariadb TABLE.
bool BuildDuckDBCreateTableSql(const std::string &table_name, TABLE *table,
                               std::string *sql, std::string *error);

//appends the current value of one field (read from table->record[0] via
//the field itself) onto an in-progress duckdb appender row.
bool AppendMariaDBFieldToDuckDBAppender(Field *field, duckdb::Appender *appender,
                                        std::string *error);

//stores one duckdb value back into a mariadb field.
bool StoreDuckDBValueInMariaDBField(Field *field, const duckdb::Value &value,
                                    std::string *error);

} // namespace parquet

#endif
