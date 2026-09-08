#ifndef PARQUET_SCHEMA_INCLUDED
#define PARQUET_SCHEMA_INCLUDED

#define MYSQL_SERVER 1

#include "duckdb.hpp"

#include <string>

class Field;
struct TABLE;
typedef unsigned char uchar;

namespace parquet
{

std::string QuoteIdentifier(const std::string &identifier);

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
