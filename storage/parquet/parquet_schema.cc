#include "parquet_schema.h"
#include "field.h"
#include "table.h"

namespace parquet
{

std::string QuoteIdentifier(const std::string &identifier)
{
  std::string quoted= "\"";
  for (char c : identifier)
  {
    if (c == '"')
      quoted+= '"';
    quoted+= c;
  }
  quoted+= '"';
  return quoted;
}

static std::string QuoteStringLiteral(const std::string &value)
{
  std::string quoted= "'";
  for (char c : value)
  {
    if (c == '\'')
      quoted+= '\'';
    quoted+= c;
  }
  quoted+= '\'';
  return quoted;
}

std::string BuildDuckDBStringList(const std::vector<std::string> &values)
{
  std::string list= "[";
  for (size_t i= 0; i < values.size(); i++)
  {
    if (i > 0)
      list+= ", ";
    list+= QuoteStringLiteral(values[i]);
  }
  list+= "]";
  return list;
}

std::string BuildDuckDBReadParquetSql(const std::vector<std::string> &paths)
{
  return "read_parquet(" + BuildDuckDBStringList(paths) + ")";
}

std::string BuildDuckDBCopyToParquetSql(const std::string &table_or_query,
                                        const std::string &path)
{
  return "COPY " + table_or_query + " TO " + QuoteStringLiteral(path) +
         " (FORMAT PARQUET)";
}

bool MariaDBFieldToDuckDBType(Field *field, std::string *duckdb_type, std::string *error)
{
  switch (field->type())
  {
    case MYSQL_TYPE_TINY:
    case MYSQL_TYPE_SHORT:
    case MYSQL_TYPE_INT24:
    case MYSQL_TYPE_LONG:
    case MYSQL_TYPE_LONGLONG:
      *duckdb_type= "BIGINT";
      return true;
    case MYSQL_TYPE_FLOAT:
    case MYSQL_TYPE_DOUBLE:
      *duckdb_type= "DOUBLE";
      return true;
    case MYSQL_TYPE_VARCHAR:
    case MYSQL_TYPE_VAR_STRING:
    case MYSQL_TYPE_STRING:
      *duckdb_type= "VARCHAR";
      return true;
    default:
      //decimal/date/blob/etc aren't wired up yet, extend as we need them
      *error= std::string("unsupported column type for field '") +
              field->field_name.str + "'";
      return false;
  }
}

bool BuildDuckDBCreateTableSql(const std::string &table_name, TABLE *table,
                               std::string *sql, std::string *error)
{
  std::string columns;
  for (Field **field= table->field; *field; field++)
  {
    std::string duckdb_type;
    if (!MariaDBFieldToDuckDBType(*field, &duckdb_type, error))
      return false;
    if (!columns.empty())
      columns+= ", ";
    columns+= QuoteIdentifier((*field)->field_name.str) + " " + duckdb_type;
  }
  columns+= ", " PARQUET_TXN_ID_COLUMN " BIGINT";
  *sql= "CREATE TABLE IF NOT EXISTS " + QuoteIdentifier(table_name) +
        " (" + columns + ")";
  return true;
}

bool AppendMariaDBFieldToDuckDBAppender(Field *field, duckdb::Appender *appender,
                                        std::string *error)
{
  if (field->is_null())
  {
    appender->Append<std::nullptr_t>(nullptr);
    return true;
  }
  switch (field->type())
  {
    case MYSQL_TYPE_TINY:
    case MYSQL_TYPE_SHORT:
    case MYSQL_TYPE_INT24:
    case MYSQL_TYPE_LONG:
    case MYSQL_TYPE_LONGLONG:
      appender->Append<int64_t>(field->val_int());
      return true;
    case MYSQL_TYPE_FLOAT:
    case MYSQL_TYPE_DOUBLE:
      appender->Append<double>(field->val_real());
      return true;
    case MYSQL_TYPE_VARCHAR:
    case MYSQL_TYPE_VAR_STRING:
    case MYSQL_TYPE_STRING:
    {
      String buf;
      String *s= field->val_str(&buf);
      appender->Append(s->ptr(), (uint32_t)s->length());
      return true;
    }
    default:
      *error= "unsupported column type in write_row";
      return false;
  }
}

bool StoreDuckDBValueInMariaDBField(Field *field, const duckdb::Value &value,
                                    std::string *error)
{
  if (value.IsNull())
  {
    field->set_null();
    return true;
  }
  field->set_notnull();
  switch (field->type())
  {
    case MYSQL_TYPE_TINY:
    case MYSQL_TYPE_SHORT:
    case MYSQL_TYPE_INT24:
    case MYSQL_TYPE_LONG:
    case MYSQL_TYPE_LONGLONG:
      field->store(value.GetValue<int64_t>(), false);
      return true;
    case MYSQL_TYPE_FLOAT:
    case MYSQL_TYPE_DOUBLE:
      field->store(value.GetValue<double>());
      return true;
    case MYSQL_TYPE_VARCHAR:
    case MYSQL_TYPE_VAR_STRING:
    case MYSQL_TYPE_STRING:
    {
      std::string s= value.GetValue<std::string>();
      field->store(s.data(), s.length(), field->charset());
      return true;
    }
    default:
      *error= "unsupported column type in rnd_next";
      return false;
  }
}

} // namespace parquet
