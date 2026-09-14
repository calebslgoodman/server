#ifndef PARQUET_CATALOG_INCLUDED
#define PARQUET_CATALOG_INCLUDED

#include <map>
#include <string>
#include <vector>

namespace parquet
{

enum class CatalogStatusCode
{
  kOk,
  kInvalidResponse,
  kConflict,
  kTransportError,
  kServerError
};

struct CatalogStatus
{
  CatalogStatusCode code= CatalogStatusCode::kOk;
  long http_status= 0;
  std::string message;

  bool ok() const { return code == CatalogStatusCode::kOk; }
};

struct CatalogClientConfig
{
  std::string base_uri;
  //warehouse name, e.g. "parquet_warehouse" -- resolved to the rest
  //catalog's prefix (its warehouse id) during BootstrapConfig().
  std::string warehouse;
  long connect_timeout_ms= 10000;
  long timeout_ms= 30000;
};

struct CatalogNamespaceIdent
{
  std::vector<std::string> parts;
};

struct CatalogTableIdent
{
  CatalogNamespaceIdent namespace_ident;
  std::string table_name;
};

struct CatalogCreateTableRequest
{
  CatalogTableIdent ident;
  std::string schema_json;
};

struct CatalogLoadTableResult
{
  std::string metadata_location;
  std::string raw_response_json;
};

//joins namespace parts with the given separator, url-encoding each part
//(matches the iceberg rest spec's multi-level namespace path convention).
std::string EncodeNamespaceForUrlPath(const CatalogNamespaceIdent &ident,
                                      const std::string &encoded_separator);

class ParquetCatalogClient
{
public:
  explicit ParquetCatalogClient(CatalogClientConfig config);

  //GET /v1/config, then resolves config_.warehouse (a name) to the
  //warehouse id lakekeeper actually wants as the rest catalog prefix via
  //its management api. must be called once before any other method.
  CatalogStatus BootstrapConfig();

  CatalogStatus EnsureNamespace(const CatalogNamespaceIdent &ident);

  CatalogStatus CreateTable(const CatalogCreateTableRequest &request,
                            CatalogLoadTableResult *result);

  CatalogStatus TableExists(const CatalogTableIdent &ident, bool *exists);

  const std::string &prefix() const { return prefix_; }

private:
  CatalogClientConfig config_;
  std::string namespace_separator_= "%1F";
  std::string prefix_;
  bool bootstrapped_= false;
};

} // namespace parquet

#endif
