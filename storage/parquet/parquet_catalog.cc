#include "parquet_catalog.h"

#include "json.hpp"

#include <curl/curl.h>

namespace parquet
{

namespace
{

struct HttpResponse
{
  long status= 0;
  std::string body;
};

size_t WriteCallback(char *ptr, size_t size, size_t nmemb, void *userdata)
{
  size_t bytes= size * nmemb;
  static_cast<std::string *>(userdata)->append(ptr, bytes);
  return bytes;
}

//plain http request (get/post/delete/head), json body in, json body out.
//no aws-style request signing here -- that's only needed for s3, not for
//talking to lakekeeper's own rest api.
HttpResponse DoRequest(const std::string &url, const std::string &method,
                       const std::string &body, const CatalogClientConfig &config)
{
  HttpResponse response;
  CURL *curl= curl_easy_init();
  if (!curl)
  {
    response.status= -1;
    return response;
  }

  struct curl_slist *headers= nullptr;
  headers= curl_slist_append(headers, "Content-Type: application/json");
  headers= curl_slist_append(headers, "Accept: application/json");

  curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
  curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
  curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, WriteCallback);
  curl_easy_setopt(curl, CURLOPT_WRITEDATA, &response.body);
  curl_easy_setopt(curl, CURLOPT_CONNECTTIMEOUT_MS, config.connect_timeout_ms);
  curl_easy_setopt(curl, CURLOPT_TIMEOUT_MS, config.timeout_ms);

  if (method == "POST")
  {
    curl_easy_setopt(curl, CURLOPT_POSTFIELDS, body.c_str());
    curl_easy_setopt(curl, CURLOPT_POSTFIELDSIZE, (long)body.size());
  }
  else if (method == "DELETE")
  {
    curl_easy_setopt(curl, CURLOPT_CUSTOMREQUEST, "DELETE");
  }
  else if (method == "HEAD")
  {
    curl_easy_setopt(curl, CURLOPT_NOBODY, 1L);
  }

  CURLcode res= curl_easy_perform(curl);
  if (res == CURLE_OK)
    curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &response.status);
  else
    response.status= -1;

  curl_slist_free_all(headers);
  curl_easy_cleanup(curl);
  return response;
}

std::string UrlEncode(const std::string &value)
{
  CURL *curl= curl_easy_init();
  char *encoded= curl_easy_escape(curl, value.c_str(), (int)value.size());
  std::string result= encoded ? encoded : value;
  if (encoded)
    curl_free(encoded);
  curl_easy_cleanup(curl);
  return result;
}

//create-table and load-table responses share the same shape:
//{"metadata-location": "...", "metadata": {...}}. pull out what a
//commit needs to know about the table's current state.
bool ParseLoadTableResult(const std::string &body, CatalogLoadTableResult *result)
{
  nlohmann::json parsed= nlohmann::json::parse(body, nullptr, false);
  if (parsed.is_discarded() || !parsed.contains("metadata-location") ||
      !parsed.contains("metadata"))
    return false;

  result->metadata_location= parsed["metadata-location"].get<std::string>();
  const nlohmann::json &metadata= parsed["metadata"];
  result->raw_metadata_json= metadata.dump();
  result->table_uuid= metadata.value("table-uuid", "");
  result->current_schema_id= metadata.value("current-schema-id", 0);
  result->last_sequence_number= metadata.value("last-sequence-number", 0ULL);
  //fresh tables have no snapshot at all yet -- stays empty in that case.
  if (metadata.contains("current-snapshot-id") &&
      !metadata["current-snapshot-id"].is_null())
    result->current_snapshot_id=
        std::to_string(metadata["current-snapshot-id"].get<int64_t>());
  return true;
}

} // namespace

std::string EncodeNamespaceForUrlPath(const CatalogNamespaceIdent &ident,
                                      const std::string &encoded_separator)
{
  std::string encoded;
  for (size_t i= 0; i < ident.parts.size(); i++)
  {
    if (i > 0)
      encoded+= encoded_separator;
    encoded+= UrlEncode(ident.parts[i]);
  }
  return encoded;
}

ParquetCatalogClient::ParquetCatalogClient(CatalogClientConfig config)
  :config_(std::move(config))
{}

CatalogStatus ParquetCatalogClient::BootstrapConfig()
{
  CatalogStatus status;
  //lakekeeper resolves the human-friendly warehouse name we were given
  //into the uuid it actually wants as the rest catalog's {prefix} path
  //segment -- see defaults.prefix / overrides.prefix in the response.
  std::string url= config_.base_uri + "/v1/config?warehouse=" + UrlEncode(config_.warehouse);
  HttpResponse response= DoRequest(url, "GET", "", config_);

  if (response.status != 200)
  {
    status.code= CatalogStatusCode::kTransportError;
    status.http_status= response.status;
    status.message= "GET /v1/config failed: " + response.body;
    return status;
  }

  nlohmann::json parsed= nlohmann::json::parse(response.body, nullptr, false);
  if (parsed.is_discarded())
  {
    status.code= CatalogStatusCode::kInvalidResponse;
    status.message= "could not parse /v1/config response as json";
    return status;
  }

  if (parsed.contains("overrides") && parsed["overrides"].contains("prefix"))
    prefix_= parsed["overrides"]["prefix"].get<std::string>();
  else if (parsed.contains("defaults") && parsed["defaults"].contains("prefix"))
    prefix_= parsed["defaults"]["prefix"].get<std::string>();
  else
  {
    status.code= CatalogStatusCode::kInvalidResponse;
    status.message= "/v1/config response had no prefix for warehouse '" +
                    config_.warehouse + "'";
    return status;
  }

  bootstrapped_= true;
  return status;
}

CatalogStatus ParquetCatalogClient::EnsureNamespace(const CatalogNamespaceIdent &ident)
{
  CatalogStatus status;
  if (!bootstrapped_)
  {
    status.code= CatalogStatusCode::kServerError;
    status.message= "BootstrapConfig() must succeed before EnsureNamespace()";
    return status;
  }

  nlohmann::json body= {{"namespace", ident.parts}};
  std::string url= config_.base_uri + "/v1/" + UrlEncode(prefix_) + "/namespaces";
  HttpResponse response= DoRequest(url, "POST", body.dump(), config_);

  //409 means the namespace is already there, which is exactly what
  //"ensure" is asking for -- not an error.
  if (response.status == 200 || response.status == 409)
    return status;

  status.code= response.status == 400 ? CatalogStatusCode::kInvalidResponse
                                      : CatalogStatusCode::kServerError;
  status.http_status= response.status;
  status.message= "POST /v1/{prefix}/namespaces failed: " + response.body;
  return status;
}

CatalogStatus ParquetCatalogClient::CreateTable(const CatalogCreateTableRequest &request,
                                                CatalogLoadTableResult *result)
{
  CatalogStatus status;
  if (!bootstrapped_)
  {
    status.code= CatalogStatusCode::kServerError;
    status.message= "BootstrapConfig() must succeed before CreateTable()";
    return status;
  }

  nlohmann::json schema= nlohmann::json::parse(request.schema_json, nullptr, false);
  if (schema.is_discarded())
  {
    status.code= CatalogStatusCode::kInvalidResponse;
    status.message= "schema_json was not valid json";
    return status;
  }

  nlohmann::json body= {{"name", request.ident.table_name}, {"schema", schema}};
  std::string namespace_path=
      EncodeNamespaceForUrlPath(request.ident.namespace_ident, namespace_separator_);
  std::string url= config_.base_uri + "/v1/" + UrlEncode(prefix_) + "/namespaces/" +
                   namespace_path + "/tables";
  HttpResponse response= DoRequest(url, "POST", body.dump(), config_);

  if (response.status == 409)
  {
    status.code= CatalogStatusCode::kConflict;
    status.http_status= response.status;
    status.message= "table already exists: " + request.ident.table_name;
    return status;
  }
  if (response.status != 200)
  {
    status.code= CatalogStatusCode::kServerError;
    status.http_status= response.status;
    status.message= "POST .../tables failed: " + response.body;
    return status;
  }

  if (!ParseLoadTableResult(response.body, result))
  {
    status.code= CatalogStatusCode::kInvalidResponse;
    status.message= "create table response was not a valid load-table result";
    return status;
  }
  return status;
}

CatalogStatus ParquetCatalogClient::LoadTable(const CatalogTableIdent &ident,
                                              CatalogLoadTableResult *result)
{
  CatalogStatus status;
  std::string namespace_path=
      EncodeNamespaceForUrlPath(ident.namespace_ident, namespace_separator_);
  std::string url= config_.base_uri + "/v1/" + UrlEncode(prefix_) + "/namespaces/" +
                   namespace_path + "/tables/" + UrlEncode(ident.table_name);
  HttpResponse response= DoRequest(url, "GET", "", config_);

  if (response.status != 200)
  {
    status.code= response.status == 404 ? CatalogStatusCode::kInvalidResponse
                                        : CatalogStatusCode::kServerError;
    status.http_status= response.status;
    status.message= "GET .../tables/{table} failed: " + response.body;
    return status;
  }
  if (!ParseLoadTableResult(response.body, result))
  {
    status.code= CatalogStatusCode::kInvalidResponse;
    status.message= "load table response was not a valid load-table result";
  }
  return status;
}

CatalogStatus ParquetCatalogClient::CommitTable(const CatalogCommitRequest &request,
                                                CatalogLoadTableResult *result)
{
  CatalogStatus status;
  std::string namespace_path= EncodeNamespaceForUrlPath(
      request.ident.namespace_ident, namespace_separator_);
  std::string url= config_.base_uri + "/v1/" + UrlEncode(prefix_) + "/namespaces/" +
                   namespace_path + "/tables/" + UrlEncode(request.ident.table_name);
  HttpResponse response=
      DoRequest(url, "POST", request.commit_request_json, config_);

  if (response.status == 409)
  {
    status.code= CatalogStatusCode::kConflict;
    status.http_status= response.status;
    status.message= "commit lost the optimistic-concurrency race: " + response.body;
    return status;
  }
  if (response.status != 200)
  {
    status.code= CatalogStatusCode::kServerError;
    status.http_status= response.status;
    status.message= "POST .../tables/{table} (commit) failed: " + response.body;
    return status;
  }
  if (!ParseLoadTableResult(response.body, result))
  {
    status.code= CatalogStatusCode::kInvalidResponse;
    status.message= "commit response was not a valid load-table result";
  }
  return status;
}

CatalogStatus ParquetCatalogClient::TableExists(const CatalogTableIdent &ident,
                                                bool *exists)
{
  CatalogStatus status;
  std::string namespace_path=
      EncodeNamespaceForUrlPath(ident.namespace_ident, namespace_separator_);
  std::string url= config_.base_uri + "/v1/" + UrlEncode(prefix_) + "/namespaces/" +
                   namespace_path + "/tables/" + UrlEncode(ident.table_name);
  HttpResponse response= DoRequest(url, "HEAD", "", config_);

  if (response.status == 200)
  {
    *exists= true;
    return status;
  }
  if (response.status == 404)
  {
    *exists= false;
    return status;
  }
  status.code= CatalogStatusCode::kServerError;
  status.http_status= response.status;
  status.message= "HEAD .../tables/{table} failed";
  return status;
}

} // namespace parquet
