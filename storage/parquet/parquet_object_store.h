#ifndef PARQUET_OBJECT_STORE_INCLUDED
#define PARQUET_OBJECT_STORE_INCLUDED

#include <string>

namespace parquet
{

struct ObjectStoreConfig
{
  //s3-compatible endpoint, e.g. "http://127.0.0.1:9000" for a local minio.
  //empty means s3 upload is disabled and files stay local-only.
  std::string endpoint;
  std::string bucket;
  std::string region;
  std::string access_key;
  std::string secret_key;

  bool enabled() const { return !endpoint.empty() && !bucket.empty(); }
};

//uploads a local file to <endpoint>/<bucket>/<key> using curl's built-in
//aws sigv4 signing (no aws sdk dependency).
bool UploadFileToS3(const std::string &local_path, const std::string &key,
                    const ObjectStoreConfig &config, std::string *error);

//downloads <endpoint>/<bucket>/<key> to a local file, same signing.
bool DownloadFileFromS3(const std::string &key, const std::string &local_path,
                        const ObjectStoreConfig &config, std::string *error);

//key accepts either a bare object key or a full "s3://bucket/key" uri
//(iceberg metadata always stores the latter) -- strips the bucket/
//scheme back off before downloading, since our signer just wants a key.
bool DownloadFileFromS3Uri(const std::string &uri, const std::string &local_path,
                           const ObjectStoreConfig &config, std::string *error);

} // namespace parquet

#endif
