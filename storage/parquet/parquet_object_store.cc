#include "parquet_object_store.h"

#include <curl/curl.h>

#include <cstring>
#include <fstream>
#include <sstream>

namespace parquet
{

namespace
{

struct UploadContext
{
  const char *data;
  size_t size;
  size_t offset;
};

size_t ReadCallback(char *buffer, size_t size, size_t nitems, void *userdata)
{
  UploadContext *ctx= static_cast<UploadContext *>(userdata);
  size_t want= size * nitems;
  size_t remaining= ctx->size - ctx->offset;
  if (want > remaining)
    want= remaining;
  memcpy(buffer, ctx->data + ctx->offset, want);
  ctx->offset+= want;
  return want;
}

size_t WriteCallback(char *ptr, size_t size, size_t nmemb, void *userdata)
{
  size_t bytes= size * nmemb;
  static_cast<std::string *>(userdata)->append(ptr, bytes);
  return bytes;
}

} // namespace

bool UploadFileToS3(const std::string &local_path, const std::string &key,
                    const ObjectStoreConfig &config, std::string *error)
{
  std::ifstream file(local_path, std::ios::binary);
  if (!file)
  {
    *error= "could not open " + local_path + " for upload";
    return false;
  }
  std::ostringstream body_stream;
  body_stream << file.rdbuf();
  std::string body= body_stream.str();

  UploadContext ctx{body.data(), body.size(), 0};

  CURL *curl= curl_easy_init();
  if (!curl)
  {
    *error= "curl_easy_init failed";
    return false;
  }

  std::string url= config.endpoint + "/" + config.bucket + "/" + key;
  std::string sigv4= "aws:amz:" + config.region + ":s3";
  std::string userpwd= config.access_key + ":" + config.secret_key;

  curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
  curl_easy_setopt(curl, CURLOPT_UPLOAD, 1L);
  curl_easy_setopt(curl, CURLOPT_READFUNCTION, ReadCallback);
  curl_easy_setopt(curl, CURLOPT_READDATA, &ctx);
  curl_easy_setopt(curl, CURLOPT_INFILESIZE_LARGE, (curl_off_t)ctx.size);
  curl_easy_setopt(curl, CURLOPT_AWS_SIGV4, sigv4.c_str());
  curl_easy_setopt(curl, CURLOPT_USERPWD, userpwd.c_str());
  //local minio dev setup only, real s3 always verifies
  curl_easy_setopt(curl, CURLOPT_SSL_VERIFYPEER, 0L);

  char errbuf[CURL_ERROR_SIZE]= {0};
  curl_easy_setopt(curl, CURLOPT_ERRORBUFFER, errbuf);

  CURLcode res= curl_easy_perform(curl);
  long http_code= 0;
  curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &http_code);
  curl_easy_cleanup(curl);

  if (res != CURLE_OK)
  {
    *error= std::string("curl error: ") +
            (errbuf[0] ? errbuf : curl_easy_strerror(res));
    return false;
  }
  if (http_code < 200 || http_code >= 300)
  {
    *error= "s3 upload failed with http status " + std::to_string(http_code);
    return false;
  }
  return true;
}

bool DownloadFileFromS3(const std::string &key, const std::string &local_path,
                        const ObjectStoreConfig &config, std::string *error)
{
  CURL *curl= curl_easy_init();
  if (!curl)
  {
    *error= "curl_easy_init failed";
    return false;
  }

  std::string url= config.endpoint + "/" + config.bucket + "/" + key;
  std::string sigv4= "aws:amz:" + config.region + ":s3";
  std::string userpwd= config.access_key + ":" + config.secret_key;
  std::string body;

  curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
  curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, WriteCallback);
  curl_easy_setopt(curl, CURLOPT_WRITEDATA, &body);
  curl_easy_setopt(curl, CURLOPT_AWS_SIGV4, sigv4.c_str());
  curl_easy_setopt(curl, CURLOPT_USERPWD, userpwd.c_str());
  curl_easy_setopt(curl, CURLOPT_SSL_VERIFYPEER, 0L);

  char errbuf[CURL_ERROR_SIZE]= {0};
  curl_easy_setopt(curl, CURLOPT_ERRORBUFFER, errbuf);

  CURLcode res= curl_easy_perform(curl);
  long http_code= 0;
  curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &http_code);
  curl_easy_cleanup(curl);

  if (res != CURLE_OK)
  {
    *error= std::string("curl error: ") +
            (errbuf[0] ? errbuf : curl_easy_strerror(res));
    return false;
  }
  if (http_code < 200 || http_code >= 300)
  {
    *error= "s3 download failed with http status " + std::to_string(http_code);
    return false;
  }

  std::ofstream out(local_path, std::ios::binary | std::ios::trunc);
  if (!out)
  {
    *error= "could not open " + local_path + " for writing";
    return false;
  }
  out.write(body.data(), (std::streamsize)body.size());
  return out.good();
}

bool DownloadFileFromS3Uri(const std::string &uri, const std::string &local_path,
                           const ObjectStoreConfig &config, std::string *error)
{
  std::string prefix= "s3://" + config.bucket + "/";
  if (uri.rfind(prefix, 0) != 0)
  {
    *error= "uri '" + uri + "' did not start with expected prefix '" + prefix + "'";
    return false;
  }
  return DownloadFileFromS3(uri.substr(prefix.size()), local_path, config, error);
}

} // namespace parquet
