#include "parquet_iceberg.h"

#include "json.hpp"

#include <chrono>
#include <fstream>
#include <limits>
#include <random>
#include <sstream>

namespace parquet
{

namespace
{

using json= nlohmann::json;

struct ManifestDataFile
{
  int status= 0; //0 = existing, 1 = added
  uint64_t snapshot_id= 0;
  uint64_t sequence_number= 0;
  std::string file_path;
  uint64_t record_count= 0;
  uint64_t file_size_bytes= 0;
};

//iceberg manifest-list entries always describe exactly one manifest
//file here, since we rewrite the whole manifest on every commit.
struct ManifestListEntry
{
  std::string manifest_path;
  uint64_t manifest_length= 0;
  uint64_t added_snapshot_id= 0;
  int added_files_count= 0;
  int existing_files_count= 0;
  uint64_t added_rows_count= 0;
  uint64_t existing_rows_count= 0;
  uint64_t sequence_number= 0;
};

uint64_t NextUniqueId()
{
  thread_local std::mt19937_64 generator(std::random_device{}());
  //keep it well clear of 0/negative-looking values once cast to int64_t
  thread_local std::uniform_int_distribution<uint64_t> distribution(
      1ULL << 40, std::numeric_limits<uint64_t>::max() >> 1);
  return distribution(generator);
}

std::string NextUniqueToken()
{
  std::ostringstream stream;
  stream << std::hex << NextUniqueId();
  return stream.str();
}

//---- avro binary encoding (zigzag varint ints/longs, length-prefixed
//---- strings/bytes, union branch index + value) -- just enough to write
//---- the two fixed record schemas below, uncompressed.

void WriteLong(std::string *out, int64_t value)
{
  uint64_t encoded=
      (static_cast<uint64_t>(value) << 1) ^ static_cast<uint64_t>(value >> 63);
  while ((encoded & ~0x7FULL) != 0)
  {
    out->push_back(static_cast<char>((encoded & 0x7F) | 0x80));
    encoded>>= 7;
  }
  out->push_back(static_cast<char>(encoded));
}

void WriteInt(std::string *out, int32_t value) { WriteLong(out, value); }

void WriteString(std::string *out, const std::string &value)
{
  WriteLong(out, (int64_t)value.size());
  out->append(value);
}

void WriteLongUnion(std::string *out, uint64_t value)
{
  WriteLong(out, 1);
  WriteLong(out, (int64_t)value);
}

//---- avro binary decoding, the mirror of the writers above. ported
//---- from (and cross-checked against) the independent python decoder
//---- written to verify day 8's writer -- see that day's commit message.

int64_t ReadLong(const std::string &data, size_t *pos)
{
  uint64_t result= 0;
  int shift= 0;
  while (true)
  {
    uint8_t b= (uint8_t)data[(*pos)++];
    result|= (uint64_t)(b & 0x7F) << shift;
    if (!(b & 0x80))
      break;
    shift+= 7;
  }
  return (int64_t)(result >> 1) ^ -(int64_t)(result & 1);
}

int32_t ReadInt(const std::string &data, size_t *pos) { return (int32_t)ReadLong(data, pos); }

std::string ReadString(const std::string &data, size_t *pos)
{
  int64_t n= ReadLong(data, pos);
  std::string s= data.substr(*pos, (size_t)n);
  *pos+= (size_t)n;
  return s;
}

//[null, long] union: branch 0 means the value is absent.
bool ReadLongUnion(const std::string &data, size_t *pos, uint64_t *value)
{
  int64_t branch= ReadLong(data, pos);
  if (branch == 0)
    return false;
  *value= (uint64_t)ReadLong(data, pos);
  return true;
}

struct DecodedObjectContainer
{
  std::string records; //every block's record bytes, concatenated
  int64_t total_objects= 0;
};

//validates the OCF header (magic, codec must be "null" -- the only one
//we ever write) and concatenates every block's records. doesn't need
//the embedded avro.schema back out since the caller already knows
//exactly which fixed schema it's decoding.
bool ReadAvroObjectContainerFile(const std::string &local_path,
                                 DecodedObjectContainer *container, std::string *error)
{
  std::ifstream stream(local_path, std::ios::binary);
  if (!stream)
  {
    *error= "could not open " + local_path + " for reading";
    return false;
  }
  std::ostringstream buf;
  buf << stream.rdbuf();
  std::string data= buf.str();

  if (data.size() < 4 || data.compare(0, 4, "Obj\x01", 4) != 0)
  {
    *error= local_path + " is not a valid avro object container file";
    return false;
  }
  size_t pos= 4;

  std::string codec= "null";
  int64_t n= ReadLong(data, &pos);
  while (n != 0)
  {
    for (int64_t i= 0; i < n; i++)
    {
      std::string key= ReadString(data, &pos);
      std::string value= ReadString(data, &pos);
      if (key == "avro.codec")
        codec= value;
    }
    n= ReadLong(data, &pos);
  }
  if (codec != "null")
  {
    *error= local_path + " uses avro codec '" + codec + "', only 'null' is supported";
    return false;
  }

  std::string sync_marker= data.substr(pos, 16);
  pos+= 16;

  container->records.clear();
  container->total_objects= 0;
  while (pos + 16 < data.size())
  {
    int64_t object_count= ReadLong(data, &pos);
    int64_t byte_length= ReadLong(data, &pos);
    container->records.append(data, pos, (size_t)byte_length);
    pos+= (size_t)byte_length;
    container->total_objects+= object_count;

    if (data.compare(pos, 16, sync_marker) != 0)
    {
      *error= local_path + " has a sync marker mismatch mid-file";
      return false;
    }
    pos+= 16;
  }
  return true;
}

//writes a single-block, uncompressed avro object container file -- the
//standard avro OCF layout ("Obj\x01" magic, header with embedded
//schema, sync marker, one block of records, sync marker again) with
//avro.codec forced to "null" so there's no compression codec to
//implement.
bool WriteAvroObjectContainerFile(const std::string &path,
                                  const std::string &schema_json,
                                  const std::map<std::string, std::string> &metadata,
                                  const std::string &records, size_t object_count,
                                  std::string *error)
{
  std::string payload;
  payload.append("Obj", 3);
  payload.push_back('\x01');

  std::map<std::string, std::string> header_metadata= metadata;
  header_metadata["avro.codec"]= "null";
  header_metadata["avro.schema"]= schema_json;

  WriteLong(&payload, (int64_t)header_metadata.size());
  for (const auto &entry : header_metadata)
  {
    WriteString(&payload, entry.first);
    WriteString(&payload, entry.second);
  }
  WriteLong(&payload, 0); //end of header map

  std::string sync_marker;
  for (int i= 0; i < 16; i++)
    sync_marker.push_back((char)(NextUniqueId() & 0xFF));
  payload.append(sync_marker);

  WriteLong(&payload, (int64_t)object_count);
  WriteLong(&payload, (int64_t)records.size());
  payload.append(records);
  payload.append(sync_marker);

  std::ofstream stream(path, std::ios::binary | std::ios::trunc);
  if (!stream)
  {
    *error= "could not open " + path + " for writing";
    return false;
  }
  stream.write(payload.data(), (std::streamsize)payload.size());
  if (!stream.good())
  {
    *error= "failed writing " + path;
    return false;
  }
  return true;
}

//the two iceberg manifest avro schemas, non-partitioned tables only
//(matches ayush's fork -- partitioned tables aren't supported here
//either). field-ids match the real iceberg spec, not just an arbitrary
//choice, since real iceberg readers key off them.
std::string BuildManifestSchemaJson()
{
  return json({
    {"type", "record"}, {"name", "manifest_entry"},
    {"fields", json::array({
      {{"name", "status"}, {"type", "int"}, {"field-id", 0}},
      {{"name", "snapshot_id"}, {"type", json::array({"null", "long"})},
       {"default", nullptr}, {"field-id", 1}},
      {{"name", "sequence_number"}, {"type", json::array({"null", "long"})},
       {"default", nullptr}, {"field-id", 3}},
      {{"name", "file_sequence_number"}, {"type", json::array({"null", "long"})},
       {"default", nullptr}, {"field-id", 4}},
      {{"name", "data_file"},
       {"type", {{"type", "record"}, {"name", "data_file"},
                 {"fields", json::array({
                   {{"name", "content"}, {"type", "int"}, {"field-id", 134}},
                   {{"name", "file_path"}, {"type", "string"}, {"field-id", 100}},
                   {{"name", "file_format"}, {"type", "string"}, {"field-id", 101}},
                   {{"name", "partition"},
                    {"type", {{"type", "record"}, {"name", "partition_data"},
                              {"fields", json::array()}}},
                    {"field-id", 102}},
                   {{"name", "record_count"}, {"type", "long"}, {"field-id", 103}},
                   {{"name", "file_size_in_bytes"}, {"type", "long"}, {"field-id", 104}},
                 })}}},
       {"field-id", 2}},
    })},
  }).dump();
}

std::string BuildManifestListSchemaJson()
{
  return json({
    {"type", "record"}, {"name", "manifest_file"},
    {"fields", json::array({
      {{"name", "manifest_path"}, {"type", "string"}, {"field-id", 500}},
      {{"name", "manifest_length"}, {"type", "long"}, {"field-id", 501}},
      {{"name", "partition_spec_id"}, {"type", "int"}, {"field-id", 502}},
      {{"name", "added_snapshot_id"}, {"type", "long"}, {"field-id", 503}},
      {{"name", "added_files_count"}, {"type", "int"}, {"field-id", 504}},
      {{"name", "existing_files_count"}, {"type", "int"}, {"field-id", 505}},
      {{"name", "deleted_files_count"}, {"type", "int"}, {"field-id", 506}},
      {{"name", "added_rows_count"}, {"type", "long"}, {"field-id", 512}},
      {{"name", "existing_rows_count"}, {"type", "long"}, {"field-id", 513}},
      {{"name", "deleted_rows_count"}, {"type", "long"}, {"field-id", 514}},
      {{"name", "sequence_number"}, {"type", "long"}, {"field-id", 515}},
      {{"name", "min_sequence_number"}, {"type", "long"}, {"field-id", 516}},
      {{"name", "content"}, {"type", "int"}, {"field-id", 517}},
    })},
  }).dump();
}

void EncodeManifestEntry(std::string *out, const ManifestDataFile &entry)
{
  WriteInt(out, entry.status);
  WriteLongUnion(out, entry.snapshot_id);
  WriteLongUnion(out, entry.sequence_number);
  WriteLongUnion(out, entry.sequence_number); //file_sequence_number == sequence_number here
  WriteInt(out, 0);                          //data_file.content: 0 = data
  WriteString(out, entry.file_path);
  WriteString(out, "PARQUET");
  //partition_data has zero fields -- avro records aren't length-prefixed,
  //so an empty record is genuinely zero bytes, nothing to write here.
  WriteLong(out, (int64_t)entry.record_count);
  WriteLong(out, (int64_t)entry.file_size_bytes);
}

void EncodeManifestListEntry(std::string *out, const ManifestListEntry &entry)
{
  WriteString(out, entry.manifest_path);
  WriteLong(out, (int64_t)entry.manifest_length);
  WriteInt(out, 0); //partition_spec_id (unpartitioned)
  WriteLong(out, (int64_t)entry.added_snapshot_id);
  WriteInt(out, entry.added_files_count);
  WriteInt(out, entry.existing_files_count);
  WriteInt(out, 0); //deleted_files_count
  WriteLong(out, (int64_t)entry.added_rows_count);
  WriteLong(out, (int64_t)entry.existing_rows_count);
  WriteLong(out, 0); //deleted_rows_count
  WriteLong(out, (int64_t)entry.sequence_number);
  WriteLong(out, (int64_t)entry.sequence_number); //min_sequence_number
  WriteInt(out, 0); //content: 0 = data manifest
}

//pulls the schema object matching current-schema-id out of the raw
//table metadata json, same as ayush's ParseTableState.
bool CurrentSchemaJson(const std::string &raw_metadata_json, int current_schema_id,
                       std::string *schema_json, std::string *error)
{
  json payload= json::parse(raw_metadata_json, nullptr, false);
  if (payload.is_discarded())
  {
    *error= "table metadata was not valid json";
    return false;
  }
  if (payload.contains("schemas") && payload["schemas"].is_array())
  {
    for (const auto &schema : payload["schemas"])
    {
      if (schema.value("schema-id", -1) == current_schema_id)
      {
        *schema_json= schema.dump();
        return true;
      }
    }
  }
  *error= "table metadata had no schema with id " + std::to_string(current_schema_id);
  return false;
}

//mariadb table paths start with "./" (e.g. "./testdb/t1"), which we've
//been reusing directly as the local file path AND the s3 object key
//since day 4/5. that's harmless for a local path, but baked verbatim
//into a manifest's file_path or a snapshot's manifest-list field it
//produces a technically-wrong s3:// uri (a real iceberg reader
//resolving strictly wouldn't find "./testdb/..." under the bucket).
std::string ToS3Uri(const std::string &bucket, const std::string &relative_path)
{
  std::string normalized=
      relative_path.rfind("./", 0) == 0 ? relative_path.substr(2) : relative_path;
  return "s3://" + bucket + "/" + normalized;
}

} // namespace

bool BuildIcebergCommitArtifacts(const CatalogTableIdent &ident,
                                 const CatalogLoadTableResult &load_result,
                                 const std::vector<CatalogDataFile> &existing_files,
                                 const CatalogDataFile &new_file,
                                 const std::string &s3_bucket,
                                 const std::string &local_dir,
                                 IcebergCommitArtifacts *artifacts,
                                 std::string *error)
{
  if (load_result.table_uuid.empty())
  {
    *error= "catalog load result has no table-uuid";
    return false;
  }

  std::string current_schema_json;
  if (!CurrentSchemaJson(load_result.raw_metadata_json, load_result.current_schema_id,
                        &current_schema_json, error))
    return false;

  const uint64_t snapshot_id= NextUniqueId();
  const uint64_t sequence_number= load_result.last_sequence_number + 1;
  const std::string token= NextUniqueToken();

  //manifest: every previously-active file, unchanged, plus the new one.
  std::vector<ManifestDataFile> entries;
  for (const auto &f : existing_files)
  {
    ManifestDataFile entry;
    entry.status= 0;
    entry.snapshot_id= f.added_in_snapshot_id;
    entry.sequence_number= f.added_in_sequence_number;
    entry.file_path= ToS3Uri(s3_bucket, f.path);
    entry.record_count= f.record_count;
    entry.file_size_bytes= f.file_size_bytes;
    entries.push_back(entry);
  }
  ManifestDataFile new_entry;
  new_entry.status= 1;
  new_entry.snapshot_id= snapshot_id;
  new_entry.sequence_number= sequence_number;
  new_entry.file_path= ToS3Uri(s3_bucket, new_file.path);
  new_entry.record_count= new_file.record_count;
  new_entry.file_size_bytes= new_file.file_size_bytes;
  entries.push_back(new_entry);

  std::string manifest_records;
  for (const auto &entry : entries)
    EncodeManifestEntry(&manifest_records, entry);

  const std::string manifest_name= "manifest-" + std::to_string(snapshot_id) + "-" +
                                   token + ".avro";
  const std::string manifest_list_name= "snap-" + std::to_string(snapshot_id) + "-1-" +
                                        token + ".avro";
  artifacts->manifest_local_path= local_dir + "/" + manifest_name;
  artifacts->manifest_key= local_dir + "/" + manifest_name;
  artifacts->manifest_list_local_path= local_dir + "/" + manifest_list_name;
  artifacts->manifest_list_key= local_dir + "/" + manifest_list_name;

  std::map<std::string, std::string> manifest_meta= {
      {"schema", current_schema_json},
      {"schema-id", std::to_string(load_result.current_schema_id)},
      {"partition-spec", "[]"},
      {"partition-spec-id", "0"},
      {"format-version", "2"},
      {"content", "data"},
  };
  if (!WriteAvroObjectContainerFile(artifacts->manifest_local_path,
                                    BuildManifestSchemaJson(), manifest_meta,
                                    manifest_records, entries.size(), error))
    return false;

  uint64_t manifest_length= 0;
  {
    std::ifstream stream(artifacts->manifest_local_path, std::ios::binary | std::ios::ate);
    manifest_length= stream ? (uint64_t)stream.tellg() : 0;
  }

  ManifestListEntry list_entry;
  list_entry.manifest_path= ToS3Uri(s3_bucket, artifacts->manifest_key);
  list_entry.manifest_length= manifest_length;
  list_entry.added_snapshot_id= snapshot_id;
  list_entry.added_files_count= 1;
  list_entry.existing_files_count= (int)existing_files.size();
  list_entry.added_rows_count= new_file.record_count;
  for (const auto &f : existing_files)
    list_entry.existing_rows_count+= f.record_count;
  list_entry.sequence_number= sequence_number;

  std::string manifest_list_records;
  EncodeManifestListEntry(&manifest_list_records, list_entry);
  if (!WriteAvroObjectContainerFile(artifacts->manifest_list_local_path,
                                    BuildManifestListSchemaJson(), {},
                                    manifest_list_records, 1, error))
    return false;

  const uint64_t timestamp_ms=
      (uint64_t)std::chrono::duration_cast<std::chrono::milliseconds>(
          std::chrono::system_clock::now().time_since_epoch())
          .count();
  uint64_t total_records= new_file.record_count;
  for (const auto &f : existing_files)
    total_records+= f.record_count;

  json snapshot= {
      {"snapshot-id", snapshot_id},
      {"sequence-number", sequence_number},
      {"timestamp-ms", timestamp_ms},
      {"manifest-list", ToS3Uri(s3_bucket, artifacts->manifest_list_key)},
      {"summary", {{"operation", "append"},
                   {"added-data-files", "1"},
                   {"added-records", std::to_string(new_file.record_count)},
                   {"total-data-files", std::to_string(entries.size())},
                   {"total-records", std::to_string(total_records)}}},
      {"schema-id", load_result.current_schema_id},
  };

  json requirements= json::array();
  requirements.push_back({{"type", "assert-table-uuid"}, {"uuid", load_result.table_uuid}});

  json updates= json::array();
  if (!load_result.current_snapshot_id.empty())
  {
    int64_t parent_snapshot_id= std::stoll(load_result.current_snapshot_id);
    snapshot["parent-snapshot-id"]= parent_snapshot_id;
    requirements.push_back({{"type", "assert-ref-snapshot-id"},
                            {"ref", "main"},
                            {"snapshot-id", parent_snapshot_id}});
  }
  else
  {
    requirements.push_back({{"type", "assert-ref-snapshot-id"},
                            {"ref", "main"},
                            {"snapshot-id", nullptr}});
  }
  updates.push_back({{"action", "add-snapshot"}, {"snapshot", snapshot}});
  updates.push_back({{"action", "set-snapshot-ref"},
                     {"ref-name", "main"},
                     {"snapshot-id", snapshot_id},
                     {"type", "branch"}});

  artifacts->snapshot_id= snapshot_id;
  artifacts->sequence_number= sequence_number;
  artifacts->commit_request_json=
      json({{"requirements", requirements}, {"updates", updates}}).dump();
  return true;
}

bool DecodeManifestListFile(const std::string &local_path,
                            std::vector<std::string> *manifest_paths, std::string *error)
{
  DecodedObjectContainer container;
  if (!ReadAvroObjectContainerFile(local_path, &container, error))
    return false;

  size_t pos= 0;
  manifest_paths->clear();
  for (int64_t i= 0; i < container.total_objects; i++)
  {
    std::string manifest_path= ReadString(container.records, &pos);
    ReadLong(container.records, &pos); //manifest_length
    ReadInt(container.records, &pos);  //partition_spec_id
    ReadLong(container.records, &pos); //added_snapshot_id
    ReadInt(container.records, &pos);  //added_files_count
    ReadInt(container.records, &pos);  //existing_files_count
    ReadInt(container.records, &pos);  //deleted_files_count
    ReadLong(container.records, &pos); //added_rows_count
    ReadLong(container.records, &pos); //existing_rows_count
    ReadLong(container.records, &pos); //deleted_rows_count
    ReadLong(container.records, &pos); //sequence_number
    ReadLong(container.records, &pos); //min_sequence_number
    ReadInt(container.records, &pos);  //content
    manifest_paths->push_back(manifest_path);
  }
  return true;
}

bool DecodeManifestFile(const std::string &local_path,
                        std::vector<CatalogDataFile> *data_files, std::string *error)
{
  DecodedObjectContainer container;
  if (!ReadAvroObjectContainerFile(local_path, &container, error))
    return false;

  size_t pos= 0;
  data_files->clear();
  for (int64_t i= 0; i < container.total_objects; i++)
  {
    int32_t status= ReadInt(container.records, &pos);
    uint64_t snapshot_id= 0, sequence_number= 0, file_sequence_number= 0;
    ReadLongUnion(container.records, &pos, &snapshot_id);
    ReadLongUnion(container.records, &pos, &sequence_number);
    ReadLongUnion(container.records, &pos, &file_sequence_number);
    ReadInt(container.records, &pos);           //data_file.content
    std::string file_path= ReadString(container.records, &pos);
    ReadString(container.records, &pos);         //file_format
    //partition_data has zero fields -- nothing to read (see day 8 fix)
    int64_t record_count= ReadLong(container.records, &pos);
    int64_t file_size_bytes= ReadLong(container.records, &pos);

    if (status == 2) //deleted -- not currently live
      continue;

    CatalogDataFile file;
    file.path= file_path;
    file.record_count= (uint64_t)record_count;
    file.file_size_bytes= (uint64_t)file_size_bytes;
    file.added_in_snapshot_id= snapshot_id;
    file.added_in_sequence_number= sequence_number;
    data_files->push_back(file);
  }
  return true;
}

namespace
{

std::string BaseName(const std::string &path)
{
  size_t slash= path.find_last_of('/');
  return slash == std::string::npos ? path : path.substr(slash + 1);
}

} // namespace

bool ResolveActiveDataFilesFromIceberg(const CatalogLoadTableResult &load_result,
                                       const ObjectStoreConfig &s3_config,
                                       const std::string &local_dir,
                                       std::vector<CatalogDataFile> *data_files,
                                       std::string *error)
{
  data_files->clear();
  if (load_result.current_snapshot_manifest_list.empty())
    return true; //no commits yet -- an empty table, not an error

  std::string manifest_list_local=
      local_dir + "/downloaded_" + BaseName(load_result.current_snapshot_manifest_list);
  if (!DownloadFileFromS3Uri(load_result.current_snapshot_manifest_list,
                             manifest_list_local, s3_config, error))
    return false;

  std::vector<std::string> manifest_uris;
  if (!DecodeManifestListFile(manifest_list_local, &manifest_uris, error))
    return false;

  for (const auto &manifest_uri : manifest_uris)
  {
    std::string manifest_local= local_dir + "/downloaded_" + BaseName(manifest_uri);
    if (!DownloadFileFromS3Uri(manifest_uri, manifest_local, s3_config, error))
      return false;

    std::vector<CatalogDataFile> files_in_manifest;
    if (!DecodeManifestFile(manifest_local, &files_in_manifest, error))
      return false;
    data_files->insert(data_files->end(), files_in_manifest.begin(),
                       files_in_manifest.end());
  }
  return true;
}

} // namespace parquet
