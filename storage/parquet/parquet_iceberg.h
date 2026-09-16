#ifndef PARQUET_ICEBERG_INCLUDED
#define PARQUET_ICEBERG_INCLUDED

#include "parquet_catalog.h"
#include "parquet_object_store.h"

#include <cstdint>
#include <string>
#include <vector>

namespace parquet
{

//everything needed to commit one new data file into an iceberg table as
//a new snapshot. one manifest (avro) lists every active file -- the
//previously committed ones plus the new one -- and one manifest-list
//(avro) points at just that manifest. this rewrites the manifest fully
//on every commit rather than accumulating a growing manifest-list,
//matching ayush's fork -- simpler at the cost of write amplification
//that would matter at real scale, not at ours.
struct IcebergCommitArtifacts
{
  uint64_t snapshot_id= 0;
  uint64_t sequence_number= 0;
  std::string manifest_local_path;
  std::string manifest_key;
  std::string manifest_list_local_path;
  std::string manifest_list_key;
  //full iceberg rest commit body: {"requirements": [...], "updates": [...]}
  std::string commit_request_json;
};

//writes the manifest + manifest-list files to local_dir (not yet
//uploaded -- that's the caller's job, same as flushed parquet files)
//and builds the commit request body. existing_files is every file the
//table's current snapshot already points at; new_file is the one being
//added this round.
bool BuildIcebergCommitArtifacts(const CatalogTableIdent &ident,
                                 const CatalogLoadTableResult &load_result,
                                 const std::vector<CatalogDataFile> &existing_files,
                                 const CatalogDataFile &new_file,
                                 const std::string &s3_bucket,
                                 const std::string &local_dir,
                                 IcebergCommitArtifacts *artifacts,
                                 std::string *error);

//reads a local manifest-list avro file (already downloaded from s3) and
//returns the s3:// path of every manifest file it points at. lakekeeper
//reports "scan-planning-mode":"client" -- it does not resolve manifests
//for us, so this is real, unavoidable work, not a shortcut we're
//choosing not to take.
bool DecodeManifestListFile(const std::string &local_path,
                            std::vector<std::string> *manifest_paths,
                            std::string *error);

//reads a local manifest avro file (already downloaded from s3) and
//returns every currently-live data file it lists (status 0 "existing"
//or 1 "added" -- excludes status 2 "deleted", though we don't produce
//those yet).
bool DecodeManifestFile(const std::string &local_path,
                        std::vector<CatalogDataFile> *data_files, std::string *error);

//the full read-side pipeline: downloads the current snapshot's
//manifest-list from s3, decodes it, downloads and decodes every
//manifest it points at, and returns the union of every currently-live
//data file -- purely from iceberg's own metadata, independent of our
//in-process parquet_files map. an empty load_result.current_snapshot_id
//(no commits yet) returns an empty list, not an error. downloaded avro
//files land in local_dir alongside everything else this table already
//keeps there.
bool ResolveActiveDataFilesFromIceberg(const CatalogLoadTableResult &load_result,
                                       const ObjectStoreConfig &s3_config,
                                       const std::string &local_dir,
                                       std::vector<CatalogDataFile> *data_files,
                                       std::string *error);

} // namespace parquet

#endif
