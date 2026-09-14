#ifndef PARQUET_ICEBERG_INCLUDED
#define PARQUET_ICEBERG_INCLUDED

#include "parquet_catalog.h"

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

} // namespace parquet

#endif
