#parquet build config

FIND_PACKAGE(CURL REQUIRED)

IF(NOT TARGET libduckdb)
  MESSAGE(FATAL_ERROR
    "Parquet storage engine requires DuckDB target 'libduckdb'. "
    "Make sure the DuckDB submodule is initialized and configured "
    "(for example: git submodule update --init --recursive storage/duckdb/duckdb).")
ENDIF()

MYSQL_ADD_PLUGIN(parquet
  ha_parquet.cc
  parquet_schema.cc
  parquet_object_store.cc
  parquet_catalog.cc
  parquet_iceberg.cc
  STORAGE_ENGINE
)

IF(TARGET parquet)
  target_include_directories(parquet PRIVATE
    ${CMAKE_SOURCE_DIR}/storage/duckdb/duckdb
    ${CMAKE_SOURCE_DIR}/storage/duckdb/duckdb/third_parties/duckdb/src/include
    ${CMAKE_SOURCE_DIR}/storage/columnstore/columnstore/utils/json
  )
  target_link_libraries(parquet
    libduckdb
    CURL::libcurl
  )
ENDIF()
