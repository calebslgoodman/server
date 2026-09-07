#parquet build config

IF(NOT TARGET libduckdb)
  MESSAGE(FATAL_ERROR
    "Parquet storage engine requires DuckDB target 'libduckdb'. "
    "Make sure the DuckDB submodule is initialized and configured "
    "(for example: git submodule update --init --recursive storage/duckdb/duckdb).")
ENDIF()

MYSQL_ADD_PLUGIN(parquet
  ha_parquet.cc
  STORAGE_ENGINE
)

IF(TARGET parquet)
  target_include_directories(parquet PRIVATE
    ${CMAKE_SOURCE_DIR}/storage/duckdb/duckdb
    ${CMAKE_SOURCE_DIR}/storage/duckdb/duckdb/third_parties/duckdb/src/include
  )
  target_link_libraries(parquet
    libduckdb
  )
ENDIF()
