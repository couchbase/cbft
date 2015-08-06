CMAKE_MINIMUM_REQUIRED (VERSION 2.8)
PROJECT (cbft)

# This project is intended to be built as part of a Couchbase Server build,
# but for nominal support building stand-alone, the following variables
# need to be specified:
#
# CMAKE_MODULE_PATH - should contain a directory containing
#      FindCouchbaseGo.cmake, eg. tlm/cmake/Modules
#
# CMAKE_INSTALL_PREFIX - should point to a directory where the bin/ output
#      directory will be placed
#
# GODEPSDIR - should point to a Go workspace directory containing all
#      transitive Go dependencies
#
# FORESTDB_INCLUDE_DIR - should point to where libforestdb/forestdb.h exists
#
# FORESTDB_LIBRARY_DIR - should point to where libforestdb.so resides
#
# Optionally:
#
# FORESTDB_TARGET - if set, assumed to the the CMake target name which
# builds libforestdb.so; will set dependencies to ensure it is build
# before attempting to compile indexer

INCLUDE (FindCouchbaseGo)

SET (_forestdb_dep)
IF (DEFINED FORESTDB_TARGET)
  SET (_forestdb_dep DEPENDS ${FORESTDB_TARGET})
ENDIF (DEFINED FORESTDB_TARGET)

GoInstall (TARGET cbft PACKAGE github.com/couchbaselabs/cbft/cmd/cbft
  GOPATH "${PROJECT_SOURCE_DIR}/../../../.." "${GODEPSDIR}"
  ${_forestdb_dep}
  CGO_INCLUDE_DIRS "${FORESTDB_INCLUDE_DIR}"
  CGO_LIBRARY_DIRS "${FORESTDB_LIBRARY_DIR}"
  GOTAGS "forestdb forestdb_default_kvstore"
  INSTALL_PATH bin OUTPUT cbft)
