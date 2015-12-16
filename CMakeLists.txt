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

IF (APPLE)
  # On OS X 10.11 (El Capitan) upwards we can no longer use DYLD_LIBRARY_PATH to locate
  # runtime dependancies. Instead add the location of forestdb etc to the actual
  # binary itself.
  SET (_ldflags "-extldflags '-Wl,-rpath,@executable_path/../lib'")
ENDIF ()

GoInstall (TARGET cbft PACKAGE github.com/couchbase/cbft/cmd/cbft
  GOPATH "${PROJECT_SOURCE_DIR}/../../../.." "${GODEPSDIR}"
  ${_forestdb_dep}
  CGO_INCLUDE_DIRS "${FORESTDB_INCLUDE_DIR}"
  CGO_LIBRARY_DIRS "${FORESTDB_LIBRARY_DIR}"
  LDFLAGS "${_ldflags}"
  GOTAGS "forestdb forestdb_default_kvstore"
  INSTALL_PATH bin OUTPUT cbft)

GoInstall (TARGET cbft-bleve-dump PACKAGE github.com/blevesearch/bleve/utils/bleve_dump
  GOPATH "${PROJECT_SOURCE_DIR}/../../../.." "${GODEPSDIR}"
  ${_forestdb_dep}
  CGO_INCLUDE_DIRS "${FORESTDB_INCLUDE_DIR}"
  CGO_LIBRARY_DIRS "${FORESTDB_LIBRARY_DIR}"
  LDFLAGS "${_ldflags}"
  GOTAGS "forestdb forestdb_default_kvstore"
  INSTALL_PATH bin OUTPUT cbft-bleve-dump)

GoInstall (TARGET cbft-bleve-query PACKAGE github.com/blevesearch/bleve/utils/bleve_query
  GOPATH "${PROJECT_SOURCE_DIR}/../../../.." "${GODEPSDIR}"
  ${_forestdb_dep}
  CGO_INCLUDE_DIRS "${FORESTDB_INCLUDE_DIR}"
  CGO_LIBRARY_DIRS "${FORESTDB_LIBRARY_DIR}"
  LDFLAGS "${_ldflags}"
  GOTAGS "forestdb forestdb_default_kvstore"
  INSTALL_PATH bin OUTPUT cbft-bleve-query)

# Generate pluggable-ui-fts.json file.
SET (BIN_PREFIX "${CMAKE_INSTALL_PREFIX}/lib")
configure_file (pluggable-ui-fts.json.in pluggable-ui-fts.json)

# Copy pluggable-ui-fts.json to install/etc/couchbase directory.
#
# TODO: Turned off for now, as there's a startup problem, where
# ns-server is trying to access cbft, even though cbft isn't spawned by ns-server yet.
#
# install (FILES "${PROJECT_BINARY_DIR}/pluggable-ui-fts.json"
#   DESTINATION etc/couchbase)

# Copy everything else to install/lib/fts directory.
install (DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/ns_server_static/fts"
  DESTINATION lib)

# Generate pluggable-ui-fts.json file for cluster_run.
SET (BIN_PREFIX "${CMAKE_CURRENT_SOURCE_DIR}/ns_server_static")
configure_file (pluggable-ui-fts.json.in pluggable-ui-fts.cluster_run.json)

# To run using the ./cluster_run tool:
#   cd [your dev directory where you ran 'repo sync' for couchbase server]
#   cd ns_server
#   ./cluster_run --pluggable-config="../build/goproj/src/github.com/couchbase/cbft/pluggable-ui-fts.cluster_run.json"
