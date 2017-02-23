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

SET (FTS_DEFAULT_KVSTORE "mossStore_default_kvstore")

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

IF (WIN32)
  INCLUDE (PlatformIntrospection)
  _DETERMINE_ARCH (_arch)
  IF (NOT _arch STREQUAL x86)
    SET (_ldflags "-linkmode internal")
  ENDIF ()
ENDIF ()

GoInstall (TARGET cbft PACKAGE github.com/couchbase/cbft/cmd/cbft
  GOPATH "${PROJECT_SOURCE_DIR}/../../../.." "${GODEPSDIR}"
  ${_forestdb_dep}
  CGO_INCLUDE_DIRS "${FORESTDB_INCLUDE_DIR}"
  CGO_LIBRARY_DIRS "${FORESTDB_LIBRARY_DIR}"
  LDFLAGS "${_ldflags}"
  GOTAGS "forestdb ${FTS_DEFAULT_KVSTORE}"
  INSTALL_PATH bin OUTPUT cbft
  GOVERSION 1.8)

GoInstall (TARGET cbft-bleve PACKAGE github.com/couchbase/cbft/cmd/cbft-bleve
  GOPATH "${PROJECT_SOURCE_DIR}/../../../.." "${GODEPSDIR}"
  ${_forestdb_dep}
  CGO_INCLUDE_DIRS "${FORESTDB_INCLUDE_DIR}"
  CGO_LIBRARY_DIRS "${FORESTDB_LIBRARY_DIR}"
  LDFLAGS "${_ldflags}"
  GOTAGS "forestdb ${FTS_DEFAULT_KVSTORE}"
  INSTALL_PATH bin OUTPUT cbft-bleve
  GOVERSION 1.8)

# Generate pluggable-ui-fts.json file.
SET (DOC_ROOT_PREFIX "")
configure_file (pluggable-ui-fts.json.in pluggable-ui-fts.json)

# Copy pluggable-ui-fts.json to install/etc/couchbase directory.
install (FILES "${PROJECT_BINARY_DIR}/pluggable-ui-fts.json"
  DESTINATION etc/couchbase)

# Copy static resources to install/lib/fts directory.
install (DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/ns_server_static/fts"
  DESTINATION lib)

install (DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/staticx"
  DESTINATION lib/fts)

install (DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/../cbgt/rest/static"
  DESTINATION lib/fts)

# Generate pluggable-ui-fts.cluster_run.json file for cluster_run.
SET (GOPROJ_PREFIX "${CMAKE_CURRENT_SOURCE_DIR}/..")
SET (GODEPS_PREFIX "${CMAKE_CURRENT_SOURCE_DIR}/../../../../../godeps/src/github.com")
configure_file (pluggable-ui-fts.cluster_run.json.in
                ${PROJECT_BINARY_DIR}/../../../../../cluster_run_ui_plugins/pluggable-ui-fts.cluster_run.json)

GoInstall(TARGET mossScope
  PACKAGE github.com/couchbase/moss/cmd/mossScope
  GOPATH "${CMAKE_SOURCE_DIR}/goproj" "${GODEPSDIR}"
  INSTALL_PATH bin OUTPUT mossScope
  GOVERSION 1.8)
