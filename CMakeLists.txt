CMAKE_MINIMUM_REQUIRED (VERSION 3.13)

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

SET (FTS_DEFAULT "server")

INCLUDE (FindCouchbaseGo)

IF (BUILD_ENTERPRISE)
    SET (BUILD_TAG "enterprise")
ENDIF(BUILD_ENTERPRISE)

SET (GOVERSION SUPPORTED_NEWER)

GoModBuild (TARGET cbft PACKAGE github.com/couchbase/cbft/cmd/cbft
  GOTAGS "${FTS_DEFAULT} ${BUILD_TAG}"
  CGO_INCLUDE_DIRS "${sigar_SOURCE_DIR}/include"
  CGO_LIBRARY_DIRS "${sigar_BINARY_DIR}/src"
  INSTALL_PATH bin OUTPUT cbft
  DEPENDS sigar
  GOVERSION ${GOVERSION})

GoModBuild (TARGET cbft-bleve PACKAGE github.com/couchbase/cbft/cmd/cbft-bleve
  GOTAGS "${FTS_DEFAULT} ${BUILD_TAG}"
  CGO_INCLUDE_DIRS "${sigar_SOURCE_DIR}/include"
  CGO_LIBRARY_DIRS "${sigar_BINARY_DIR}/src"
  INSTALL_PATH bin
  DEPENDS sigar
  GOVERSION ${GOVERSION})

# Build the UI and copy the files to the correct place
ADD_SUBDIRECTORY(ns_server_static)

install (DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/staticx"
  DESTINATION lib/fts)

install (DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/../cbgt/rest/static/css"
  DESTINATION lib/fts/static)

install (DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/../cbgt/rest/static/fonts"
  DESTINATION lib/fts/static)

install (DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/../cbgt/rest/static/img"
  DESTINATION lib/fts/static)

install (DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/../cbgt/rest/static/js"
  DESTINATION lib/fts/static)

install (DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/../cbgt/rest/static/modal"
  DESTINATION lib/fts/static)

install (DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/../cbgt/rest/static/partials"
  DESTINATION lib/fts/static)

install (DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/../cbgt/rest/static/tabs"
  DESTINATION lib/fts/static)

install (DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/../cbgt/rest/static/lib/angular-route"
  DESTINATION lib/fts/static/lib)

install (DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/../cbgt/rest/static/lib/angular-ui-tree"
  DESTINATION lib/fts/static/lib)

INSTALL (FILES etc/metrics_metadata.json DESTINATION "etc/couchbase/fts")

# Generate pluggable-ui-fts.cluster_run.json file for cluster_run.
SET (GODEPS_PREFIX "${CMAKE_CURRENT_SOURCE_DIR}/../../../../../godeps/src/github.com")
