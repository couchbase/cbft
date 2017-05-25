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

SET (FTS_DEFAULT_KVSTORE "mossStore_default_kvstore")

INCLUDE (FindCouchbaseGo)

GoInstall (TARGET cbft PACKAGE github.com/couchbase/cbft/cmd/cbft
  GOPATH "${PROJECT_SOURCE_DIR}/../../../.." "${GODEPSDIR}"
  GOTAGS "${FTS_DEFAULT_KVSTORE}"
  INSTALL_PATH bin OUTPUT cbft
  GOVERSION 1.8.3)

GoInstall (TARGET cbft-bleve PACKAGE github.com/couchbase/cbft/cmd/cbft-bleve
  GOPATH "${PROJECT_SOURCE_DIR}/../../../.." "${GODEPSDIR}"
  GOTAGS "${FTS_DEFAULT_KVSTORE}"
  INSTALL_PATH bin OUTPUT cbft-bleve
  GOVERSION 1.8.3)

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

# Generate pluggable-ui-fts.cluster_run.json file for cluster_run.
SET (GOPROJ_PREFIX "${CMAKE_CURRENT_SOURCE_DIR}/..")
SET (GODEPS_PREFIX "${CMAKE_CURRENT_SOURCE_DIR}/../../../../../godeps/src/github.com")
configure_file (pluggable-ui-fts.cluster_run.json.in
                ${PROJECT_BINARY_DIR}/../../../../../cluster_run_ui_plugins/pluggable-ui-fts.cluster_run.json)

GoInstall(TARGET mossScope
  PACKAGE github.com/couchbase/mossScope
  GOPATH "${CMAKE_SOURCE_DIR}/goproj" "${GODEPSDIR}"
  INSTALL_PATH bin OUTPUT mossScope
  GOVERSION 1.8.3)
