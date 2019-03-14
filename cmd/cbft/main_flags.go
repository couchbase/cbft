//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the
//  License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing,
//  software distributed under the License is distributed on an "AS
//  IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
//  express or implied. See the License for the specific language
//  governing permissions and limitations under the License.

package main

import (
	"flag"
	"fmt"
	"os"
	"path"
	"sort"
	"strings"
)

const defaultDataDir = "data"

type cbftFlags struct {
	BindHTTP    string
	BindHTTPS   string
	BindGRPC    string
	BindGRPCSSL string
	CfgConnect  string
	Container   string
	DataDir     string
	Help        bool
	Options     string
	Register    string
	Server      string
	StaticDir   string
	StaticETag  string
	Tags        string
	UUID        string
	Version     bool
	Weight      int
	Extras      string
	AuthType    string

	TLSCertFile string
	TLSKeyFile  string
}

var flags cbftFlags
var flagAliases map[string][]string

func init() {
	flagAliases = initFlags(&flags)
}

func initFlags(flags *cbftFlags) map[string][]string {
	flagAliasesInit := map[string][]string{} // main flag name => all aliases.
	flagKinds := map[string]string{}

	s := func(v *string, names []string, kind string,
		defaultVal, usage string) { // String cmd-line param.
		for _, name := range names {
			flag.StringVar(v, name, defaultVal, usage)
		}
		flagAliasesInit[names[0]] = names
		flagKinds[names[0]] = kind
	}

	i := func(v *int, names []string, kind string,
		defaultVal int, usage string) { // Integer cmd-line param.
		for _, name := range names {
			flag.IntVar(v, name, defaultVal, usage)
		}
		flagAliasesInit[names[0]] = names
		flagKinds[names[0]] = kind
	}

	b := func(v *bool, names []string, kind string,
		defaultVal bool, usage string) { // Bool cmd-line param.
		for _, name := range names {
			flag.BoolVar(v, name, defaultVal, usage)
		}
		flagAliasesInit[names[0]] = names
		flagKinds[names[0]] = kind
	}

	s(&flags.BindHTTP,
		[]string{"bindHttp", "b"}, "ADDR:PORT", "0.0.0.0:8094",
		"local address:port where this node will listen and"+
			"\nserve HTTP/REST API requests and the web-based"+
			"\nadmin UI; default is '0.0.0.0:8094';"+
			"\nmultiple ADDR:PORT's can be specified, separated by commas,"+
			"\nwhere the first ADDR:PORT is used for node cfg registration.")
	s(&flags.BindHTTPS,
		[]string{"bindHttps"}, "ADDR:PORT", "",
		"local address:port where this node will listen and"+
			"\nserve HTTPS/REST API requests and the web-based"+
			"\nadmin UI; by default, disabled; for example, ':18094'.")
	s(&flags.BindGRPC,
		[]string{"bindGrpc"}, "ADDR:PORT", "",
		"local address:port where this node will listen and"+
			"\nserve GRPC requests; for example, ':28094'.")
	s(&flags.BindGRPCSSL,
		[]string{"bindGrpcSsl"}, "ADDR:PORT", "",
		"local address:port where this node will listen and"+
			"\nserve GRPC requests; for example, ':28094'.")
	s(&flags.CfgConnect,
		[]string{"cfgConnect", "cfg", "c"}, "CFG_CONNECT", "simple",
		"connection string to a configuration provider/server"+
			"\nfor clustering multiple cbft nodes:"+
			"\n* couchbase:http://BUCKET_USER:BUCKET_PSWD@CB_HOST:CB_PORT"+
			"\n     - manages a cbft cluster configuration in a couchbase"+
			"\n       3.x bucket; for example:"+
			"\n       'couchbase:http://my-cfg-bucket@127.0.0.1:8091';"+
			"\n* simple"+
			"\n     - intended for development usage, the 'simple'"+
			"\n       configuration provider manages a configuration"+
			"\n       for a single, unclustered cbft node in a local"+
			"\n       file that's stored in the dataDir;"+
			"\n* metakv"+
			"\n     - manages a cbft cluster configuration in couchbase metakv store;"+
			"\n       environment variable CBAUTH_REVRPC_URL needs to be set"+
			"\n       for metakv; for example:"+
			"\n       'export CBAUTH_REVRPC_URL=http://user:password@localhost:9000/cbft';"+
			"\ndefault is 'simple'.")
	s(&flags.Container,
		[]string{"container"}, "PATH", "",
		"optional slash separated path of logical parent containers"+
			"\nfor this node, for shelf/rack/row/zone awareness.")
	s(&flags.DataDir,
		[]string{"dataDir", "data"}, "DIR", defaultDataDir,
		"optional directory path where local index data and"+
			"\nlocal config files will be stored for this node;"+
			"\ndefault is '"+defaultDataDir+"'.")
	b(&flags.Help,
		[]string{"help", "?", "H", "h"}, "", false,
		"print this usage message and exit.")
	s(&flags.Options,
		[]string{"options"}, "KEY=VALUE,...", "",
		"optional comma-separated key=value pairs for advanced configurations.")
	s(&flags.Register,
		[]string{"register"}, "STATE", "wanted",
		"optional flag to register this node in the cluster as:"+
			"\n* wanted      - make node wanted in the cluster,"+
			"\n                if not already, so that it will participate"+
			"\n                fully in data operations;"+
			"\n* wantedForce - same as wanted, but forces a cfg update;"+
			"\n* known       - make node known to the cluster,"+
			"\n                if not already, so it will be admin'able"+
			"\n                but won't yet participate in data operations;"+
			"\n                this is useful for staging several nodes into"+
			"\n                the cluster before making them fully wanted;"+
			"\n* knownForce  - same as known, but forces a cfg update;"+
			"\n* unwanted    - make node unwanted, but still known to the cluster;"+
			"\n* unknown     - make node unwanted and unknown to the cluster;"+
			"\n* unchanged   - don't change the node's registration state;"+
			"\ndefault is 'wanted'.")
	s(&flags.Server,
		[]string{"server", "s"}, "URL", "",
		"URL to datasource server; example when using couchbase 3.x as"+
			"\nyour datasource server: 'http://localhost:8091';"+
			"\nuse '.' when there is no datasource server.")
	s(&flags.StaticDir,
		[]string{"staticDir"}, "DIR", "static",
		"optional directory for web UI static content;"+
			"\ndefault is using the static resources embedded"+
			"\nin the program binary.")
	s(&flags.StaticETag,
		[]string{"staticETag"}, "ETAG", "",
		"optional ETag for web UI static content.")
	s(&flags.Tags,
		[]string{"tags"}, "TAGS", "",
		"optional comma-separated list of tags or enabled roles"+
			"\nfor this node, such as:"+
			"\n* feed    - node can connect feeds to datasources;"+
			"\n* janitor - node can run a local janitor;"+
			"\n* pindex  - node can maintain local index partitions;"+
			"\n* planner - node can replan cluster-wide resource allocations;"+
			"\n* queryer - node can execute queries;"+
			"\ndefault is (\"\") which means all roles are enabled.")
	s(&flags.UUID,
		[]string{"uuid"}, "UUID", "",
		"optional uuid for this node; by default, a previous uuid file"+
			"\nis read from the dataDir, or a new uuid is auto-generated"+
			"\nand saved into the dataDir.")
	b(&flags.Version,
		[]string{"version", "v"}, "", false,
		"print version string and exit.")
	i(&flags.Weight,
		[]string{"weight"}, "INTEGER", 1,
		"optional weight of this node, where a more capable"+
			"\nnode should have higher weight; default is 1.")
	s(&flags.Extras,
		[]string{"extras", "extra", "e"}, "EXTRAS", "",
		"extra information you want stored with this node.")
	s(&flags.AuthType,
		[]string{"authType", "auth"}, "AUTH_TYPE", "",
		"authentication type for cbft requests.")
	s(&flags.TLSCertFile,
		[]string{"tlsCertFile"}, "PATH", "",
		"TLS cert file; see also bindHttps.")
	s(&flags.TLSKeyFile,
		[]string{"tlsKeyFile"}, "PATH", "",
		"TLS key file; see also bindHttps.")

	flag.Usage = func() {
		if !flags.Help {
			return
		}

		base := path.Base(os.Args[0])

		fmt.Fprintf(os.Stderr, "%s: couchbase full-text server\n", base)
		fmt.Fprintf(os.Stderr, "\nUsage: %s [flags]\n", base)
		fmt.Fprintf(os.Stderr, "\nFlags:\n")

		flagsByName := map[string]*flag.Flag{}
		flag.VisitAll(func(f *flag.Flag) {
			flagsByName[f.Name] = f
		})

		flags := []string(nil)
		for name := range flagAliasesInit {
			flags = append(flags, name)
		}
		sort.Strings(flags)

		for _, name := range flags {
			aliases := flagAliasesInit[name]
			a := []string(nil)
			for i := len(aliases) - 1; i >= 0; i-- {
				a = append(a, aliases[i])
			}
			f := flagsByName[name]
			fmt.Fprintf(os.Stderr, "  -%s %s\n",
				strings.Join(a, ", -"), flagKinds[name])
			fmt.Fprintf(os.Stderr, "      %s\n",
				strings.Join(strings.Split(f.Usage, "\n"),
					"\n      "))
		}

		fmt.Fprintf(os.Stderr, "\nExamples:")
		fmt.Fprintf(os.Stderr, examples)
		fmt.Fprintf(os.Stderr, "\nSee also:"+
			" http://github.com/couchbase/cbft\n\n")
	}

	return flagAliasesInit
}

const examples = `
  Getting started, using a couchbase (3.x) on localhost as the datasource:
    ./cbft -server=http://localhost:8091

  Example where cbft's configuration is kept in a couchbase "cfg-bucket":
    ./cbft -cfg=couchbase:http://cfg-bucket@CB_HOST:8091 \
           -server=http://CB_HOST:8091
`
