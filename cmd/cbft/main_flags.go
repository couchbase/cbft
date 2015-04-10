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

type Flags struct {
	BindHttp   string
	CfgConnect string
	Container  string
	DataDir    string
	Help       bool
	Register   string
	Server     string
	StaticDir  string
	StaticETag string
	Tags       string
	Version    bool
	Weight     int
}

var flags Flags
var flagAliases map[string][]string

func init() {
	flagAliases = initFlags(&flags)
}

func initFlags(flags *Flags) map[string][]string {
	flagAliases := map[string][]string{} // main flag name => all aliases.
	flagKinds := map[string]string{}

	s := func(v *string, names []string, kind string,
		defaultVal, usage string) { // String cmd-line param.
		for _, name := range names {
			flag.StringVar(v, name, defaultVal, usage)
		}
		flagAliases[names[0]] = names
		flagKinds[names[0]] = kind
	}

	i := func(v *int, names []string, kind string,
		defaultVal int, usage string) { // Integer cmd-line param.
		for _, name := range names {
			flag.IntVar(v, name, defaultVal, usage)
		}
		flagAliases[names[0]] = names
		flagKinds[names[0]] = kind
	}

	b := func(v *bool, names []string, kind string,
		defaultVal bool, usage string) { // Bool cmd-line param.
		for _, name := range names {
			flag.BoolVar(v, name, defaultVal, usage)
		}
		flagAliases[names[0]] = names
		flagKinds[names[0]] = kind
	}

	s(&flags.BindHttp,
		[]string{"bindHttp", "b"}, "ADDR:PORT", "localhost:8095",
		"optional HTTP/REST listen addr:port;"+
			"\ndefault is 'localhost:8095'.")
	s(&flags.CfgConnect,
		[]string{"cfgConnect", "cfg", "c"}, "CFG_CONNECT", "simple",
		"optional connection string/info to a configuration server"+
			"\nfor clustering multiple cbft nodes:"+
			"\n* couchbase:http://BUCKET_USER:BUCKET_PSWD@CB_HOST:CB_PORT"+
			"\n     - manages a cbft cluster configuration in a couchbase"+
			"\n       bucket; for example:"+
			"\n       'couchbase:http://my-cfg-bucket@127.0.0.1:8091';"+
			"\n* simple"+
			"\n     - intended for development usage, the 'simple'"+
			"\n       configuration provider manages a configuration"+
			"\n       for a single, unclustered cbft node in a local,"+
			"\n       simple file that's stored in the dataDir;"+
			"\ndefault is 'simple'.")
	s(&flags.Container,
		[]string{"container"}, "PATH", "",
		"optional slash separated path of logical parent containers"+
			"\nfor this node, for shelf/rack/row/zone awareness.")
	s(&flags.DataDir,
		[]string{"dataDir", "data"}, "DIR", "data",
		"optional directory path where local index data and local"+
			"\nconfiguration files will be stored on this node;"+
			"\ndefault is 'data'.")
	b(&flags.Help,
		[]string{"help", "?", "H", "h"}, "", false,
		"print this usage message and exit.")
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
		"required URL to datasource server;"+
			"\nexample for couchbase: 'http://localhost:8091';"+
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
	b(&flags.Version,
		[]string{"version", "v"}, "", false,
		"print version string and exit.")
	i(&flags.Weight,
		[]string{"weight"}, "INTEGER", 1,
		"optional weight of this node, where a more capable"+
			"\nnode should have higher weight; default is 1.")

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
		for name := range flagAliases {
			flags = append(flags, name)
		}
		sort.Strings(flags)

		for _, name := range flags {
			aliases := flagAliases[name]
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

		fmt.Fprintf(os.Stderr, "\nExample:")
		fmt.Fprintf(os.Stderr, example)
		fmt.Fprintf(os.Stderr, "\nSee also:"+
			" http://github.com/couchbaselabs/cbft\n\n")
	}

	return flagAliases
}

const example = `
  ./cbft -bindHttp=localhost:9090 \
         -cfg=couchbase:http://my-cfg-bucket@localhost:8091 \
         -data=/var/data/cbft-node-9090 \
         -server=http://localhost:8091
`
