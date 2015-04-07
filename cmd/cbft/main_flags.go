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
	BindAddr   string
	DataDir    string
	Help       bool
	LogFlags   string
	StaticDir  string
	StaticETag string
	Server     string
	Tags       string
	Container  string
	Version    bool
	Weight     int
	Register   string
	CfgConnect string
}

var flags Flags
var flagAliases map[string][]string

func init() {
	flagAliases = initFlags(&flags)
}

func initFlags(flags *Flags) map[string][]string {
	flagAliases := map[string][]string{} // main flag name => all aliases.

	s := func(v *string, names []string,
		defaultVal, usage string) { // String cmd-line param.
		for _, name := range names {
			flag.StringVar(v, name, defaultVal, usage)
		}
		flagAliases[names[0]] = names
	}

	i := func(v *int, names []string,
		defaultVal int, usage string) { // Integer cmd-line param.
		for _, name := range names {
			flag.IntVar(v, name, defaultVal, usage)
		}
		flagAliases[names[0]] = names
	}

	b := func(v *bool, names []string,
		defaultVal bool, usage string) { // Bool cmd-line param.
		for _, name := range names {
			flag.BoolVar(v, name, defaultVal, usage)
		}
		flagAliases[names[0]] = names
	}

	s(&flags.BindAddr,
		[]string{"bindAddr"}, "localhost:8095",
		"http listen address:port")
	s(&flags.DataDir,
		[]string{"dataDir", "data"}, "data",
		"directory path where index data and"+
			"\nlocal configuration files will be stored")
	b(&flags.Help,
		[]string{"help", "?", "H", "h"}, false,
		"print this usage message and exit")
	s(&flags.LogFlags,
		[]string{"logFlags"}, "",
		"comma-separated logging control flags")
	s(&flags.StaticDir,
		[]string{"staticDir"}, "static",
		"directory for static web UI content")
	s(&flags.StaticETag,
		[]string{"staticETag"}, "",
		"static etag value")
	s(&flags.Server,
		[]string{"server"}, "",
		"url to datasource server;"+
			"\nexample for couchbase: http://localhost:8091")
	s(&flags.Tags,
		[]string{"tags"}, "",
		"comma-separated list of tags (or roles) for this node")
	s(&flags.Container,
		[]string{"container"}, "",
		"slash separated path of parent containers for this node,"+
			"\nfor shelf/rack/row/zone awareness")
	b(&flags.Version,
		[]string{"version", "v"}, false,
		"print version string and exit")
	i(&flags.Weight,
		[]string{"weight"}, 1,
		"weight of this node (a more capable node has higher weight)")
	s(&flags.Register,
		[]string{"register"}, "wanted",
		"register this node as wanted, wantedForce,"+
			"\nknown, knownForce, unwanted, unknown or unchanged")
	s(&flags.CfgConnect,
		[]string{"cfgConnect", "cfg"}, "simple",
		"connection string/info to configuration provider")

	flag.Usage = func() {
		if !flags.Help {
			return
		}

		base := path.Base(os.Args[0])

		fmt.Fprintf(os.Stderr,
			"%s: couchbase full-text server\n\n", base)
		fmt.Fprintf(os.Stderr, "more information is available at:\n"+
			"  http://github.com/couchbaselabs/cbft\n\n")
		fmt.Fprintf(os.Stderr, "usage:\n  %s [flags]\n\n", base)
		fmt.Fprintf(os.Stderr, "flags:\n")

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
			fmt.Fprintf(os.Stderr, "  -%s=%q\n",
				strings.Join(a, ", -"), f.Value)
			fmt.Fprintf(os.Stderr, "      %s\n",
				strings.Join(strings.Split(f.Usage, "\n"),
					"\n      "))
		}
	}

	return flagAliases
}
