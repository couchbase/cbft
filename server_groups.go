//  Copyright 2021-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package cbft

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math"
	"net/http"
	"strings"
	"time"

	"github.com/couchbase/cbgt"
	log "github.com/couchbase/clog"
)

// serverGroupTracker tracks the server group changes within a
// cluster and reflects those cross rack node movemements to
// the node definitions.
type serverGroupTracker struct {
	mgr     *cbgt.Manager
	url     string
	prevRev string
	notifCh chan *streamingPoolResponse
}

// streamingPoolResponse represents the server group uri
// details from a poolStreaming endpoint.
type streamingPoolResponse struct {
	ServerGroupsUri string `json:"serverGroupsUri"`
}

func StartServerGroupTracker(mgr *cbgt.Manager) {
	sg := serverGroupTracker{
		url:     mgr.Server() + "/poolsStreaming/default",
		mgr:     mgr,
		notifCh: make(chan *streamingPoolResponse, 1),
	}

	go sg.listen()
}

func (st *serverGroupTracker) listen() {
	decodeJson := func(resp []byte) (interface{}, error) {
		sg := &streamingPoolResponse{}
		err := json.Unmarshal(resp, &sg)
		if err != nil {
			return nil, err
		}
		return sg, err
	}

	notifyUpdates := func(res interface{}) error {
		var ok bool
		var notif *streamingPoolResponse
		if notif, ok = res.(*streamingPoolResponse); !ok {
			return fmt.Errorf("server_groups: invalid stream response %v", res)
		}
		st.notifCh <- notif
		return nil
	}

	st.handleServerGroupUpdates()

	err := listenStreamingEndpoint(st.url, decodeJson,
		notifyUpdates, nil)
	if err != nil {
		log.Errorf("server_groups: listenStreamingEndpoint, err: %v", err)
	}
}

// handleServerGroupUpdates listens to the server group updates
// and debounces the updates to minimise the config writes.
// It reuses the cfg debounce configuration values.
func (st *serverGroupTracker) handleServerGroupUpdates() {
	if st.mgr.Cfg() == nil {
		return
	}

	// get the config debounce offset and node offset multiplier.
	offset, nm := st.mgr.GetCfgDeBounceOffsetAndMultiplier()

	// compute the node specific offset from the node index.
	pos := 1
	nodeDefs, _ := st.mgr.GetNodeDefs(cbgt.NODE_DEFS_KNOWN, false)
	if nodeDefs != nil {
		for _, nodeDef := range nodeDefs.NodeDefs {
			if nodeDef.UUID == st.mgr.UUID() {
				break
			}
			pos++
		}
	}

	dbinterval := nm * pos * offset
	log.Printf("server_groups: debounce duration: %d MS", dbinterval)

	var resp *streamingPoolResponse

	go func() {

		for {

			resp = <-st.notifCh

			debounceTimeCh := time.After(time.Millisecond * time.Duration(dbinterval))
		DEBOUNCE_LOOP:
			for {

				select {
				case resp = <-st.notifCh:
					// NOOP upon more updates.
				case <-debounceTimeCh:
					break DEBOUNCE_LOOP
				}
			}

			if !strings.Contains(resp.ServerGroupsUri, "v=") {
				log.Printf("server_groups: no rev found %s",
					resp.ServerGroupsUri)
				continue
			}

			// parse the server group revision number.
			var msgs []string
			rev := resp.ServerGroupsUri[strings.Index(
				resp.ServerGroupsUri, "v=")+2:]

			// skip if there is no change in the server group rev numbers.
			if rev == st.prevRev {
				log.Printf("server_groups: rev %s, st.prevRev: %s", rev, st.prevRev)
				continue
			}

			// get the node UUIDs for all the fts nodes.
			nodeUUIDs := getNodeUUIDs(st.mgr)

			// get the latest server group for all the nodes.
			nodeHierarchy, err := fetchServerGroupDetails(st.mgr, nodeUUIDs)
			if err != nil {
				continue
			}

			// update the server groups for all the nodes if required.
			for _, kind := range []string{cbgt.NODE_DEFS_KNOWN,
				cbgt.NODE_DEFS_WANTED} {

				nodeDefs, _, err := cbgt.CfgGetNodeDefs(st.mgr.Cfg(), kind)
				if err != nil {
					continue
				}

				var updated bool
				nodeDefs.UUID = cbgt.NewUUID()
				for uuid, nodeDef := range nodeDefs.NodeDefs {
					if container, ok := nodeHierarchy[uuid]; ok {
						if nodeDef.Container != container {
							msgs = append(msgs, fmt.Sprintf("server_groups:"+
								" node: %s has moved from server group: %s to"+
								" group: %s", uuid, nodeDef.Container,
								container))
							nodeDef.Container = container
							updated = true
						}
					}
				}

				if !updated {
					log.Printf("server_groups: no server group membership"+
						" updates found for nodeDefs kind: %s", kind)
					continue
				}

				for {
					_, err = cbgt.CfgSetNodeDefs(st.mgr.Cfg(), kind,
						nodeDefs, math.MaxUint64)
					if err != nil {
						if _, ok := err.(*cbgt.CfgCASError); ok {
							// retry if it was a CAS mismatch
							continue
						}
						log.Printf("server_groups: CfgSetNodeDefs failed, for"+
							" kind: %s, err: %v", kind, err)
						break
					}

					st.prevRev = rev
					log.Printf("server_groups: nodeDefs updated for kind: %s", kind)
					break
				}
			}

			if len(msgs) > 0 {
				msgs = cbgt.StringsRemoveDuplicates(msgs)
				for _, msg := range msgs {
					log.Printf(msg)
				}
			}
		}
	}()

}

// getNodeUUIDs collects all the unique search node UUIDs
// from the node definitions.
func getNodeUUIDs(mgr *cbgt.Manager) []string {
	var nodeUUIDs []string
	for _, kind := range []string{cbgt.NODE_DEFS_KNOWN,
		cbgt.NODE_DEFS_WANTED} {
		nodeDefs, err := mgr.GetNodeDefs(kind, true)
		if err != nil {
			continue
		}
		for _, nodeDef := range nodeDefs.NodeDefs {
			nodeUUIDs = append(nodeUUIDs, nodeDef.UUID)
		}
	}
	return cbgt.StringsRemoveDuplicates(nodeUUIDs)
}

type serverGroups struct {
	ServerGroups []serverGroup `json:"groups"`
}

type serverGroup struct {
	Name        string       `json:"name"`
	NodeDetails []nodeDetail `json:"nodes"`
}

type nodeDetail struct {
	NodeUUID string `json:"nodeUUID"`
}

// fetchServerGroupDetails retrives the server group node hierarchy
// for all the given node UUIDs.
func fetchServerGroupDetails(mgr *cbgt.Manager, uuids []string) (
	map[string]string, error) {
	if len(uuids) == 0 {
		return nil, fmt.Errorf("empty UUID list")
	}

	url := mgr.Server() + "/pools/default/serverGroups"
	u, err := cbgt.CBAuthURL(url)
	if err != nil {
		return nil, fmt.Errorf("server_groups: auth for ns_server,"+
			" url: %s, authType: %s, err: %v",
			url, "cbauth", err)
	}

	req, err := http.NewRequest("GET", u, nil)
	if err != nil {
		return nil, err
	}

	resp, err := HttpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	respBuf, err := ioutil.ReadAll(resp.Body)
	if err != nil || len(respBuf) == 0 {
		return nil, fmt.Errorf("server_groups: error reading resp.Body,"+
			" url: %s, resp: %#v, err: %v", url, resp, err)
	}

	var sg serverGroups
	err = json.Unmarshal(respBuf, &sg)
	if err != nil {
		return nil, fmt.Errorf("server_groups: error parsing respBuf: %s,"+
			" url: %s, err: %v", respBuf, url, err)
	}

	uuidMap := cbgt.StringsToMap(uuids)
	nodeHierarchy := make(map[string]string, len(uuids))
	for _, g := range sg.ServerGroups {
		for _, nd := range g.NodeDetails {
			if _, found := uuidMap[nd.NodeUUID]; found {
				nodeHierarchy[nd.NodeUUID] = "datacenter/" + g.Name
			}
		}
	}

	return nodeHierarchy, nil
}

// listenStreamingEndpoint is a generic stream endpoint listen
// utility function which accepts callback methods for decoding the
// stream response, notify methods and a stop signalling channel.
func listenStreamingEndpoint(url string, decodeResponse func([]byte) (
	interface{}, error), notify func(interface{}) error,
	stopCh chan struct{}) error {
	u, err := cbgt.CBAuthURL(url)
	if err != nil {
		return fmt.Errorf("server_groups: auth for ns_server,"+
			" server: %s, authType: %s, err: %v",
			url, "cbauth", err)
	}

	req, err := http.NewRequest("GET", u, nil)
	if err != nil {
		return err
	}

	req.Header.Add("Content-Type", "application/json")

	resp, err := HttpClient.Do(req)
	if err != nil {
		return err
	}

	reader := bufio.NewReader(resp.Body)
	defer resp.Body.Close()

	for {
		if stopCh != nil {
			select {
			case <-stopCh:
				return nil
			default:
			}
		}

		resBytes, err := reader.ReadBytes('\r')
		if err != nil {
			log.Printf("server_groups: reader, err: %v", err)
			continue
		}
		if len(resBytes) == 1 && resBytes[0] == '\r' {
			continue
		}

		res, err := decodeResponse(resBytes)
		if err != nil {
			log.Printf("server_groups: decodeResponse, err: %v", err)
			continue
		}

		err = notify(res)
		if err != nil {
			log.Printf("server_groups: notify, err: %v", err)
			continue
		}
	}

}
