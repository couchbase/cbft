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
	"io"
	"net/http"
	"sort"
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
	decodeAndNotifyResponse := func(resp []byte) error {
		sg := &streamingPoolResponse{}
		err := json.Unmarshal(resp, &sg)
		if err != nil {
			return err
		}
		st.notifCh <- sg
		return nil
	}

	st.handleServerGroupUpdates()

	err := ListenStreamingEndpoint("server_groups", st.url, decodeAndNotifyResponse, nil)
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

	// Sorting the node UUIDs to have a consistent ordering across the nodes.
	var sortedNodeUUID []string
	if nodeDefs != nil {
		for _, nodeDef := range nodeDefs.NodeDefs {
			sortedNodeUUID = append(sortedNodeUUID, nodeDef.UUID)
		}

		sort.Strings(sortedNodeUUID)

		for _, nodeDefUUID := range sortedNodeUUID {
			if nodeDefUUID == st.mgr.UUID() {
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
				log.Warnf("server_groups: no rev found %s",
					resp.ServerGroupsUri)
				continue
			}

			// parse the server group revision number.
			var msgs []string
			rev := resp.ServerGroupsUri[strings.Index(
				resp.ServerGroupsUri, "v=")+2:]

			// skip if there is no change in the server group rev numbers.
			if rev == st.prevRev {
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
					log.Debugf("server_groups: no server group membership"+
						" updates found for nodeDefs kind: %s", kind)
					continue
				}

				nodeDefsSetFunc := func() error {
					_, err = cbgt.CfgSetNodeDefs(st.mgr.Cfg(), kind,
						nodeDefs, cbgt.CFG_CAS_FORCE)
					return err
				}

				// Keep retrying till the node defs are successfully set.
				err = cbgt.RetryOnCASMismatch(nodeDefsSetFunc, -1)
				if err != nil {
					log.Warnf("server_groups: CfgSetNodeDefs failed, for"+
						" kind: %s, err: %v", kind, err)
					break
				}

				st.prevRev = rev
				log.Printf("server_groups: nodeDefs updated for kind: %s", kind)
				break
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

	resp, err := cbgt.HttpClient().Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	respBuf, err := io.ReadAll(resp.Body)
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
func ListenStreamingEndpoint(msg, url string,
	decodeAndNotifyResponse func([]byte) error, stopCh chan struct{}) error {

	backoffStartSleepMS := 500
	backoffFactor := float32(1.5)
	backoffMaxSleepMS := 60000

RECONNECT:
	for {

		stopCh2 := make(chan struct{})
		var resp *http.Response

		// keep retrying with backoff logic upon connection errors.
		cbgt.ExponentialBackoffLoop(msg+" listening",
			func() int {
				u, err := cbgt.CBAuthURL(url)
				if err != nil {
					log.Warnf(msg+": auth for ns_server,"+
						" server: %s, authType: %s, err: %v",
						url, "cbauth", err)
					return 0
				}

				req, err := http.NewRequest("GET", u, nil)
				if err != nil {
					log.Warnf(msg+": req, err: %v", err)
					return 0
				}
				req.Header.Add("Content-Type", "application/json")

				resp, err = cbgt.HttpClient().Do(req)
				if err != nil || (resp.StatusCode != 200 && resp.StatusCode != 404) {
					log.Warnf(msg+": http client, response: %+v, err: %v", resp, err)
					return 0
				} else if resp.StatusCode == 404 {
					return -1
				}

				log.Printf(msg + ": pool streaming started")
				return -1 // success
			},
			backoffStartSleepMS, backoffFactor, backoffMaxSleepMS,
		)
		terminator(stopCh, stopCh2, resp.Body)
		reader := bufio.NewReader(resp.Body)

		for {
			if stopCh != nil {
				select {
				case <-stopCh:
					resp.Body.Close()
					log.Warnf(msg + ": terminating pool streaming")
					return nil
				default:
				}
			}

			resBytes, err := reader.ReadBytes('\n')
			if err != nil {
				select {
				case <-stopCh:
					continue
				default:
					log.Warnf(msg+": reconnecting upon reader, bytes read: %d, err: %v",
						len(resBytes), err)
					close(stopCh2)
					resp.Body.Close()
					continue RECONNECT
				}
			}
			if len(resBytes) == 1 && resBytes[0] == '\n' {
				continue
			}

			err = decodeAndNotifyResponse(resBytes)
			if err != nil {
				log.Warnf(msg+": notify, err: %v", err)
				continue
			}
		}
	}
}

// terminator is a routine that would listen to stop channel in-parallel to the
// streaming endpoint listener. this aids in immediate cleanup of the routine,
// especially when the streaming endpoint has no updates to send
func terminator(stopCh, stopCh2 chan struct{}, respBody io.ReadCloser) {
	go func(stopCh, stopCh2 chan struct{}, respBody io.ReadCloser) {
		if stopCh != nil {
			select {
			case <-stopCh: // terminate the streaming endpoint listener altogether
				respBody.Close()
				return
			case <-stopCh2: // cleanup the terminator routine
				return
			}
		}
	}(stopCh, stopCh2, respBody)
}
