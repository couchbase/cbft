//  Copyright 2022-Present Couchbase, Inc.
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

	"github.com/couchbase/cbgt"
	log "github.com/couchbase/clog"
)

type streamingResponse struct {
	Name        string `json:"name,omitempty"`
	Controllers struct {
		Flush string `json:"flush,omitempty"`
	} `json:"controllers"`
}

func pauseHandleFunc(reader *bufio.Reader) (int, error) {
	resBytes, err := reader.ReadBytes('\n')
	if err != nil {
		if err == io.EOF {
			return 1, nil // Success
		}
		return 0, err
	}

	if len(resBytes) == 1 && resBytes[0] == '\n' {
		return 0, nil
	}

	sg := &streamingResponse{}
	err = json.Unmarshal(resBytes, &sg)
	if err != nil {
		return 0, err
	}

	// Pause failed.
	// TODO Replace with the right field, "hibernation-state" == "".
	if sg.Controllers.Flush == "" {
		return -1, nil
	}

	// Sill in the process of pausing.
	// TODO Replace with the right field, "hibernation-state" == "pausing".
	if sg.Controllers.Flush != "" {
		return 0, nil
	}

	return 0, nil
}

func resumeHandleFunc(reader *bufio.Reader) (int, error) {
	resBytes, err := reader.ReadBytes('\n')
	if err != nil {
		if err == io.EOF {
			return -1, nil
		}
		return 0, err
	}

	if len(resBytes) == 1 && resBytes[0] == '\n' {
		return 0, nil
	}

	sg := &streamingResponse{}
	err = json.Unmarshal(resBytes, &sg)
	if err != nil {
		// Failure since failure to unmarshal indicates that the bucket
		// is deleted. Ref: VerifySourceNotExists()
		return -1, err
	}

	// Sill in the process of resuming.
	// TODO Replace with the right field, "hibernation-state" == "resuming".
	if sg.Controllers.Flush == "" {
		return 1, nil // Success
	}

	// TODO Replace with the right field, "hibernation-state" == "".
	if sg.Controllers.Flush != "" {
		return 0, nil
	}

	return 0, nil
}

func TrackBucketState(mgr *cbgt.Manager, operation, bucketName string) (int, error) {
	mgr.RegisterHibernationBucketTracker(bucketName)

	if operation == cbgt.HIBERNATE_TASK {
		return setupStreamingEndpoint(mgr, bucketName, pauseHandleFunc)
	} else if operation == cbgt.UNHIBERNATE_TASK {
		return setupStreamingEndpoint(mgr, bucketName, resumeHandleFunc)
	}
	return 0, fmt.Errorf("bucket state: not implemented for operations" +
		"other than hibernation and unhibernation")
}

func setupStreamingEndpoint(mgr *cbgt.Manager, bucketName string,
	handleResponse func(*bufio.Reader) (int, error)) (int, error) {
	var err error

	bucketStreamingURI := mgr.Server() + "/pools/default/bucketsStreaming/" +
		bucketName

	backoffStartSleepMS := 500
	backoffFactor := float32(1.5)
	backoffMaxSleepMS := 60000

	var resp *http.Response
	var status int

	// keep retrying with backoff logic upon connection errors.
	cbgt.ExponentialBackoffLoop("listening to "+bucketStreamingURI,
		func() int {
			u, err := cbgt.CBAuthURL(bucketStreamingURI)
			if err != nil {
				log.Warnf("bucket state: error adding credentials to url: %v",
					err)
				return 0
			}

			req, err := http.NewRequest("GET", u, nil)
			if err != nil {
				log.Warnf("bucket state: req, err: %v", err)
				return 0
			}
			req.Header.Add("Content-Type", "application/json")

			resp, err = cbgt.HttpClient().Do(req)
			if err != nil || resp.StatusCode != 200 {
				log.Warnf("bucket state: http client, response: %+v, err: %v", resp, err)
				return 0
			}

			log.Printf("bucket state: pool streaming started for bucket %s",
				bucketName)
			return -1 // success
		},
		backoffStartSleepMS, backoffFactor, backoffMaxSleepMS,
	)

	reader := bufio.NewReader(resp.Body)

	status = 0

	for status == 0 {
		status, err = handleResponse(reader)
		if status == 1 || status == -1 {
			return status, nil
		}
		// Exit at the first error.
		if err != nil {
			resp.Body.Close()
			return status, err
		}

		// continue if neither case, waiting for either conclusive
		// success or failure
		continue
	}

	return status, err
}
