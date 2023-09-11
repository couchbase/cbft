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
	Name             string `json:"name,omitempty"`
	HibernationState string `json:"hibernationState,omitempty"`
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
	err = UnmarshalJSON(resBytes, &sg)
	if err != nil {
		return 0, err
	}

	// Pause failed.
	if sg.HibernationState == "" {
		return -1, nil
	}

	// Sill in the process of pausing.
	if sg.HibernationState == "pausing" {
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
	err = UnmarshalJSON(resBytes, &sg)
	if err != nil {
		// Failure since failure to unmarshal indicates that the bucket
		// is deleted. Ref: VerifySourceNotExists()
		return -1, err
	}

	if sg.HibernationState == "" {
		return 1, nil // Success
	}

	// Sill in the process of resuming.
	if sg.HibernationState == "resuming" {
		return 0, nil
	}

	return 0, nil
}

func TrackBucketState(mgr *cbgt.Manager, operation, bucketName string) (int, error) {
	mgr.RegisterHibernationBucketTracker(bucketName)

	bucketStreamingURI, err := getBucketStreamingURI(mgr, bucketName)
	if err != nil {
		if err == ErrBucketNotFound {
			if operation == cbgt.HIBERNATE_TASK {
				return 1, nil // Pause is a success
			} else if operation == cbgt.UNHIBERNATE_TASK {
				return -1, nil // Resume has failed
			}
		} else {
			return -1, err
		}
	}

	if operation == cbgt.HIBERNATE_TASK {
		return setupStreamingEndpoint(mgr, bucketStreamingURI, bucketName, pauseHandleFunc)
	} else if operation == cbgt.UNHIBERNATE_TASK {
		return setupStreamingEndpoint(mgr, bucketStreamingURI, bucketName, resumeHandleFunc)
	}
	return 0, fmt.Errorf("bucket state: not implemented for operations" +
		"other than hibernation and unhibernation")
}

type bucketDefaultResponse struct {
	Name         string `json:"name"`
	StreamingURI string `json:"streamingUri"`
}

var ErrBucketNotFound = fmt.Errorf("bucket state: bucket not found")

func getBucketStreamingURI(mgr *cbgt.Manager, bucketName string) (string, error) {
	poolDefaultURL := mgr.Server() + "/pools/default/buckets"

	u, err := cbgt.CBAuthURL(poolDefaultURL)
	if err != nil {
		log.Warnf("bucket state: error adding credentials to streaming URI: %v",
			err)
		return "", err
	}

	req, err := http.NewRequest("GET", u, nil)
	if err != nil {
		log.Warnf("bucket state: req, err: %v", err)
		return "", err
	}
	req.Header.Add("Content-Type", "application/json")

	resp, err := cbgt.HttpClient().Do(req)
	if err != nil || resp.StatusCode != 200 {
		log.Warnf("bucket state: http client, response: %+v, err: %v", resp, err)
		return "", err
	}

	var bucketsDefaultResponse []bucketDefaultResponse
	err = json.NewDecoder(resp.Body).Decode(&bucketsDefaultResponse)
	if err != nil {
		return "", err
	}

	for _, bucketResponse := range bucketsDefaultResponse {
		if bucketResponse.Name == bucketName {
			return bucketResponse.StreamingURI, nil
		}
	}

	return "", ErrBucketNotFound
}

func setupStreamingEndpoint(mgr *cbgt.Manager, bucketStreamingURI, bucketName string,
	handleResponse func(*bufio.Reader) (int, error)) (int, error) {
	var err error

	bucketStreamingURL := mgr.Server() + bucketStreamingURI

	backoffStartSleepMS := 500
	backoffFactor := float32(1.5)
	backoffMaxSleepMS := 60000

	var resp *http.Response
	var status int

	// keep retrying with backoff logic upon connection errors.
	cbgt.ExponentialBackoffLoop("listening to "+bucketStreamingURL,
		func() int {
			u, err := cbgt.CBAuthURL(bucketStreamingURL)
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
			if err != nil || (resp.StatusCode != 200 && resp.StatusCode != 404) {
				log.Warnf("bucket state: http client, response: %+v, err: %v", resp, err)
				return 0
			} else if resp.StatusCode == 404 {
				status = -1
				return -1
			}

			log.Printf("bucket state: pool streaming started for bucket %s",
				bucketName)
			return -1 // success
		},
		backoffStartSleepMS, backoffFactor, backoffMaxSleepMS,
	)

	reader := bufio.NewReader(resp.Body)
	if status == -1 {
		return status, fmt.Errorf("bucket state: endpoint not found for bucket %s", bucketName)
	}
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
