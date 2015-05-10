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

package cbft

import (
	"encoding/json"
	"fmt"
	"hash"
	"hash/crc32"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	log "github.com/couchbase/clog"
)

const FILES_FEED_SLEEP_START_MS = 5000
const FILES_FEED_BACKOFF_FACTOR = 1.5
const FILES_FEED_MAX_SLEEP_MS = 1000 * 60 * 5 // 5 minutes.

func init() {
	RegisterFeedType("files", &FeedType{
		Start:      StartFilesFeed,
		Partitions: FilesFeedPartitions,
		Public:     true,
		Description: "general/files" +
			" - files under a dataDir subdirectory tree will be the data source",
		StartSample: &FilesFeedParams{
			RegExps:       []string{".txt$", ".md$"},
			SleepStartMS:  FILES_FEED_SLEEP_START_MS,
			BackoffFactor: FILES_FEED_BACKOFF_FACTOR,
			MaxSleepMS:    FILES_FEED_MAX_SLEEP_MS,
		},
	})
}

// FilesFeed is a feed implementation that that emits file contents
// from a local subdirectory tree.
//
// The subdirectory tree lives under the dataDir...
//
//    <dataDir>/<sourceName/**
//
// FilesFeed supports optional regexp patterns to allow you to filter
// for only the file paths that you want.
//
// Limitations:
//
// - Only a small number of files will work well (hundreds to low
// thousands, not millions).
//
// - FilesFeed uses file modification timestamps as a poor-man's
// approach instead of properly tracking sequence numbers.  That has
// implications such as whenever a FilesFeed (re-)starts (e.g., the
// process restarts), the FilesFeed will re-emits all files and then
// track the max modification timestamp going forwards as it regularly
// polls for file changes.
type FilesFeed struct {
	mgr        *Manager
	name       string
	indexName  string
	sourceName string
	params     *FilesFeedParams
	dests      map[string]Dest
	disable    bool

	m       sync.Mutex
	closeCh chan struct{}
}

type FilesFeedParams struct {
	RegExps       []string `json:"regExps"`
	MaxFileSize   int64    `json:"maxFileSize"`
	NumPartitions int      `json:"numPartitions"`
	SleepStartMS  int      `json:"sleepStartMS"`
	BackoffFactor float32  `json:"backoffFactor"`
	MaxSleepMS    int      `json:"maxSleepMS"`
}

type FileDoc struct {
	Name     string `json:"name"`
	Path     string `json:"path"` // Path relative to the source name.
	Contents string `json:"contents"`
}

func StartFilesFeed(mgr *Manager, feedName, indexName, indexUUID,
	sourceType, sourceName, sourceUUID, params string,
	dests map[string]Dest) error {
	feed, err := NewFilesFeed(mgr, feedName, indexName, sourceName,
		params, dests, mgr.tagsMap != nil && !mgr.tagsMap["feed"])
	if err != nil {
		return fmt.Errorf("feed_files: NewFilesFeed,"+
			" feedName: %s, err: %v", feedName, err)
	}
	err = feed.Start()
	if err != nil {
		return fmt.Errorf("feed_files: could not start,"+
			" feedName: %s, err: %v", feedName, err)
	}
	err = mgr.registerFeed(feed)
	if err != nil {
		feed.Close()
		return err
	}
	return nil
}

func NewFilesFeed(mgr *Manager, name, indexName, sourceName,
	paramsStr string, dests map[string]Dest, disable bool) (
	*FilesFeed, error) {
	if sourceName == "" {
		return nil, fmt.Errorf("feed_files: missing source name")
	}

	if strings.Index(sourceName, "..") >= 0 {
		return nil, fmt.Errorf("feed_files: disallowed source name,"+
			" name: %s, sourceName: %q", name, sourceName)
	}

	params := &FilesFeedParams{}
	if paramsStr != "" {
		err := json.Unmarshal([]byte(paramsStr), params)
		if err != nil {
			return nil, err
		}
	}

	return &FilesFeed{
		mgr:        mgr,
		name:       name,
		indexName:  indexName,
		sourceName: sourceName,
		params:     params,
		dests:      dests,
		disable:    disable,
		closeCh:    make(chan struct{}),
	}, nil
}

func (t *FilesFeed) Name() string {
	return t.name
}

func (t *FilesFeed) IndexName() string {
	return t.indexName
}

func (t *FilesFeed) Start() error {
	if t.disable {
		log.Printf("feed_files: disable, name: %s", t.Name())
		return nil
	}

	startSleepMS := t.params.SleepStartMS
	if startSleepMS <= 0 {
		startSleepMS = FILES_FEED_SLEEP_START_MS
	}

	backoffFactor := t.params.BackoffFactor
	if backoffFactor <= 0 {
		backoffFactor = FILES_FEED_BACKOFF_FACTOR
	}

	maxSleepMS := t.params.MaxSleepMS
	if maxSleepMS <= 0 {
		maxSleepMS = FILES_FEED_MAX_SLEEP_MS
	}

	numPartitions := t.params.NumPartitions
	if numPartitions < 0 {
		numPartitions = 0
	}

	partitions := make([]string, numPartitions)
	for i := 0; i < len(partitions); i++ {
		partitions[i] = strconv.Itoa(i)
	}

	go func() {
		initTime := time.Now()
		initTimeMicroSecs := initTime.UnixNano() / int64(1000)

		// TODO: NOTE: We're assuming (lazily, incorrectly) that this
		// way of initializing a sequence number never goes downwards,
		// even during fast restarts or clock changes or node
		// rebalances/reassignments.
		seqs := map[string]uint64{}
		for partition, _ := range t.dests {
			seqs[partition] = uint64(initTimeMicroSecs)
		}

		var prevStartTime time.Time

		ExponentialBackoffLoop(t.Name(),
			func() int {
				select {
				case <-t.closeCh:
					return -1
				default:
				}

				h := crc32.NewIEEE()

				startTime := time.Now()

				progress := false

				paths, err := FilesFindMatches(t.mgr.DataDir(),
					t.sourceName, t.params.RegExps, prevStartTime,
					t.params.MaxFileSize)
				if err != nil {
					log.Printf("feed_files, FilesFindMatches, err: %v", err)
					return -1
				}

				seqDeltaMax := uint64(0)

				seqEnds := map[string]uint64{}

				for _, path := range paths {
					partition := FilesPathToPartition(h, partitions, path)

					if t.dests[partition] == nil {
						continue
					}

					seq := seqs[partition]

					seqEnd, exists := seqEnds[partition]
					if exists {
						seqEnd = seqEnd + 1
					} else {
						seqEnd = seq
					}
					seqEnds[partition] = seqEnd

					if seqDeltaMax < seqEnd-seq {
						seqDeltaMax = seqEnd - seq
					}
				}

				snapshotSent := map[string]bool{}

				for _, path := range paths {
					select {
					case <-t.closeCh:
						return -1
					default:
					}

					partition := FilesPathToPartition(h, partitions, path)

					dest := t.dests[partition]
					if dest == nil {
						continue
					}

					seqCur := seqs[partition]
					seqs[partition] = seqCur + 1

					buf, err := ioutil.ReadFile(path)
					if err != nil {
						log.Printf("feed_files: read file,"+
							" name: %s, path: %s, err: %v",
							t.Name(), path, err)
						continue
					}

					jbuf, err := json.Marshal(FileDoc{
						Name:     filepath.Base(path),
						Path:     path,
						Contents: string(buf),
					})
					if err != nil {
						log.Printf("feed_files: json marshal file,"+
							" name: %s, path: %s, err: %v",
							t.Name(), path, err)
						continue
					}

					if !snapshotSent[partition] {
						err = dest.SnapshotStart(partition, seqCur,
							seqEnds[partition])
						if err != nil {
							log.Printf("feed_files: SnapshotStart,"+
								" name: %s, partition: %s,"+
								" seqCur: %d, seqEnd: %d, err: %v",
								t.Name(), partition,
								seqCur, seqEnds[partition], err)
							return -1
						}

						snapshotSent[partition] = true
					}

					pathBuf := []byte(path)

					err = dest.DataUpdate(partition, pathBuf, seqCur, jbuf)
					if err != nil {
						log.Printf("feed_files: DataUpdate,"+
							" name: %s, path: %s, partition: %s,"+
							" seqCur: %d, err: %v",
							t.Name(), path, partition, seqCur, err)
						return -1
					}

					progress = true
				}

				prevStartTime = startTime

				// NOTE: We may need to sleep a certain amount in case
				// there were tons of file updates/mutations, and we
				// want to reduce the window of potentially repeating
				// sequence numbers.  The window still exists if we
				// crash and quickly restart during the sleep, where
				// the restarted process might have a
				// lower-than-wanted initTime.
				wantTime := initTime // Copy, because Add() mutates.
				wantTime.Add(time.Duration(int64(seqDeltaMax)))

				currTime := time.Now()
				if wantTime.After(currTime) {
					time.Sleep(wantTime.Sub(currTime))
				}

				if progress {
					return 1
				}
				return 0
			},
			startSleepMS,
			backoffFactor,
			maxSleepMS)
	}()

	return nil
}

func (t *FilesFeed) Close() error {
	t.m.Lock()
	if t.closeCh != nil {
		close(t.closeCh)
		t.closeCh = nil
	}
	t.m.Unlock()

	return nil
}

func (t *FilesFeed) Dests() map[string]Dest {
	return t.dests
}

func (t *FilesFeed) Stats(w io.Writer) error {
	_, err := w.Write([]byte("{}"))
	return err
}

// -----------------------------------------------------

func FilesFeedPartitions(sourceType, sourceName, sourceUUID, sourceParams,
	server string) ([]string, error) {
	ffp := &FilesFeedParams{}
	if sourceParams != "" {
		err := json.Unmarshal([]byte(sourceParams), ffp)
		if err != nil {
			return nil, fmt.Errorf("feed_files:"+
				" could not parse sourceParams: %s, err: %v",
				sourceParams, err)
		}
	}
	rv := make([]string, ffp.NumPartitions)
	for i := 0; i < ffp.NumPartitions; i++ {
		rv[i] = strconv.Itoa(i)
	}
	return rv, nil
}

// -----------------------------------------------------

func FilesFindMatches(dataDir, sourceName string,
	regExps []string, modTimeGTE time.Time, maxSize int64) (
	[]string, error) {
	walkPath, err := filepath.EvalSymlinks(dataDir +
		string(os.PathSeparator) + "files" +
		string(os.PathSeparator) + sourceName)
	if err != nil {
		return nil, err
	}

	pathsOk := []string(nil)

	err = filepath.Walk(walkPath,
		func(path string, fi os.FileInfo, err error) error {
			if err != nil ||
				fi.IsDir() ||
				fi.ModTime().Before(modTimeGTE) ||
				(maxSize > 0 && fi.Size() > maxSize) {
				return nil
			}

			if len(regExps) <= 0 {
				pathsOk = append(pathsOk, path)
				return nil
			}

			for _, reStr := range regExps {
				matched, err := regexp.MatchString(reStr, path)
				if err != nil {
					return fmt.Errorf("feed_files, MatchString,"+
						" reStr: %s, path: %s, err: %v",
						reStr, path, err)
				}
				if matched {
					pathsOk = append(pathsOk, path)
					return nil
				}
			}

			return nil
		})
	if err != nil {
		return nil, err
	}

	return pathsOk, nil
}

func FilesPathToPartition(h hash.Hash32,
	partitions []string, path string) string {
	if len(partitions) <= 0 {
		return ""
	}

	h.Reset()
	io.WriteString(h, path)
	i := h.Sum32() % uint32(len(partitions))
	return partitions[i]
}
