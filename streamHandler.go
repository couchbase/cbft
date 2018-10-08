//  Copyright (c) 2019 Couchbase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// 		http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cbft

import (
	"fmt"
	"sync"

	"github.com/blevesearch/bleve/search"
	pb "github.com/couchbase/cbft/protobuf"
	log "github.com/couchbase/clog"
)

var defaultStreamBatchSize = 100
var sliceStart = []byte{'['}
var sliceEnd = []byte{']'}
var itemGlue = []byte{','}

// streamHandler interface is for implementations
// which streams the document matches over streams
type streamHandler interface {
	write([]byte, []uint64, int) error
}

type streamer struct {
	m       sync.Mutex
	stream  pb.SearchService_SearchServer
	sizeSet bool
	skipSet bool
	total   int

	curSkip int
	curSize int
}

func newStreamHandler(size, skip int64, outStream pb.SearchService_SearchServer) *streamer {
	rv := &streamer{
		curSize: int(size),
		curSkip: int(skip),
		stream:  outStream,
	}
	if size > 0 {
		rv.sizeSet = true
	}
	if skip > 0 {
		rv.skipSet = true
	}
	return rv
}

func (s *streamer) write(b []byte, offsets []uint64, hitsCount int) error {
	s.m.Lock()
	s.total += hitsCount

	if s.curSkip > 0 && s.skipSet {
		if hitsCount <= s.curSkip {
			s.curSkip -= hitsCount
			s.m.Unlock()
			return nil
		}

		// advance the hit bytes by the skip factor
		loc := offsets[s.curSkip-1] + 1 // extra 1 to accommodate the glue size
		b = b[loc:]
		b = append(sliceStart, b...)
		offsets = offsets[s.curSkip:]
		hitsCount -= s.curSkip
		// accouting the sliceStart offset
		loc--
		// adjusting the offset due to slicing of b
		for i := range offsets {
			offsets[i] -= loc
		}
		s.curSkip = 0
	}

	// we have already streamed the requested page
	if s.curSize == 0 && s.sizeSet {
		s.m.Unlock()
		return fmt.Errorf("write: already streamed the requested hits")
	}

	if s.curSize > 0 && s.sizeSet {
		if hitsCount > s.curSize {
			b = b[:offsets[s.curSize-1]]
			offsets = offsets[:s.curSize]
			b = append(b, sliceEnd...)
			offsets[len(offsets)-1] = offsets[len(offsets)-1] + 1
			s.curSize = 0
		} else {
			s.curSize -= hitsCount
		}
	}

	hitRes := &pb.StreamSearchResults{
		PayLoad: &pb.StreamSearchResults_Hits{
			Hits: &pb.StreamSearchResults_Batch{
				Bytes:   b,
				Offsets: offsets,
				Total:   uint64(len(offsets)),
			},
		},
	}
	if err := s.stream.Send(hitRes); err != nil {
		s.m.Unlock()
		return err
	}

	s.m.Unlock()
	return nil
}

type docMatchHandler struct {
	bhits   []byte
	offsets []uint64
	ctx     *search.SearchContext
	s       *streamer
	n       int
}

func (dmh *docMatchHandler) documentMatchHandler(hit *search.DocumentMatch) error {
	if hit != nil {
		b, err := MarshalJSON(hit)
		if err != nil {
			log.Printf("streamHandler: json marshal err: %v", err)
			return err
		}

		if dmh.n > 0 {
			dmh.bhits = append(append(dmh.bhits, itemGlue...), b...)
		} else {
			dmh.bhits = append(dmh.bhits, b...)
		}
		// remember the ending offset position
		dmh.offsets = append(dmh.offsets, uint64(len(dmh.bhits)))
		dmh.n++
		if dmh.n < defaultStreamBatchSize {
			return nil
		}
	}

	if len(dmh.bhits) > 1 {
		dmh.bhits = append(dmh.bhits, sliceEnd...)
		err := dmh.s.write(dmh.bhits, dmh.offsets, dmh.n)
		dmh.bhits = dmh.bhits[:1]
		dmh.offsets = dmh.offsets[:0]
		dmh.n = 0
		return err
	}

	return nil
}

func (s *streamer) MakeDocumentMatchHandler(
	ctx *search.SearchContext) (search.DocumentMatchHandler, bool, error) {
	dmh := docMatchHandler{
		s:       s,
		ctx:     ctx,
		bhits:   make([]byte, 0, defaultStreamBatchSize*50),
		offsets: make([]uint64, 0, defaultStreamBatchSize),
	}
	dmh.bhits = append(dmh.bhits, sliceStart...)

	return dmh.documentMatchHandler, true, nil
}
