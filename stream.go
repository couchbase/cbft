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
	"encoding/binary"
	"fmt"
	"sync"

	"github.com/blevesearch/bleve"
	"github.com/blevesearch/bleve/search"
	"github.com/blevesearch/bleve/search/highlight"
	pb "github.com/couchbase/cbft/protobuf"
	log "github.com/couchbase/clog"
)

var DefaultStreamBatchSize = 100
var sliceStart = []byte{'['}
var sliceEnd = []byte{']'}
var itemGlue = []byte{','}

// streamHandler interface is for implementations
// which streams the document matches over streams
type streamHandler interface {
	write([]byte, []uint64, int) error
}

type streamer struct {
	index string

	m       sync.Mutex
	req     *bleve.SearchRequest
	stream  pb.SearchService_SearchServer
	sizeSet bool
	skipSet bool
	total   int

	curSkip int
	curSize int
}

func newStreamHandler(index string, req *bleve.SearchRequest,
	outStream pb.SearchService_SearchServer) *streamer {
	rv := &streamer{
		index:   index,
		curSize: int(req.Size),
		curSkip: int(req.From),
		stream:  outStream,
		req:     req,
	}
	if req.Size > 0 {
		rv.sizeSet = true
	}
	if req.From > 0 {
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
		b = append(sliceStart, b...) // TODO: reuse b slice instead of alloc?
		offsets = offsets[s.curSkip:]
		hitsCount -= s.curSkip
		// accounting for sliceStart offset
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
		return nil
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

	// TODO: perf, can hitRes be reused across stream.Send() calls?
	hitRes := &pb.StreamSearchResults{
		Contents: &pb.StreamSearchResults_Hits{
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
	bhits       []byte
	offsets     []uint64
	ctx         *search.SearchContext
	s           *streamer
	n           int
	highlighter highlight.Highlighter
	collNameMap map[uint32]string
}

func (dmh *docMatchHandler) documentMatchHandler(hit *search.DocumentMatch) error {
	if hit != nil {
		/***** remove unnecessary fields *****/
		if dmh.s.req.Highlight == nil {
			if !dmh.s.req.IncludeLocations {
				hit.Locations = nil
			}

			hit.Fragments = nil
		}

		if len(dmh.s.req.Fields) == 0 {
			hit.Fields = nil
		}

		if !dmh.s.req.Explain {
			hit.Expl = nil
		}
		/***** removed unnecessary fields *****/

		if dmh.s.req.IncludeLocations || dmh.s.req.Highlight != nil {
			hit.Complete(nil)
		}

		if len(dmh.s.req.Fields) > 0 || dmh.highlighter != nil {
			bleve.LoadAndHighlightFields(hit, dmh.s.req, "", dmh.ctx.IndexReader,
				dmh.highlighter)
		}

		// If this is a multi collection index, then strip the colelction UID
		// from the hit ID.
		if dmh.collNameMap != nil {
			idBytes := []byte(hit.ID)
			cuid := binary.LittleEndian.Uint32(idBytes[:4])
			if collName, ok := dmh.collNameMap[cuid]; ok {
				hit.ID = string(idBytes[4:])
				if hit.Fields == nil {
					hit.Fields = make(map[string]interface{})
				}
				hit.Fields["_$c"] = collName
			}
		}

		// TODO: perf, perhaps encode directly into output buffer
		// (dmh.bits?)  instead of alloc'ing memory and copying?
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
		if dmh.n < DefaultStreamBatchSize {
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
	var highlighter highlight.Highlighter
	if s.req.Highlight != nil {
		var err error
		// get the right highlighter
		highlighter, err = bleve.Config.Cache.HighlighterNamed(
			bleve.Config.DefaultHighlighter)
		if err != nil {
			return nil, false, err
		}
		if s.req.Highlight.Style != nil {
			highlighter, err = bleve.Config.Cache.HighlighterNamed(
				*s.req.Highlight.Style)
			if err != nil {
				return nil, false, err
			}
		}
		if highlighter == nil {
			return nil, false, fmt.Errorf("no highlighter named `%s` registered",
				*s.req.Highlight.Style)
		}
	}

	dmh := docMatchHandler{
		s:           s,
		ctx:         ctx,
		bhits:       make([]byte, 0, DefaultStreamBatchSize*50),
		offsets:     make([]uint64, 0, DefaultStreamBatchSize),
		highlighter: highlighter,
	}
	dmh.bhits = append(dmh.bhits, sliceStart...)

	if collNameMap, multiCollIndex :=
		metaFieldValCache.getCollUIDNameMap(s.index); multiCollIndex {
		dmh.collNameMap = collNameMap
	}

	return dmh.documentMatchHandler, true, nil
}
