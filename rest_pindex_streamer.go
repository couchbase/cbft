//  Copyright 2021-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package cbft

import (
	"archive/tar"
	"context"
	"encoding/hex"
	"fmt"
	"hash/crc32"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	log "github.com/couchbase/clog"

	"github.com/blevesearch/bleve/v2"
	"github.com/couchbase/cbgt"
	"github.com/couchbase/cbgt/rest"
)

// ---------------------------------------------------------------------

var crcTable = crc32.MakeTable(crc32.Castagnoli)

// PIndexContentHandler is a REST handler for retrieving the archived,
// optionally compressed PIndex contents
type PIndexContentHandler struct {
	mgr *cbgt.Manager
}

// PIndexContentRequest represent the PIndex content request
type PIndexContentRequest struct {
	Name    string `json:"name"`
	Version string `json:"version,omitempty"`
}

// NewPIndexContentHandler returns a PIndexContentHandler
func NewPIndexContentHandler(mgr *cbgt.Manager) *PIndexContentHandler {
	return &PIndexContentHandler{mgr: mgr}
}

// contentStreamer implements the io.WriteCloser interface.
type contentStreamer struct {
	ctx context.Context
	mw  io.Writer
	tw  *tar.Writer
	buf []byte
}

func (w *contentStreamer) Write(src []byte) (written int, err error) {
	return w.mw.Write(src)
}

func (fs *contentStreamer) Close() error {
	return nil
}

func (w *contentStreamer) writeFromReader(src io.Reader) (
	written int, err error) {
	for {
		if isCanceled(w.ctx) {
			return written, w.ctx.Err()
		}
		nr, er := io.ReadFull(src, w.buf)
		if nr > 0 {
			nw, ew := w.mw.Write(w.buf[0:nr])
			if nw > 0 {
				written += nw
			}
			if ew != nil {
				err = ew
				break
			}
			if nr != nw {
				err = io.ErrShortWrite
				break
			}
		}
		if er != nil {
			if er != io.EOF && er != io.ErrUnexpectedEOF {
				err = er
			}
			break
		}
	}

	return written, err
}

func (h *PIndexContentHandler) ServeHTTP(
	w http.ResponseWriter, req *http.Request) {
	piName := rest.PIndexNameLookup(req)
	if piName == "" {
		rest.ShowError(w, req, "rest_pindex_streamer: pindex"+
			" name is required", http.StatusBadRequest)
		return
	}

	h.streamPIndexContents(piName, w, req)
}

func (h *PIndexContentHandler) streamPIndexContents(pindexName string,
	w http.ResponseWriter, req *http.Request) {
	pindexPath := h.mgr.PIndexPath(pindexName)
	_, err := ioutil.ReadDir(pindexPath)
	if err != nil {
		rest.ShowError(w, req, fmt.Sprintf("rest_pindex_streamer:"+
			" read failed for path: %s, err: %v", pindexPath, err),
			http.StatusInternalServerError)
		return
	}

	size, err := cbgt.GetDirectorySize(pindexPath)
	if err != nil {
		rest.ShowError(w, req, fmt.Sprintf("rest_pindex_streamer:"+
			" GetDirectorySize failed for path: %s, err: %v",
			pindexPath, err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Size", strconv.Itoa(int(size)))
	w.Header().Set("Content-Type", "application/x-tar")
	w.Header().Set("Trailer", "Checksum")

	h.streamTarArchive(pindexName, w, req)
}

func (h *PIndexContentHandler) streamTarArchive(pindexName string,
	w http.ResponseWriter, req *http.Request) {
	rootPath := h.mgr.PIndexPath(pindexName)
	// temp dir for storing all memory segments and root bolt file.
	tempPath, err := ioutil.TempDir(filepath.Dir(rootPath), "temp$$")
	if err != nil {
		rest.ShowError(w, req, fmt.Sprintf("rest_pindex_streamer:"+
			" dir err: %v, for path: %s", err, tempPath),
			http.StatusInternalServerError)
		return
	}

	log.Printf("rest_pindex_streamer: streamTarArchive"+
		" started %s", pindexName)

	// cleanup the tempPath
	defer cleanup(tempPath)

	tw := tar.NewWriter(w)
	defer tw.Close()

	// hash and archive together
	hash := crc32.New(crcTable)
	mw := io.MultiWriter(tw, hash)
	ctx := req.Context()
	start := time.Now()

	cs := &contentStreamer{
		mw:  mw,
		tw:  tw,
		buf: make([]byte, 1*1024*1024), // 1MB arbitrary size
		ctx: ctx,
	}

	ic, err := getCopyableBleveIndex(pindexName, h.mgr)
	if err != nil {
		rest.ShowError(w, req, fmt.Sprintf("rest_pindex_streamer:"+
			" index reader err: %v", err), http.StatusInternalServerError)
		return
	}

	err = ic.CopyTo(&archiveDirectory{pindexPath: rootPath,
		tempPath: tempPath, cs: cs})
	if err != nil {
		rest.ShowError(w, req, fmt.Sprintf("rest_pindex_streamer:"+
			" WriteTo err: %v", err), http.StatusInternalServerError)
		return
	}

	// transfer the files created in the temp location,
	// eg: the in-memory segments, bolt and bleve meta files.
	for path, suffixes := range map[string][]string{
		rootPath: {"PINDEX_BLEVE_META", "PINDEX_META"},
		tempPath: {".bolt", ".zap", ".json"}} {
		err = cs.walkDirAndStreamFiles(path, suffixes)
		if err != nil {
			rest.ShowError(w, req, fmt.Sprintf("rest_pindex_streamer:"+
				" stream err: %v", err), http.StatusInternalServerError)
			return
		}
	}

	checkSum := hex.EncodeToString(hash.Sum(nil))
	w.Header().Set("Checksum", checkSum)

	log.Printf("rest_pindex_streamer, partition: %s,"+
		" transfer took: %s", pindexName, time.Since(start).String())
}

func (fs *contentStreamer) walkDirAndStreamFiles(path string,
	nameSuffixes []string) error {
	return filepath.Walk(path, func(file string,
		fi os.FileInfo, err error) error {
		if isCanceled(fs.ctx) {
			log.Printf("rest_pindex_streamer: canceled,"+
				" err: %v", fs.ctx.Err())
			return fs.ctx.Err()
		}

		// return on any error
		if err != nil {
			return err
		}

		if fi.Mode().IsDir() {
			return nil
		}

		// look for name relevant file suffixes.
		var found bool
		for _, s := range nameSuffixes {
			if strings.HasSuffix(fi.Name(), s) {
				found = true
				break
			}
		}
		if !found {
			return nil
		}

		// create a new dir/file header
		header, err := tar.FileInfoHeader(fi, fi.Name())
		if err != nil {
			return err
		}
		header.Name = filepath.Base(file)
		if err := fs.tw.WriteHeader(header); err != nil {
			return err
		}

		// open files for taring
		f, err := os.OpenFile(file, os.O_RDONLY, 0666)
		if err != nil {
			return err
		}
		defer f.Close()

		// copy file data into tar writer
		if _, err = fs.writeFromReader(f); err != nil {
			log.Printf("rest_pindex_streamer: file: %s,"+
				" write err: %v", fi.Name(), err)
			return err
		}

		log.Printf("rest_pindex_streamer: writeToStream finished"+
			" file: %s", fi.Name())
		return nil
	})
}

type archiveDirectory struct {
	pindexPath string
	tempPath   string
	cs         *contentStreamer
}

// GetWriter is an implementation of the index.Directory interface.
func (d *archiveDirectory) GetWriter(filePath string) (
	io.WriteCloser, error) {
	// check whether the file already exists. If file does not exists,
	// then it could be either a memory segment or index meta files.
	// Persist them in a temp location as direct socket streaming won't
	// be possible here since we don't have enough info(FileInfo) on the
	// eventual file on disk, esp size.
	fi, err := os.Stat(filepath.Join(d.pindexPath, filePath))
	if err != nil && os.IsNotExist(err) ||
		strings.HasSuffix(filePath, "root.bolt") ||
		strings.HasSuffix(filePath, ".json") {
		f, err := os.OpenFile(filepath.Join(d.tempPath,
			filepath.Base(filePath)),
			os.O_RDWR|os.O_CREATE, 0600)
		if err != nil {
			log.Printf("rest_pindex_streamer: file create"+
				" err: %v, %s", err, filePath)
			return nil, err
		}

		return f, nil
	}

	// stream the persisted files directly.
	header, err := tar.FileInfoHeader(fi, fi.Name())
	if err != nil {
		log.Printf("rest_pindex_streamer: tar.FileInfoHeader,"+
			" err: %v", err)
		return nil, err
	}

	header.Name = filepath.Base(filePath)

	if err := d.cs.tw.WriteHeader(header); err != nil {
		log.Printf("rest_pindex_streamer: tw.WriteHeader,"+
			" err: %v", err)
		return nil, err
	}

	return d.cs, nil
}

func getCopyableBleveIndex(pindexName string, mgr *cbgt.Manager) (
	bleve.IndexCopyable, error) {
	pi := mgr.GetPIndex(pindexName)
	if pi == nil {
		return nil,
			fmt.Errorf("rest_pindex_streamer: no pindex found")
	}

	bindex, _, _, err := bleveIndex(pi)
	if err != nil {
		return nil,
			fmt.Errorf("rest_pindex_streamer: pindex fetch err")
	}

	if ib, ok := bindex.(bleve.IndexCopyable); ok {
		return ib, nil
	}

	return nil, fmt.Errorf("rest_pindex_streamer: bleve.Index" +
		" is not copyable")
}

func isCanceled(ctx context.Context) bool {
	select {
	case <-ctx.Done():
		return true
	default:
		return false
	}
}

func cleanup(path string) {
	err := os.RemoveAll(path)
	if err != nil {
		log.Printf("rest_pindex_streamer: os.RemoveAll, err: %v", err)
	}
}
