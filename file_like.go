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
	"errors"
	"io"
	"os"
)

var FileNotReadable = errors.New("file is not open for reading")
var FileNotWritable = errors.New("file is not open for writing")

// A FileLike looks like a File, but file opening/closing is delayed
// until the actual read/write/etc operation.  See also FileService.
type FileLike interface {
	io.Closer
	io.ReaderAt
	io.WriterAt
	Stat() (os.FileInfo, error)
	Truncate(size int64) error
}

type fileLike struct {
	fs   *FileService
	path string
	mode int
}

func (f *fileLike) Close() error {
	return nil
}

// Stat the underlying path.
func (f *fileLike) Stat() (os.FileInfo, error) {
	return os.Lstat(f.path)
}

func (f *fileLike) ReadAt(p []byte, off int64) (n int, err error) {
	if f.mode&os.O_WRONLY == os.O_WRONLY {
		return 0, FileNotReadable
	}
	err = f.fs.Do(f.path, f.mode, func(file *os.File) error {
		n, err = file.ReadAt(p, off)
		return err
	})
	return
}

func (f *fileLike) WriteAt(p []byte, off int64) (n int, err error) {
	if f.mode&(os.O_WRONLY|os.O_RDWR) == 0 {
		return 0, FileNotWritable
	}
	err = f.fs.Do(f.path, f.mode, func(file *os.File) error {
		n, err = file.WriteAt(p, off)
		return err
	})
	return
}

func (f *fileLike) Truncate(size int64) (err error) {
	if f.mode&(os.O_WRONLY|os.O_RDWR) == 0 {
		return FileNotWritable
	}
	err = f.fs.Do(f.path, f.mode, func(file *os.File) error {
		return file.Truncate(size)
	})
	return
}
