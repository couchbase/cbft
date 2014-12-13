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
	"os"
)

type FileService struct {
	reqs chan bool
}

func NewFileService(concurrency int) *FileService {
	return &FileService{make(chan bool, concurrency)}
}

// Open a FileLike thing that works within this FileService.
func (fs *FileService) OpenFile(path string, mode int) (FileLike, error) {
	rv := &fileLike{fs, path, mode}
	err := fs.Do(rv.path, rv.mode, func(*os.File) error { return nil })
	// Clear out bits that only make sense the first time
	// you open something.
	rv.mode = rv.mode &^ (os.O_EXCL | os.O_APPEND | os.O_TRUNC)
	return rv, err
}

func (f *FileService) Do(path string, flags int, fn func(*os.File) error) error {
	f.reqs <- true
	defer func() { <-f.reqs }()
	file, err := os.OpenFile(path, flags, 0666)
	if err != nil {
		return err
	}
	defer file.Close()
	return fn(file)
}

func (f *FileService) Close() error {
	close(f.reqs)
	return nil
}
