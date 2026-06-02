//  Copyright 2026-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

//go:build gpu
// +build gpu

package cbft

/*
#cgo CFLAGS: -DGPU=1
#cgo LDFLAGS: -lcudart

#include "gpu.h"
*/
import "C"
import (
	"errors"
	"unsafe"

	log "github.com/couchbase/clog"
)

const (
	// stringBufferSize is the size of the buffer used to hold C strings returned by the GPU APIs.
	// if the C string is larger than this size, it will be truncated.
	stringBufferSize = 256
)

func LogGPUState() {
	count, err := getGPUCount()
	if err != nil {
		log.Printf("GPU: error getting GPU device count: %v", err)
		return
	}
	log.Printf("GPU: %d GPU device(s) detected", count)
	for i := 0; i < count; i++ {
		name, err := getGPUName(i)
		if err != nil {
			log.Printf("GPU: error getting name for GPU device %d: %v", i, err)
			continue
		}
		freeMem, totalMem, err := getGPUMemoryInfo(i)
		if err != nil {
			log.Printf("GPU: error getting memory info for GPU device %d: %v", i, err)
			continue
		}
		log.Printf("GPU: device %d: name=%s, totalMem=%d bytes, freeMem=%d bytes",
			i, name, totalMem, freeMem)
	}
}

func getGPUCount() (int, error) {
	var count C.int
	if c := C.gpu_device_count(&count); c != 0 {
		return 0, getLastError()
	}
	return int(count), nil
}

func getGPUName(deviceID int) (string, error) {
	buf := make([]byte, stringBufferSize)
	if c := C.gpu_get_device_name(
		C.int(deviceID),
		(*C.char)(unsafe.Pointer(&buf[0])),
		C.size_t(len(buf)),
	); c != 0 {
		return "", getLastError()
	}
	return bufferToString(buf), nil
}

func getLastError() error {
	buf := make([]byte, stringBufferSize)
	C.gpu_last_error(
		(*C.char)(unsafe.Pointer(&buf[0])),
		C.size_t(len(buf)),
	)
	return errors.New(bufferToString(buf))
}

func getGPUMemoryInfo(deviceID int) (uint64, uint64, error) {
	var freeMem, totalMem C.size_t
	if c := C.gpu_get_device_memory(
		C.int(deviceID),
		&totalMem,
		&freeMem,
	); c != 0 {
		return 0, 0, getLastError()
	}
	return uint64(freeMem), uint64(totalMem), nil
}

func bufferToString(buf []byte) string {
	n := 0
	for n < len(buf) && buf[n] != 0 {
		n++
	}
	return string(buf[:n])
}
