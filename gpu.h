//  Copyright 2026-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

#ifndef GPU_H
#define GPU_H

#include <stddef.h>
#include <stdio.h>
#include <string.h>

#ifdef GPU
    #include <cuda_runtime.h>
#endif

// All the C APIs in this file return 0 on success and -1 on failure.
// On failure, gpu_last_error can be called to retrieve a human-readable error
// message describing the failure. On success, gpu_last_error returns -1.
// All C APIs accept output parameters as pointers, and write results to
// those parameters on success.
#ifdef __cplusplus
extern "C" {
#endif
    // Returns the number of available GPU devices
    int gpu_device_count(int* count);

    // Retrieves the name of the specified GPU device
    int gpu_get_device_name(int device_id, char* name, size_t name_len);

    // Retrieves the last error message from GPU operations
    int gpu_last_error(char* buf, size_t buf_len);

    // Retrieves the total and free memory of the specified GPU device
    int gpu_get_device_memory(int device_id, size_t* total_memory, size_t* free_memory);

#ifdef __cplusplus
}
#endif

#endif // GPU_H
