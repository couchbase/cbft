//  Copyright 2026-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

#include "gpu.h"

#ifdef GPU
    int gpu_device_count(int* count) {
        int local_count = 0;
        cudaError_t err = cudaGetDeviceCount(&local_count);
        if (err != cudaSuccess) {
            return -1;
        }
        *count = local_count;
        return 0;
    }

    int gpu_get_device_name(int device_id, char* name, size_t name_len) {
        struct cudaDeviceProp prop;
        cudaError_t err = cudaGetDeviceProperties(&prop, device_id);
        if (err != cudaSuccess) {
            return -1;
        }
        strncpy(name, prop.name, name_len - 1);
        name[name_len - 1] = '\0';
        return 0;
    }

    int gpu_last_error(char* buf, size_t buf_len) {
        cudaError_t err = cudaGetLastError();
        if (err == cudaSuccess) {
            return -1;
        }
        strncpy(buf, cudaGetErrorString(err), buf_len - 1);
        buf[buf_len - 1] = '\0';
        return 0;
    }

    int gpu_get_device_memory(int device_id, size_t* total_memory, size_t* free_memory) {
        int original_device = 0;
        cudaError_t err = cudaGetDevice(&original_device);
        if (err != cudaSuccess) {
            return -1;
        }
        err = cudaSetDevice(device_id);
        if (err != cudaSuccess) {
            return -1;
        }
        err = cudaMemGetInfo(free_memory, total_memory);
        cudaError_t restore_err = cudaSetDevice(original_device);
        if (err != cudaSuccess || restore_err != cudaSuccess) {
            return -1;
        }
        return 0;
    }
#else
    int gpu_device_count(int* count) {
        return -1;
    }

    int gpu_get_device_name(int device_id, char* name, size_t name_len) {
        return -1;
    }

    int gpu_last_error(char* buf, size_t buf_len) {
        return -1;
    }

    int gpu_get_device_memory(int device_id, size_t* total_memory, size_t* free_memory) {
        return -1;
    }
#endif // GPU
