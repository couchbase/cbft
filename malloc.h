//  Copyright 2024-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

#ifndef MALLOC_H
#define MALLOC_H

#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#ifdef JEMALLOC
    /* Include jemalloc.h for jemalloc specific functions. */
    #include <jemalloc/jemalloc.h>
    /* jemalloc checks for this symbol, and it's contents for the config to use. */
    extern const char* malloc_conf;
#endif

typedef struct {
	char *buf;
	int offset;
	int size;
} stats_buf;

// ----------------------------------------------------------------------------------------------
// Set API wrappers for statistic collection and profiling from the underlying C memory allocator
// ----------------------------------------------------------------------------------------------
#ifdef __cplusplus
extern "C" {
#endif
    // number of bytes allocated in malloc - num_bytes_used_ram_c
    size_t mm_allocated();
    // if malloc implementation is jemalloc, mm_stats_json and mm_stats_text
    // return jemalloc stats by calling the malloc_stats_print API, 
    // otherwise they return NULL
    // mm_stats_json - returns jemalloc stats in json format
    char* mm_stats_json();
    // mm_stats_text - returns jemalloc stats in text format
    char* mm_stats_text();

#ifdef __cplusplus
}
#endif

#endif // MALLOC_H
