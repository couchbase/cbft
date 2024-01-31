//  Copyright 2024-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

#ifdef __cplusplus
extern "C" {
#endif
    // the first block is just to declare the heap stats function for all the
    // supported OS, whereas in the else block we have a no-op.
    #if (defined(__APPLE__) && defined(__MACH__)) || \
    (defined(__linux__) || defined(__linux) || defined(linux) || defined(__gnu_linux__))
        size_t get_total_heap_bytes();
    #else
        size_t get_total_heap_bytes() {
            return(size_t)0L;
        }
    #endif

#ifdef __cplusplus
}
#endif
