//  Copyright 2024-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

#if defined(__APPLE__) && defined(__MACH__)
    #include <malloc/malloc.h>
    #include <cstdio>
    #include <cstdlib>
    #include <cstring>
    #include "c_heap_mem_usage.h"

size_t get_total_heap_bytes() {
    // mstats on mac platform gives a copy of the struct which has information
    // like what's the bytes being used currently on the heap (the allocated bytes)
    // and also other information such as what's the total bytes that's been
    // allocated and the free bytes. this API call fetches the information by
    // talking to the memory manager which is responsible for tracking these
    // stats of malloc (which is what ultimately all dynamic datastructures call
    // underneath the hood for memory)
    // this doesn't include the golang or the other process's stats as per local testing
    //
    // https://opensource.apple.com/source/Libc/Libc-825.26/include/malloc/malloc.h.auto.html
    struct mstats ms = mstats();
    return ms.bytes_used;
}
#endif
