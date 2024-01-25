//  Copyright 2024-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

#if defined(__linux__) || defined(__linux) || defined(linux) || defined(__gnu_linux__)
    #include <cstdio>
    #include <cstdlib>
    #include <cstring>
    #include "c_heap_mem_usage.h"
    #include <malloc.h>

size_t get_attribute_value(const char *str, const char *substr) {
    const char *pos = strstr(str, substr);
    if (pos == NULL) {
        return 0;
    }
    pos += strlen(substr);

    char* next_pos = NULL;
    size_t value = strtoull(pos, &next_pos, 10);
    if (next_pos == NULL || next_pos == pos) {
        return 0;
    }

    return value;
}


size_t get_attribute_value(const char *str, const char *first_substr, const char* second_substr) {
    const char *pos = strstr(str, first_substr);
    if (pos == NULL) {
        return 0;
    }
    pos += strlen(first_substr);
    pos = strstr(pos, second_substr);
    if (pos == NULL) {
        return 0;
    }
    pos += strlen(second_substr);

    char* next_pos = NULL;
    size_t value = strtoull(pos, &next_pos, 10);
    if (next_pos == NULL || next_pos == pos) {
        return 0;
    }

    return value;
}


size_t get_total_heap_bytes(){
    // ref: https://gist.github.com/tadeu/95013963c64da4cd74a2c6f4fa4fd553
    // There are three functions in Linux libc API to retrieve heap
    // information: `malloc_stats`, `mallinfo` and `malloc_info`.
    // The first two are still broken for 64-bit systems and will
    // report wrong values if there are more than 4 Gb of allocated
    // memory. The latter works for more than 4 Gb, but it outputs
    // a XML to a file, so `open_memstream` is used to avoid writing
    // to the disk, and a very simple "parsing" is done here using
    // C-string search.
    //
    // More info:
    //   https://stackoverflow.com/questions/40878169/64-bit-capable-alternative-to-mallinfo
    //   https://stackoverflow.com/questions/3903807/how-does-malloc-info-work
    //   https://stackoverflow.com/questions/34292457/gnu-malloc-info-get-really-allocated-memory

    char* buf = NULL;
    size_t buf_size = 0;

    FILE* f = open_memstream(&buf, &buf_size);
    // this doesn't include the golang or the other process's stats
    // as per local testing
    malloc_info(0, f);
    fclose(f);

    // We are only interested in totals, so we skip everything until the
    // closing of the <heap>...</heap> block.
    const char* pos = strstr(buf, "</heap>");

    // rest and fast are blocks that have been freed, we should subtract them
    size_t rest = get_attribute_value(pos, "<total type=\"rest\" count=\"", "\" size=\"");
    size_t fast = get_attribute_value(pos, "<total type=\"fast\" count=\"", "\" size=\"");

    // mmap and current are totals (mmap is used for very large blocks)
    size_t mmap = get_attribute_value(pos, "<total type=\"mmap\" count=\"", "\" size=\"");
    size_t current = get_attribute_value(pos, "<system type=\"current\" size=\"");

    size_t free_mem = rest + fast;
    size_t total_mem = mmap + current;

    size_t allocated_mem = total_mem > free_mem ? total_mem - free_mem : 0;

    free(buf);
    buf = NULL;
    return allocated_mem;
}
#endif
