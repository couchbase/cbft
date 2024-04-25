//  Copyright 2024-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

#include "malloc.h"

#ifdef JEMALLOC
    // Underlying system allocator is jemalloc
    // -------------------------------------------------------------------------
    // Set tuning parameters for jemalloc used by cbft
    // -------------------------------------------------------------------------
    // JEMALLOC VERSION: 5.2.1
    // -------------------------------------------------------------------------
    const char* malloc_conf=
    #ifndef __APPLE__
        /* Enable background worker thread for asynchronous purging.
        * Background threads are non-functional in jemalloc 5.1.0 on macOS due to
        * implementation discrepancies between the background threads and mutexes.
        * https://github.com/jemalloc/jemalloc/issues/1433
        */
        "background_thread:true,"
    #endif
        /* Use 4 arenas, instead of the default based on number of CPUs.
        Helps to minimize heap fragmentation.
        https://github.com/jemalloc/jemalloc/blob/dev/TUNING.md
        */
        "narenas:4,"
    #ifdef __linux__
        /*
         * Start with profiling enabled but inactive; this allows us to
           turn it on/off at runtime.
         * Profiling adds an overhead which can impact performance, hence 
           it must be ensured that profiling be activated for debugging
           scenarios only. 
         * Set prof_accum as true to get a cumulative profile
           similar to Golang.
        */
        "prof:true,prof_active:false,prof_accum:true,"
    #endif
        /* abort immediately on illegal options, just for sanity */
        "abort_conf:true";
    // -------------------------------------------------------------------------
    // Use mallctl in jemalloc.h for getting memory stats and profiles
    // -------------------------------------------------------------------------
    // Helper function for calling jemalloc epoch.
    // jemalloc stats are not updated until the caller requests a synchronisation,
    // which is done by the epoch call.
    static void callJemallocEpoch() {
        size_t epoch = 1;
        size_t sz = sizeof(epoch);
        // The return of epoch is the current epoch, which we don't make use of
        mallctl("epoch", &epoch, &sz, &epoch, sz);
    }
    // returns the number of bytes allocated in jemalloc
    size_t mm_allocated() {
        // call mallctl defined in jemalloc
        // first refresh the value of the stats
        callJemallocEpoch();
        // now get the stats
        size_t allocated, sz;
        sz = sizeof(size_t);
        mallctl("stats.allocated", &allocated, &sz, NULL, 0);
        return allocated;
    }
    // writecb is callback passed to jemalloc used to process a chunk of
    // stats text. It is in charge of making sure that the buffer is
    // sufficiently sized.
    void writecb(void *ref, const char *s) {
        stats_buf *buf = (stats_buf *)(ref);
        int len;
        len = strlen(s);
        if (buf->offset + len >= buf->size) {
            // Buffer is too small, resize it to fit at least len and string terminator
            buf->size += len + 2;
            buf->buf = realloc(buf->buf, buf->size);
        }
        strncpy(buf->buf + buf->offset, s, len);
        buf->offset += len;
    }
    // doStats returns a string with jemalloc stats.
    // Caller is responsible to call free on the string buffer.
    char *doStats(char *opts)  {
        stats_buf buf;
        buf.size = 1024;
        buf.buf = malloc(buf.size);
        buf.offset = 0;
        malloc_stats_print(writecb, &buf, opts);
        buf.buf[buf.offset] = 0;
        return buf.buf;
    }
#else
    // Underlying system allocator is not jemalloc and is glibc's malloc
    #if defined(__linux__) || defined(__linux) || defined(linux) || defined(__gnu_linux__)
        #include <malloc.h>
        size_t get_attribute_value_current(const char *str, const char *substr) {
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

        size_t mm_allocated() {
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
            if (f == NULL) {
            return (size_t)0;
            }

            // this doesn't include the golang or the other process's stats
            // as per local testing
            int rv = malloc_info(0, f);
            fclose(f);

            // https://man7.org/linux/man-pages/man3/malloc_info.3.html
            if ((rv != 0) || (buf == NULL)) {
                return (size_t)0;
            }

            // We are only interested in totals, so we skip everything until the
            // closing of the <heap>...</heap> block.
            const char* pos = strstr(buf, "</heap>");

            // rest and fast are blocks that have been freed, we should subtract them
            size_t rest = get_attribute_value(pos, "<total type=\"rest\" count=\"", "\" size=\"");
            size_t fast = get_attribute_value(pos, "<total type=\"fast\" count=\"", "\" size=\"");

            // mmap and current are totals (mmap is used for very large blocks)
            size_t mmap = get_attribute_value(pos, "<total type=\"mmap\" count=\"", "\" size=\"");
            size_t current = get_attribute_value_current(pos, "<system type=\"current\" size=\"");

            size_t free_mem = rest + fast;
            size_t total_mem = mmap + current;

            size_t allocated_mem = total_mem > free_mem ? total_mem - free_mem : 0;

            free(buf);
            buf = NULL;

            return allocated_mem;
        }
    #elif defined(__APPLE__) && defined(__MACH__)
        #include <malloc/malloc.h>
        size_t mm_allocated() {
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
    #else // Unsupported platform
        size_t mm_allocated() {
            return (size_t)0L;
        }
    #endif
#endif

char *mm_stats_json() {
#ifdef JEMALLOC
    return doStats("J");
#else
    return NULL;
#endif
}

char *mm_stats_text() {
#ifdef JEMALLOC
    return doStats(NULL);
#else
    return NULL;
#endif
}

// jemalloc profiling APIs only supported on linux  as of jemalloc version 5.2.1 
int mm_prof_activate() {
#if defined(JEMALLOC) && defined(__linux__)
    bool active = true;
    return mallctl("prof.active", NULL, NULL, &active, sizeof(active));
#endif
    return ENOTSUP;
}

int mm_prof_deactivate() {
#if defined(JEMALLOC) && defined(__linux__)
    bool active = false;
    return mallctl("prof.active", NULL, NULL, &active, sizeof(active));
#endif
    return ENOTSUP;
}

int mm_prof_dump(char* filePath) {
#if defined(JEMALLOC) && defined(__linux__)
    return mallctl("prof.dump", NULL, NULL, &filePath, sizeof(const char *));
#endif
    return ENOTSUP;
}
