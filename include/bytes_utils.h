//
// Created by forked_franz on 04/02/17.
//

#ifndef FRANZ_FLOW_BYTES_UTILS_H
#define FRANZ_FLOW_BYTES_UTILS_H

#include <stdbool.h>
#include <stddef.h>
#include "index.h"

#define CACHE_LINE_LENGTH 64

inline static bool is_pow_2(const index_t value) {
    return value > 0 && ((value & (~value + 1)) == value);
}

inline static index_t align(const index_t value, const index_t pow_2_alignment) {
    return (value + (pow_2_alignment - 1)) & ~(pow_2_alignment - 1);
}

inline static index_t next_pow_2(const index_t value) {
    index_t v = value;
    v--;
    v |= v >> 1;
    v |= v >> 2;
    v |= v >> 4;
    v |= v >> 8;
    v |= v >> 16;
    v++;
    return v;
}

#endif //FRANZ_FLOW_BYTES_UTILS_H
