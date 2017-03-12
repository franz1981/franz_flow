//
// Created by forked_franz on 18/02/17.
//

#ifndef FRANZ_FLOW_FIXED_SIZE_STREAM_H
#define FRANZ_FLOW_FIXED_SIZE_STREAM_H

#include <stdbool.h>
#include <stdint.h>

struct fs_stream_t {
    uint8_t *buffer;
    _Atomic uint64_t *producers_cycle_claim;
    _Atomic uint32_t *active_cycle_index;
    _Atomic uint64_t *consumer_cache_position;
    _Atomic uint64_t *consumer_position;
    uint32_t capacity;
    uint32_t mask;
    uint32_t max_gain;
    uint32_t mask_cycle_length;
    uint32_t cycle_length;
    uint32_t mask_cycles;
    uint32_t cycles;
    uint32_t aligned_message_size;
};

static inline bool new_fs_stream(
        uint8_t *const buffer,
        struct fs_stream_t *const stream,
        const uint32_t requested_capacity,
        const uint32_t message_size,
        const uint32_t cycles);

static inline uint32_t
fs_stream_capacity(const uint32_t requested_capacity, const uint32_t message_size, const uint32_t cycles);

static inline bool fs_stream_try_claim(
        const struct fs_stream_t *const stream,
        uint8_t **const claimed_message);

static inline void fs_stream_commit_claim(const uint8_t *const claimed_message_address);

static inline uint64_t fs_stream_load_producer_position(const struct fs_stream_t *const stream);

static inline uint64_t fs_stream_load_consumer_position(const struct fs_stream_t *const stream);

static inline uint32_t fs_stream_size(const struct fs_stream_t *const stream);

typedef bool(*const fs_stream_message_consumer)(uint8_t *const, void *const);

inline static uint32_t fs_stream_read(
        const struct fs_stream_t *const stream,
        const fs_stream_message_consumer consumer,
        const uint32_t count, void *const context

);

#endif //FRANZ_FLOW_FIXED_SIZE_STREAM_H
