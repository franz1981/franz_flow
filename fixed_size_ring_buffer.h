//
// Created by forked_franz on 10/02/17.
//

#ifndef FRANZ_FLOW_FIXED_SIZE_RING_BUFFER_H
#define FRANZ_FLOW_FIXED_SIZE_RING_BUFFER_H

#include <stdbool.h>
#include <stdio.h>
#include "index.h"

struct fixed_size_ring_buffer_header {
    uint8_t *producer_position;
    uint8_t *consumer_cache_position;
    uint8_t *consumer_position;
    index_t mask;
    index_t capacity;
    uint32_t aligned_message_size;
};

static inline index_t fixed_size_ring_buffer_capacity(const index_t requested_capacity, const uint32_t message_size);

static inline bool
init_fixed_size_ring_buffer_header(uint8_t *const buffer, struct fixed_size_ring_buffer_header *const header,
                                   const index_t requested_capacity,
                                   const uint32_t message_size);

static inline bool
try_fixed_size_ring_buffer_lookahead_claim(uint8_t *const buffer, const struct fixed_size_ring_buffer_header *const header,
                                           const uint32_t max_look_ahead_step,
                                           uint8_t **const claimed_message);

static inline bool
try_fixed_size_ring_buffer_claim(uint8_t *const buffer, const struct fixed_size_ring_buffer_header *const header,
                                 uint8_t **const claimed_message);

static inline void fixed_size_ring_buffer_commit_claim(const uint8_t *const claimed_message_address);

static inline bool try_fixed_size_ring_buffer_read(uint8_t *const buffer, const struct fixed_size_ring_buffer_header *const header,
                                                   uint8_t **const read_message_address);

static inline void fixed_size_ring_buffer_commit_read(const uint8_t *const read_message_address);

typedef bool(*const fixed_size_message_consumer)(uint8_t *const, void *const);

inline static uint32_t fixed_size_ring_buffer_batch_read(
        uint8_t *const buffer,
        const struct fixed_size_ring_buffer_header *const header,
        const fixed_size_message_consumer consumer,
        const uint32_t count, void *const context);

inline static uint32_t fixed_size_ring_buffer_stream_batch_read(
        uint8_t *const buffer,
        const struct fixed_size_ring_buffer_header *const header,
        const fixed_size_message_consumer consumer,
        const uint32_t count, void *const context);

static inline index_t fixed_size_ring_buffer_size(const struct fixed_size_ring_buffer_header *const header);

#endif //FRANZ_FLOW_FIXED_SIZE_RING_BUFFER_H
