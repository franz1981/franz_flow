//
// Created by forked_franz on 10/02/17.
//

#include <stdatomic.h>
#include "fixed_size_ring_buffer.h"
#include "bytes_utils.h"

#define MESSAGE_STATE_SIZE 4

static const index_t MESSAGE_STATE_FREE = 0;
static const index_t MESSAGE_STATE_BUSY = 1;
static const index_t PRODUCER_POSITION_OFFSET = CACHE_LINE_LENGTH * 2;
static const index_t CONSUMER_CACHE_POSITION_OFFSET = CACHE_LINE_LENGTH * 4;
static const index_t CONSUMER_POSITION_OFFSET = CACHE_LINE_LENGTH * 6;
static const index_t TRAILER_LENGTH = CACHE_LINE_LENGTH * 8;

static inline index_t fixed_size_ring_buffer_capacity(const index_t requested_capacity, const uint32_t message_size) {
    const index_t next_pow_2_requested_capacity = next_pow_2(requested_capacity);
    const index_t aligned_message_size = align(message_size + MESSAGE_STATE_SIZE, MESSAGE_STATE_SIZE);
    return (next_pow_2_requested_capacity * aligned_message_size) + TRAILER_LENGTH;
}


static inline bool
init_fixed_size_ring_buffer_header(uint8_t *const buffer, struct fixed_size_ring_buffer_header *const header,
                                   const index_t requested_capacity,
                                   const uint32_t message_size) {
    const index_t next_pow_2_requested_capacity = next_pow_2(requested_capacity);
    const index_t aligned_message_size = align(message_size + MESSAGE_STATE_SIZE, MESSAGE_STATE_SIZE);
    const index_t capacity_bytes = (next_pow_2_requested_capacity * aligned_message_size);
    header->capacity = next_pow_2_requested_capacity;
    header->mask = next_pow_2_requested_capacity - 1;
    header->aligned_message_size = aligned_message_size;
    header->producer_position = buffer + capacity_bytes + PRODUCER_POSITION_OFFSET;
    header->consumer_cache_position = buffer + capacity_bytes + CONSUMER_CACHE_POSITION_OFFSET;
    header->consumer_position = buffer + capacity_bytes + CONSUMER_POSITION_OFFSET;
    return true;
}

static inline bool claim_slow_path(uint8_t *const buffer, const index_t message_state_offset,
                                   uint64_t *const consumer_cache_position_address,
                                   const uint64_t consumer_cache_position, const uint32_t max_look_ahead_step,
                                   const index_t mask, const index_t aligned_message_size) {
    //try to look ahead if the consumer has freed MAX_LOOK_AHEAD_STEP messages
    const uint64_t next_consumer_cache_position = consumer_cache_position + max_look_ahead_step;
    //check the state of the message
    const index_t look_ahead_message_offset = (next_consumer_cache_position & mask) * aligned_message_size;
    const _Atomic uint32_t *const look_ahead_message_state_atomic_address = (_Atomic uint32_t *) (buffer +
                                                                                                  look_ahead_message_offset);
    const uint32_t message_state_value = atomic_load_explicit(look_ahead_message_state_atomic_address,
                                                              memory_order_relaxed);
    if (message_state_value == MESSAGE_STATE_FREE) {
        atomic_thread_fence(memory_order_acquire);
        //can consume
        *consumer_cache_position_address = next_consumer_cache_position;
        return true;
    } else {
        //fallback case: try the current claimed message
        const _Atomic uint32_t *const claimed_message_state_atomic_address = (_Atomic uint32_t *) (buffer +
                                                                                                   message_state_offset);
        const uint32_t claimed_message_state_value = atomic_load_explicit(claimed_message_state_atomic_address,
                                                                          memory_order_relaxed);
        if (claimed_message_state_value != MESSAGE_STATE_FREE) {
            return false;
        }
        atomic_thread_fence(memory_order_acquire);
        return true;
    }
}

static inline bool
try_fixed_size_ring_buffer_lookahead_claim(uint8_t *const buffer,
                                           const struct fixed_size_ring_buffer_header *const header,
                                           const uint32_t max_look_ahead_step,
                                           uint8_t **const claimed_message) {
    const _Atomic uint64_t *const producer_position_address = (_Atomic uint64_t *) header->producer_position;
    uint64_t *const consumer_cache_position_address = (uint64_t *) header->consumer_cache_position;
    const index_t mask = header->mask;
    const index_t aligned_message_size = header->aligned_message_size;
    const uint64_t consumer_cache_position = *consumer_cache_position_address;
    const uint64_t producer_position = atomic_load_explicit(producer_position_address, memory_order_relaxed);
    const index_t message_state_offset = (producer_position & mask) * aligned_message_size;
    //the consumer_cache_position is no longer valid?
    if (producer_position >= consumer_cache_position &&
        !claim_slow_path(buffer, message_state_offset, consumer_cache_position_address, consumer_cache_position,
                         max_look_ahead_step, mask, aligned_message_size)) {
        return false;
    }
    atomic_store_explicit(producer_position_address, producer_position + 1, memory_order_relaxed);
    *claimed_message = buffer + message_state_offset + MESSAGE_STATE_SIZE;
    return true;
}

static inline bool
try_fixed_size_ring_buffer_claim(uint8_t *const buffer, const struct fixed_size_ring_buffer_header *const header,
                                 uint8_t **const claimed_message) {
    const _Atomic uint64_t *const producer_position_address = (_Atomic uint64_t *) header->producer_position;
    const index_t mask = header->mask;
    const index_t aligned_message_size = header->aligned_message_size;
    const uint64_t producer_position = atomic_load_explicit(producer_position_address, memory_order_relaxed);
    const index_t message_state_offset = (producer_position & mask) * aligned_message_size;
    const _Atomic uint32_t *const claimed_message_state_atomic_address = (_Atomic uint32_t *) (buffer +
                                                                                               message_state_offset);
    const uint32_t claimed_message_state_value = atomic_load_explicit(claimed_message_state_atomic_address,
                                                                      memory_order_relaxed);
    if (claimed_message_state_value != MESSAGE_STATE_FREE) {
        return false;
    }
    atomic_thread_fence(memory_order_acquire);
    atomic_store_explicit(producer_position_address, producer_position + 1, memory_order_relaxed);
    *claimed_message = buffer + message_state_offset + MESSAGE_STATE_SIZE;
    return true;
}

static inline void fixed_size_ring_buffer_commit_claim(const uint8_t *const claimed_message_address) {
    const _Atomic uint32_t *const message_state = (_Atomic uint32_t *) (claimed_message_address - MESSAGE_STATE_SIZE);
    atomic_store_explicit(message_state, MESSAGE_STATE_BUSY, memory_order_release);
}

static inline bool
try_fixed_size_ring_buffer_read(uint8_t *const buffer, const struct fixed_size_ring_buffer_header *const header,
                                uint8_t **const read_message_address) {
    const _Atomic uint64_t *const consumer_position_address = (_Atomic uint64_t *) header->consumer_position;
    const index_t mask = header->mask;
    const index_t aligned_message_size = header->aligned_message_size;
    const uint64_t consumer_position = atomic_load_explicit(consumer_position_address, memory_order_relaxed);
    const index_t message_state_offset = (consumer_position & mask) * aligned_message_size;
    uint8_t *const message_state_address = buffer + message_state_offset;
    const _Atomic uint32_t *const message_state_atomic_address = (_Atomic uint32_t *) message_state_address;
    const uint32_t message_state_value = atomic_load_explicit(message_state_atomic_address, memory_order_relaxed);
    //can't consume if not filled!
    if (message_state_value == MESSAGE_STATE_FREE) {
        return false;
    }
    atomic_thread_fence(memory_order_acquire);
    atomic_store_explicit(consumer_position_address, consumer_position + 1, memory_order_relaxed);
    *read_message_address = message_state_address + MESSAGE_STATE_SIZE;
    return true;
}

static inline void fixed_size_ring_buffer_commit_read(const uint8_t *const read_message_address) {
    const _Atomic uint32_t *const message_state = (_Atomic uint32_t *) (read_message_address - MESSAGE_STATE_SIZE);
    atomic_store_explicit(message_state, MESSAGE_STATE_FREE, memory_order_release);
}


inline static uint32_t fixed_size_ring_buffer_batch_read(
        uint8_t *const buffer,
        const struct fixed_size_ring_buffer_header *const header,
        const fixed_size_message_consumer consumer,
        const uint32_t count, void *const context) {
    uint32_t msg_read = 0;
    const _Atomic uint64_t *const consumer_position_address = (_Atomic uint64_t *) header->consumer_position;
    const index_t mask = header->mask;
    const index_t aligned_message_size = header->aligned_message_size;
    const uint64_t consumer_position = atomic_load_explicit(consumer_position_address, memory_order_relaxed);
    while (msg_read < count) {
        const uint64_t message_position = consumer_position + msg_read;
        const index_t message_state_offset = (message_position & mask) * aligned_message_size;
        uint8_t *const message_state_address = buffer + message_state_offset;
        const _Atomic uint32_t *const message_state_atomic_address = (_Atomic uint32_t *) message_state_address;
        const uint32_t message_state_value = atomic_load_explicit(message_state_atomic_address, memory_order_relaxed);
        if (message_state_value == MESSAGE_STATE_FREE) {
            return msg_read;
        } else {
            atomic_thread_fence(memory_order_acquire);
            atomic_store_explicit(consumer_position_address, message_position + 1, memory_order_relaxed);
            uint8_t *message_content_address = message_state_address + MESSAGE_STATE_SIZE;
            const bool stop = !consumer(message_content_address, context);
            atomic_store_explicit((_Atomic uint32_t *) message_state_address, MESSAGE_STATE_FREE, memory_order_release);
            msg_read++;
            if (stop) {
                return msg_read;
            }
        }
    }
    return count;
}

inline static uint32_t fixed_size_ring_buffer_stream_batch_read(
        uint8_t *const buffer,
        const struct fixed_size_ring_buffer_header *const header,
        const fixed_size_message_consumer consumer,
        const uint32_t count, void *const context) {
    uint8_t *read_message_address;
    for (uint32_t i = 0; i < count; i++) {
        if (!try_fixed_size_ring_buffer_read(buffer, header, &read_message_address)) {
            return i;
        } else {
            const bool stop = !consumer(read_message_address, context);
            fixed_size_ring_buffer_commit_read(read_message_address);
            if (stop) {
                return i + 1;
            }
        }
    }
    return count;
}

static inline index_t fixed_size_ring_buffer_size(const struct fixed_size_ring_buffer_header *const header) {
    const _Atomic uint64_t *consumer_position_address = (_Atomic uint64_t *) header->consumer_position;
    const _Atomic uint64_t *producer_position_address = (_Atomic uint64_t *) header->producer_position;
    const uint64_t consumer_position = atomic_load_explicit(consumer_position_address, memory_order_relaxed);
    const uint64_t producer_position = atomic_load_explicit(producer_position_address, memory_order_relaxed);
    const index_t size = (index_t) (producer_position - consumer_position);
    return size;
}



