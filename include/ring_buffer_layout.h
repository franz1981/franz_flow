//
// Created by forked_franz on 06/02/17.
//

#ifndef FRANZ_FLOW_RING_BUFFER_LAYOUT_H
#define FRANZ_FLOW_RING_BUFFER_LAYOUT_H

#include <stdint.h>
#include <stdatomic.h>
#include "message_layout.h"
#include "index.h"
#include "bytes_utils.h"

/**
 * Offset within the trailer for where the producer value is stored.
 */
static const index_t RING_BUFFER_PRODUCER_POSITION_OFFSET = CACHE_LINE_LENGTH * 2;
/**
 * Offset within the trailer for where the consumer cache value is stored.
 */
static const index_t RING_BUFFER_CONSUMER_CACHE_POSITION_OFFSET = CACHE_LINE_LENGTH * 4;
/**
 * Offset within the trailer for where the head value is stored.
 */
static const index_t RING_BUFFER_CONSUMER_POSITION_OFFSET = CACHE_LINE_LENGTH * 6;
/**
 * Total length of the trailer in bytes.
 */
static const index_t RING_BUFFER_TRAILER_LENGTH = CACHE_LINE_LENGTH * 8;

inline static bool ring_buffer_check_capacity(const index_t capacity) {
    return is_pow_2(capacity - RING_BUFFER_TRAILER_LENGTH);
}

inline static index_t ring_buffer_capacity(const index_t requested_capacity) {
    const index_t ring_buffer_capacity = next_pow_2(requested_capacity) + RING_BUFFER_TRAILER_LENGTH;
    return ring_buffer_capacity;
}

struct ring_buffer_header {
    index_t max_msg_length;
    index_t producer_position_index;
    index_t consumer_cache_position_index;
    index_t consumer_position_index;
    index_t capacity;
};

inline static bool init_ring_buffer_header(struct ring_buffer_header *const header, const index_t length) {
    if (!ring_buffer_check_capacity(length)) {
        return false;
    }
    const index_t capacity = length - RING_BUFFER_TRAILER_LENGTH;
    const index_t max_msg_length = capacity - RECORD_HEADER_LENGTH;
    const index_t producer_position_index = capacity + RING_BUFFER_PRODUCER_POSITION_OFFSET;
    const index_t consumer_cache_position_index = capacity + RING_BUFFER_CONSUMER_CACHE_POSITION_OFFSET;
    const index_t consumer_position_index = capacity + RING_BUFFER_CONSUMER_POSITION_OFFSET;
    header->capacity = capacity;
    header->max_msg_length = max_msg_length;
    header->producer_position_index = producer_position_index;
    header->consumer_cache_position_index = consumer_cache_position_index;
    header->consumer_position_index = consumer_position_index;
    return true;
}

inline static uint64_t load_consumer_position(const struct ring_buffer_header *const header, const uint8_t *const buffer) {
    //use memory order relaxed instead?
    const uint64_t *consumer_position_address = (uint64_t *) (buffer + (header->consumer_position_index));
    return *consumer_position_address;
}

inline static uint64_t load_acquire_consumer_position(const struct ring_buffer_header *const header, const uint8_t *const buffer) {
    const _Atomic uint64_t *consumer_position_address = (_Atomic uint64_t *) (buffer +
                                                                              (header->consumer_position_index));
    const uint64_t consumer_position_value = atomic_load_explicit(consumer_position_address, memory_order_acquire);
    return consumer_position_value;
}

inline static void
store_release_consumer_position(const struct ring_buffer_header *const header, const uint8_t *const buffer, const uint64_t value) {
    const _Atomic uint64_t *consumer_position_address = (_Atomic uint64_t *) (buffer + header->consumer_position_index);
    atomic_store_explicit(consumer_position_address, value, memory_order_release);
}

inline static uint64_t load_consumer_cache_position(const struct ring_buffer_header *header, const uint8_t *buffer) {
    //use memory order relaxed instead?
    const uint64_t *consumer_cache_position_address = (uint64_t *) (buffer + (header->consumer_cache_position_index));
    return *consumer_cache_position_address;
}

inline static void
store_consumer_cache_position(const struct ring_buffer_header *const header, const uint8_t *const buffer, const uint64_t value) {
    //use memory order relaxed instead?
    uint64_t *consumer_cache_position_address = (uint64_t *) (buffer + (header->consumer_cache_position_index));
    *consumer_cache_position_address = value;
}

inline static uint64_t load_producer_position(const struct ring_buffer_header *header, const uint8_t *const buffer) {
    //use memory order relaxed instead?
    const uint64_t *producer_position_address = (uint64_t *) (buffer + (header->producer_position_index));
    return *producer_position_address;
}

inline static uint64_t load_acquire_producer_position(const struct ring_buffer_header *const header, const uint8_t *const buffer) {
    const _Atomic uint64_t *producer_position_address = (_Atomic uint64_t *) (buffer +
                                                                              (header->producer_position_index));
    const uint64_t producer_position_value = atomic_load_explicit(producer_position_address, memory_order_acquire);
    return producer_position_value;
}

inline static uint64_t load_acquire_msg_header(const uint8_t *const buffer, const index_t index) {
    const _Atomic uint64_t *msg_header_address = (_Atomic uint64_t *) (buffer + index);
    const uint64_t msg_header_value = atomic_load_explicit(msg_header_address, memory_order_acquire);
    return msg_header_value;
}

inline static void store_release_msg_header(const uint8_t *const buffer, const index_t index, const uint64_t msg_header) {
    const _Atomic uint64_t *msg_header_address = (_Atomic uint64_t *) (buffer + index);
    atomic_store_explicit(msg_header_address, msg_header, memory_order_release);
}

inline static void
store_release_producer_position(const struct ring_buffer_header *const header, const uint8_t *const buffer, const uint64_t value) {
    const _Atomic uint64_t *producer_position_address = (_Atomic uint64_t *) (buffer + header->producer_position_index);
    atomic_store_explicit(producer_position_address, value, memory_order_release);
}

inline static bool
cas_release_producer_position(const struct ring_buffer_header *const header, const uint8_t *const buffer, const uint64_t *const expected,
                              const uint64_t value) {
    //seems ok considering how the ring_buffer_batch_read is performed:
    //-on the failed path there will be dependent operations on the just updated "expected" value
    //-on the success path the final msg_header's commit is what really matter for the consumer side, and the producer
    // rely on the consumer_position to make any progress
    const _Atomic uint64_t *producer_position_address = (_Atomic uint64_t *) (buffer + header->producer_position_index);
    return atomic_compare_exchange_strong_explicit(producer_position_address, expected, value, memory_order_release,
                                                   memory_order_relaxed);
}

#endif //FRANZ_FLOW_RING_BUFFER_LAYOUT_H
