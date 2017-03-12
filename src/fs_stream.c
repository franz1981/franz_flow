//
// Created by forked_franz on 18/02/17.
//

#include <stdatomic.h>
#include "fs_stream.h"
#include "bytes_utils.c"

#define MESSAGE_STATE_SIZE 4

static const uint32_t MESSAGE_STATE_FREE = 0;
static const uint32_t MESSAGE_STATE_BUSY = 1;
static const uint32_t ACTIVE_CYCLE_INDEX_OFFSET = CACHE_LINE_LENGTH * 2;
static const uint32_t CONSUMER_CACHE_POSITION_OFFSET = CACHE_LINE_LENGTH * 4;
static const uint32_t CONSUMER_POSITION_OFFSET = CACHE_LINE_LENGTH * 6;
static const uint32_t PRODUCERS_CYCLE_CLAIM_OFFSET = CACHE_LINE_LENGTH * 8;

static inline uint32_t
fs_stream_capacity(const uint32_t requested_capacity, const uint32_t message_size, const uint32_t cycles) {
    if (cycles < 2) {
        //ERROR!
        return 0;
    }
    const uint32_t next_pow_2_requested_capacity = next_pow_2(requested_capacity);
    const uint32_t next_pow_2_cycles = next_pow_2(cycles);
    const uint32_t aligned_message_size = align(message_size + MESSAGE_STATE_SIZE, MESSAGE_STATE_SIZE);
    const uint32_t stream_capacity = next_pow_2_requested_capacity * next_pow_2_cycles;
    const uint32_t capacity_bytes = stream_capacity * aligned_message_size;
    const uint32_t trailer_bytes =
            PRODUCERS_CYCLE_CLAIM_OFFSET + align((sizeof(uint64_t) * next_pow_2_cycles), CACHE_LINE_LENGTH * 2);
    return (capacity_bytes + trailer_bytes);
}

static inline bool new_fs_stream(
        uint8_t *const buffer,
        struct fs_stream_t *const stream,
        const uint32_t requested_capacity,
        const uint32_t message_size,
        const uint32_t cycles) {
    const uint32_t next_pow_2_requested_capacity = next_pow_2(requested_capacity);
    const uint32_t next_pow_2_cycles = next_pow_2(cycles);

    const uint32_t aligned_message_size = align(message_size + MESSAGE_STATE_SIZE, MESSAGE_STATE_SIZE);
    const uint32_t stream_capacity = next_pow_2_requested_capacity * next_pow_2_cycles;
    const uint32_t capacity_bytes = stream_capacity * aligned_message_size;
    stream->capacity = stream_capacity;
    stream->mask = stream_capacity - 1;
    //No producers can claim positions in the same cycle index of the consumer.
    //if the back-pressure fails to stop a producer, it won't succeed to claim more than the cycle limit:
    //if the claim reachs the cycle limit it will cause a cycle rotation (that refreshes the current cycle position for any producers)
    // and the new positions claims will fail against the max gain limit.
    stream->max_gain = (next_pow_2_requested_capacity * (next_pow_2_cycles - 1));
    stream->mask_cycle_length = next_pow_2_requested_capacity - 1;
    stream->cycle_length = next_pow_2_requested_capacity;
    stream->mask_cycles = next_pow_2_cycles - 1;
    stream->cycles = next_pow_2_cycles;
    stream->aligned_message_size = aligned_message_size;
    //indexes
    stream->buffer = buffer;
    stream->active_cycle_index = (_Atomic uint32_t *) (buffer + capacity_bytes + ACTIVE_CYCLE_INDEX_OFFSET);
    stream->consumer_cache_position = (_Atomic uint64_t *) (buffer + capacity_bytes + CONSUMER_CACHE_POSITION_OFFSET);
    stream->consumer_position = (_Atomic uint64_t *) (buffer + capacity_bytes + CONSUMER_POSITION_OFFSET);
    stream->producers_cycle_claim = (_Atomic uint64_t *) (buffer + capacity_bytes + PRODUCERS_CYCLE_CLAIM_OFFSET);
    return true;
}

static void rotate_cycle(const struct fs_stream_t *const stream,
                         const uint32_t active_cycle_index,
                         const uint64_t producer_cycle_claim) {
    //manage cycle rotation too!
    //next cycle index?
    const uint32_t next_active_cycle_index = (active_cycle_index + 1) & stream->mask_cycles;
    _Atomic uint64_t *next_producers_active_cycle_claim_address =
            stream->producers_cycle_claim + (sizeof(uint64_t) * next_active_cycle_index);
    //next cycle id
    const uint64_t next_cycle_id = (producer_cycle_claim >> 32) + 1;
    const uint64_t next_producer_cycle_claim = next_cycle_id << 32;
    //reset the active cycle producer claim position
    atomic_store_explicit(next_producers_active_cycle_claim_address, next_producer_cycle_claim,
                          memory_order_relaxed);
    //write release it changeing the next active cycle index too!
    atomic_store_explicit(stream->active_cycle_index, next_active_cycle_index, memory_order_release);
}

static bool is_backpressured(const struct fs_stream_t *const stream, const uint64_t producer_position) {
    const uint64_t consumer_position = atomic_load_explicit(stream->consumer_position, memory_order_relaxed);
    const uint64_t claim_limit = consumer_position + stream->max_gain;
    if (producer_position < claim_limit) {
        //acquires the content written by the consumer only when the producer could use it!
        atomic_thread_fence(memory_order_acquire);
        //update the cached consumer position for the other producers
        atomic_store_explicit(stream->consumer_cache_position, consumer_position, memory_order_relaxed);
        return false;
    } else {
        //backpressured due to reached limit!!
        return true;
    }
}

static inline bool fs_stream_try_claim(
        const struct fs_stream_t *const stream,
        uint8_t **const claimed_message) {
    //calculate the claim limit based on consumer_cached_position
    const uint32_t active_cycle_index = atomic_load_explicit(stream->active_cycle_index, memory_order_acquire);
    _Atomic uint64_t *producers_active_cycle_claim_address =
            stream->producers_cycle_claim + (sizeof(uint64_t) * active_cycle_index);
    const uint64_t producer_claim = atomic_load_explicit(producers_active_cycle_claim_address, memory_order_relaxed);
    //translate the claim in an absolute sequence value
    const uint64_t producer_claim_cycle_position = (producer_claim & 0xFFFFFFFF);
    //a claim position could be > cycle length
    const uint64_t producer_claim_cycle_id = (producer_claim >> 32);
    const uint64_t producer_position = (producer_claim_cycle_id * stream->cycle_length) + producer_claim_cycle_position;
    const uint64_t claim_limit =
            atomic_load_explicit(stream->consumer_cache_position, memory_order_relaxed) + stream->max_gain;
    if (producer_position >= claim_limit) {
        if (is_backpressured(stream, producer_position)) {
            return false;
        }
    }
    //try to claim on the current active cycle
    const uint64_t producer_cycle_claim = atomic_fetch_add_explicit(producers_active_cycle_claim_address, 1,
                                                                    memory_order_relaxed);
    const uint64_t cycle_position = (producer_cycle_claim & 0xFFFFFFFF);
    if (cycle_position < stream->cycle_length) {
        //compute the claimed message content position in the stream!
        const uint32_t offset =
                ((active_cycle_index * stream->cycle_length) + cycle_position) * stream->aligned_message_size;
        *claimed_message = stream->buffer + offset + MESSAGE_STATE_SIZE;
        return true;
    } else if (cycle_position == stream->cycle_length) {
        rotate_cycle(stream, active_cycle_index, producer_cycle_claim);
        return false;
    } else {
        return false;
    }
}

static inline void fs_stream_commit_claim(const uint8_t *const claimed_message_address) {
    const _Atomic uint32_t *const message_state = (_Atomic uint32_t *) (claimed_message_address - MESSAGE_STATE_SIZE);
    atomic_store_explicit(message_state, MESSAGE_STATE_BUSY, memory_order_release);
}

static inline uint32_t fs_stream_read(
        const struct fs_stream_t *const stream,
        const fs_stream_message_consumer consumer,
        const uint32_t count, void *const context) {
    uint32_t msg_read = 0;
    const _Atomic uint64_t *const consumer_position_address = stream->consumer_position;
    const uint32_t mask = stream->mask;
    uint8_t *const buffer = stream->buffer;
    const uint32_t aligned_message_size = stream->aligned_message_size;
    const uint64_t consumer_position = atomic_load_explicit(consumer_position_address, memory_order_relaxed);
    while (msg_read < count) {
        const uint64_t message_position = consumer_position + msg_read;
        const uint32_t message_state_offset = (message_position & mask) * aligned_message_size;
        uint8_t *const message_state_address = buffer + message_state_offset;
        const _Atomic uint32_t *const message_state_atomic_address = (_Atomic uint32_t *) message_state_address;
        const uint32_t message_state_value = atomic_load_explicit(message_state_atomic_address, memory_order_relaxed);
        if (message_state_value == MESSAGE_STATE_FREE) {
            return msg_read;
        } else {
            atomic_thread_fence(memory_order_acquire);
            uint8_t *message_content_address = message_state_address + MESSAGE_STATE_SIZE;
            const bool stop = !consumer(message_content_address, context);
            atomic_store_explicit((_Atomic uint32_t *) message_state_address, MESSAGE_STATE_FREE, memory_order_relaxed);
            //the release on the consumer allow the producers to continue to push data:
            //it is a write release to be sure that any content modified by consumer will be visible only after
            //read acquire the consumer
            atomic_store_explicit(consumer_position_address, message_position + 1, memory_order_release);
            msg_read++;
            if (stop) {
                return msg_read;
            }
        }
    }
    return count;
}

static inline uint64_t fs_stream_load_producer_position(const struct fs_stream_t *const stream) {
    const uint32_t active_cycle_index = atomic_load_explicit(stream->active_cycle_index, memory_order_acquire);
    _Atomic uint64_t *producers_active_cycle_claim_address =
            stream->producers_cycle_claim + (sizeof(uint64_t) * active_cycle_index);
    const uint64_t producer_claim = atomic_load_explicit(producers_active_cycle_claim_address, memory_order_relaxed);
    uint64_t cycle_position = (producer_claim & 0xFFFFFFFF);
    const uint32_t cycle_length = stream->cycle_length;
    if (cycle_position > cycle_length) {
        cycle_position = cycle_length;
    }
    const uint64_t cycle_id = (producer_claim >> 32);
    const uint64_t producer_position = (cycle_id * stream->cycle_length) + cycle_position;
    return producer_position;
}

static inline uint64_t fs_stream_load_consumer_position(const struct fs_stream_t *const stream) {
    const uint64_t consumer_position = atomic_load_explicit(stream->consumer_position, memory_order_relaxed);
    return consumer_position;
}

static inline uint32_t fs_stream_size(const struct fs_stream_t *const stream) {
    const uint64_t producer_position = fs_stream_load_producer_position(stream);
    const uint64_t consumer_position = fs_stream_load_consumer_position(stream);
    const uint32_t size = (uint32_t) (producer_position - consumer_position);
    return size;
}