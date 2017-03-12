//
// Created by forked_franz on 11/03/17.
//

#include <stdatomic.h>
#include <string.h>
#include "vs_rb.h"
#include "bytes_utils.c"

static const index_t RECORD_HEADER_LENGTH = sizeof(uint32_t) * 2;
static const index_t RECORD_ALIGNMENT = sizeof(uint32_t) * 2;
static const int32_t RECORD_PADDING_MSG_TYPE_ID = -1;

inline static index_t vs_rb_required_record_capacity(const index_t record_length) {
    return align(record_length + RECORD_HEADER_LENGTH, RECORD_ALIGNMENT);
}

inline static index_t length_offset(const index_t record_offset) {
    return record_offset;
}

inline static index_t msg_type_id_offset(const index_t record_offset) {
    return record_offset + sizeof(uint32_t);
}

inline static index_t vs_rb_encoded_msg_offset(const index_t record_offset) {
    return record_offset + RECORD_HEADER_LENGTH;
}

inline static uint64_t make_header(const int32_t msgTypeId, const index_t length) {
    return (((uint64_t) msgTypeId & 0xFFFFFFFF) << 32) | (length & 0xFFFFFFFF);
}

inline static index_t record_length(const uint64_t header) {
    return (index_t) header;
}

inline static uint32_t message_type_id(const uint64_t header) {
    return (uint32_t) (header >> 32);
}

inline static bool check_msg_type_id(const int32_t msgTypeId) {
    return (msgTypeId > 0);
}

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

inline static index_t vs_rb_capacity(const index_t requested_capacity) {
    const index_t ring_buffer_capacity = next_pow_2(requested_capacity) + RING_BUFFER_TRAILER_LENGTH;
    return ring_buffer_capacity;
}

inline static uint64_t load_consumer_position(const struct vs_rb_t *const header, const uint8_t *const buffer) {
    //use memory order relaxed instead?
    const uint64_t *consumer_position_address = (uint64_t *) (buffer + (header->consumer_position_index));
    return *consumer_position_address;
}

inline static uint64_t load_acquire_consumer_position(const struct vs_rb_t *const header, const uint8_t *const buffer) {
    const _Atomic uint64_t *consumer_position_address = (_Atomic uint64_t *) (buffer +
                                                                              (header->consumer_position_index));
    const uint64_t consumer_position_value = atomic_load_explicit(consumer_position_address, memory_order_acquire);
    return consumer_position_value;
}

inline static uint64_t vs_rb_load_consumer_position(const struct vs_rb_t *const header, const uint8_t *const buffer) {
    return load_acquire_consumer_position(header, buffer);
}

inline static void
store_release_consumer_position(const struct vs_rb_t *const header, const uint8_t *const buffer, const uint64_t value) {
    const _Atomic uint64_t *consumer_position_address = (_Atomic uint64_t *) (buffer + header->consumer_position_index);
    atomic_store_explicit(consumer_position_address, value, memory_order_release);
}

inline static uint64_t load_consumer_cache_position(const struct vs_rb_t *header, const uint8_t *buffer) {
    //use memory order relaxed instead?
    const uint64_t *consumer_cache_position_address = (uint64_t *) (buffer + (header->consumer_cache_position_index));
    return *consumer_cache_position_address;
}

inline static void
store_consumer_cache_position(const struct vs_rb_t *const header, const uint8_t *const buffer, const uint64_t value) {
    //use memory order relaxed instead?
    uint64_t *consumer_cache_position_address = (uint64_t *) (buffer + (header->consumer_cache_position_index));
    *consumer_cache_position_address = value;
}

inline static uint64_t load_producer_position(const struct vs_rb_t *header, const uint8_t *const buffer) {
    //use memory order relaxed instead?
    const uint64_t *producer_position_address = (uint64_t *) (buffer + (header->producer_position_index));
    return *producer_position_address;
}

inline static uint64_t load_acquire_producer_position(const struct vs_rb_t *const header, const uint8_t *const buffer) {
    const _Atomic uint64_t *producer_position_address = (_Atomic uint64_t *) (buffer +
                                                                              (header->producer_position_index));
    const uint64_t producer_position_value = atomic_load_explicit(producer_position_address, memory_order_acquire);
    return producer_position_value;
}

inline static uint64_t vs_rb_load_producer_position(const struct vs_rb_t *const header, const uint8_t *const buffer) {
    return load_acquire_producer_position(header, buffer);
}

inline static uint64_t load_acquire_msg_header(const uint8_t *const buffer, const index_t index) {
    const _Atomic uint64_t *msg_header_address = (_Atomic uint64_t *) (buffer + index);
    const uint64_t msg_header_value = atomic_load_explicit(msg_header_address, memory_order_acquire);
    return msg_header_value;
}

inline static void
store_release_msg_header(const uint8_t *const buffer, const index_t index, const uint64_t msg_header) {
    const _Atomic uint64_t *msg_header_address = (_Atomic uint64_t *) (buffer + index);
    atomic_store_explicit(msg_header_address, msg_header, memory_order_release);
}

inline static void
store_release_producer_position(const struct vs_rb_t *const header, const uint8_t *const buffer, const uint64_t value) {
    const _Atomic uint64_t *producer_position_address = (_Atomic uint64_t *) (buffer + header->producer_position_index);
    atomic_store_explicit(producer_position_address, value, memory_order_release);
}

inline static bool
cas_release_producer_position(const struct vs_rb_t *const header, const uint8_t *const buffer,
                              const uint64_t *const expected,
                              const uint64_t value) {
    //seems ok considering how the vs_rb_read is performed:
    //-on the failed path there will be dependent operations on the just updated "expected" value
    //-on the success path the final msg_header's commit is what really matter for the consumer side, and the producer
    // rely on the consumer_position to make any progress
    const _Atomic uint64_t *producer_position_address = (_Atomic uint64_t *) (buffer + header->producer_position_index);
    return atomic_compare_exchange_strong_explicit(producer_position_address, expected, value, memory_order_release,
                                                   memory_order_relaxed);
}

inline static bool new_vs_rb(struct vs_rb_t *const header, const index_t length) {
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

inline static bool
try_claim_when_full(const struct vs_rb_t *const header, const uint8_t *const buffer, const uint64_t producer_position,
                    const index_t required_capacity, uint64_t *const consumer_position) {
    const uint64_t last_consumer_position = load_acquire_consumer_position(header, buffer);
    //it is really full?
    const int64_t size = producer_position - last_consumer_position;
    const int64_t available_capacity = (int64_t) header->capacity - size;
    if (required_capacity > available_capacity) {
        //is full!
        //do not need to refresh the cache position because it doesn't change the producer perception of having a full buffer!
        return false;
    } else {
        //refresh the consumer cache position to allow batch writes
        store_consumer_cache_position(header, buffer, last_consumer_position);
        *consumer_position = last_consumer_position;
        return true;
    }
}

inline static bool try_acquire_from_start_of_buffer(const struct vs_rb_t *const header, const uint8_t *const buffer,
                                                    const index_t required_capacity, const index_t mask,
                                                    uint64_t *consumer_position) {
    const uint64_t last_consumer_position = load_acquire_consumer_position(header, buffer);
    //LoadLoad + LoadStore
    const index_t consumer_index = last_consumer_position & mask;
    if (required_capacity > consumer_index) {
        //there is not enough space to claim a record from the start of the buffer, the consumer is slow!
        return false;
    } else {
        store_consumer_cache_position(header, buffer, last_consumer_position);
        *consumer_position = last_consumer_position;
        return true;
    }
}

inline static bool try_claim_when_need_pad(const struct vs_rb_t *const header, const uint8_t *const buffer,
                                           const index_t required_capacity,
                                           const index_t mask,
                                           uint64_t *const consumer_position) {
    const index_t consumer_index = (*consumer_position) & mask;
    //there is enough space from the start of the buffer?
    if (required_capacity > consumer_index) {
        return try_acquire_from_start_of_buffer(header, buffer, required_capacity, mask, consumer_position);
    }
    return true;
}

inline static bool
vs_rb_try_mp_claim(const struct vs_rb_t *const header, const uint8_t *const buffer,
                   const index_t required_capacity,
                   uint64_t *const claimed_position, index_t *const claimed_index) {
    if (required_capacity > header->max_msg_length) {
        return false;
    }
    const index_t capacity = header->capacity;
    const index_t mask = capacity - 1;
    uint64_t consumer_position = load_consumer_cache_position(header, buffer);
    index_t padding = 0;
    uint64_t producer_position = 0;
    index_t producer_index = 0;
    const index_t required_msg_capacity = vs_rb_required_record_capacity(required_capacity);
    producer_position = load_acquire_producer_position(header, buffer);
    do {
        //producer position is the last read every time: on the first iteration or due to a previous failed cas
        const int64_t size = producer_position - consumer_position;
        //the available capacity could be negative due to a stale/cached consumer_position value
        const int64_t available_capacity = (int64_t) capacity - size;
        if (required_msg_capacity > available_capacity) {
            if (!try_claim_when_full(header, buffer, producer_position, required_msg_capacity, &consumer_position)) {
                return false;
            }
        }
        padding = 0;
        producer_index = producer_position & mask;
        const index_t bytes_until_end_of_buffer = capacity - producer_index;
        //the claim fits the space until the end of the buffer?
        if (required_msg_capacity > bytes_until_end_of_buffer) {
            //need padding before claim the record...but there will be enough space from the start of the buffer?
            if (!try_claim_when_need_pad(header, buffer, required_msg_capacity, mask, &consumer_position)) {
                return false;
            }
            padding = bytes_until_end_of_buffer;
        }
    } while (!cas_release_producer_position(header, buffer, &producer_position,
                                            producer_position + required_msg_capacity + padding));
    //the cas while succeed doesn't modify the producer_position local value
    if (padding != 0) {
        store_release_msg_header(buffer, producer_index, make_header(RECORD_PADDING_MSG_TYPE_ID, padding));
        const uint64_t msg_position = producer_position + padding;
        *claimed_position = msg_position;
        const index_t msg_index = msg_position & mask;
        *claimed_index = msg_index;
    } else {
        const uint64_t msg_position = producer_position;
        *claimed_position = msg_position;
        const index_t msg_index = producer_index;
        *claimed_index = msg_index;
    }
    return true;
}


inline static bool
vs_rb_try_sp_claim(const struct vs_rb_t *const header, const uint8_t *const buffer,
                   const index_t required_capacity,
                   uint64_t *const claimed_position, index_t *const claimed_index) {
    if (required_capacity > header->max_msg_length) {
        return false;
    }
    const index_t capacity = header->capacity;
    const index_t mask = capacity - 1;
    uint64_t consumer_position = load_consumer_cache_position(header, buffer);
    const uint64_t producer_position = load_producer_position(header, buffer);
    const index_t required_msg_capacity = vs_rb_required_record_capacity(required_capacity);
    const int64_t size = producer_position - consumer_position;
    //the available capacity can't be negative due to a stale/cached consumer_position value,
    //because only one producer could progress the consumer!
    const int64_t available_capacity = (int64_t) capacity - size;
    if (required_msg_capacity > available_capacity) {
        if (!try_claim_when_full(header, buffer, producer_position, required_msg_capacity, &consumer_position)) {
            return false;
        }
    }
    index_t padding = 0;
    const index_t producer_index = producer_position & mask;
    const index_t bytes_until_end_of_buffer = capacity - producer_index;
    //the claim fits the space until the end of the buffer?
    if (required_msg_capacity > bytes_until_end_of_buffer) {
        //need padding before claim the record...but there will be enough space from the start of the buffer?
        if (!try_claim_when_need_pad(header, buffer, required_msg_capacity, mask, &consumer_position)) {
            return false;
        }
        padding = bytes_until_end_of_buffer;
    }
    const uint64_t new_producer_position = producer_position + required_msg_capacity + padding;
    store_release_producer_position(header, buffer, new_producer_position);
    if (padding != 0) {
        store_release_msg_header(buffer, producer_index, make_header(RECORD_PADDING_MSG_TYPE_ID, padding));
        const uint64_t msg_position = producer_position + padding;
        *claimed_position = msg_position;
        const index_t msg_index = msg_position & mask;
        *claimed_index = msg_index;
    } else {
        const uint64_t msg_position = producer_position;
        *claimed_position = msg_position;
        const index_t msg_index = producer_index;
        *claimed_index = msg_index;
    }
    return true;
}


inline static bool
vs_rb_commit_claim(const uint8_t *const buffer, const index_t msg_index, const uint32_t msg_type_id,
                   const index_t msg_content_length) {
    //msg_length is the lengh of the content
    if (!check_msg_type_id(msg_type_id)) {
        return false;
    }
    store_release_msg_header(buffer, msg_index, make_header(msg_type_id, msg_content_length + RECORD_HEADER_LENGTH));
    return true;
}

//declare a const pointer to a function with this signature
typedef bool(*const vs_rb_message_consumer)(const uint32_t, const uint8_t *const,
                                            const index_t,
                                            const index_t, void *const);

inline static uint32_t vs_rb_read(const struct vs_rb_t *const header, uint8_t *const buffer,
                                  const vs_rb_message_consumer consumer,
                                  const uint32_t count, void *context) {
    uint32_t msg_read = 0;
    const uint64_t consumer_position = load_consumer_position(header, buffer);
    const index_t capacity = header->capacity;
    const index_t consumer_index = consumer_position & (capacity - 1);
    const uint32_t remaining_bytes = capacity - consumer_index;
    index_t bytes_consumed = 0;
    bool stop = false;
    while (!stop && (bytes_consumed < remaining_bytes) && (msg_read < count)) {
        const index_t msg_index = consumer_index + bytes_consumed;
        const uint64_t msg_header = load_acquire_msg_header(buffer, msg_index);
        const index_t msg_length = record_length(msg_header);
        if (msg_length <= 0) {
            //backpressured -> need a special return value?
            stop = true;
        } else {
            const index_t required_msg_length = align(msg_length, RECORD_ALIGNMENT);
            bytes_consumed += required_msg_length;
            const uint32_t msg_type_id = message_type_id(msg_header);
            if (msg_type_id != RECORD_PADDING_MSG_TYPE_ID) {
                msg_read++;
                const index_t msg_content_length = msg_length - RECORD_HEADER_LENGTH;
                const index_t msg_content_index = msg_index + RECORD_HEADER_LENGTH;
                stop = !consumer(msg_type_id, buffer, msg_content_index, msg_content_length, context);
            }
        }
    }
    if (bytes_consumed != 0) {
        //zeroes all the consumed bytes
        void *consumer_start_offset = buffer + consumer_index;
        memset(consumer_start_offset, 0, bytes_consumed);
        const uint64_t new_consumer_position = consumer_position + bytes_consumed;
        store_release_consumer_position(header, buffer, new_consumer_position);
    }
    return msg_read;
}

inline static index_t vs_rb_size(const struct vs_rb_t *const header, const uint8_t *const buffer) {
    uint64_t previousConsumerPosition;
    uint64_t producerPosition;
    uint64_t consumerPosition = load_acquire_consumer_position(header, buffer);

    do {
        previousConsumerPosition = consumerPosition;
        producerPosition = load_acquire_producer_position(header, buffer);
        consumerPosition = load_acquire_consumer_position(header, buffer);
    } while (consumerPosition != previousConsumerPosition);
    const index_t size = (producerPosition - consumerPosition);
    return size;
}