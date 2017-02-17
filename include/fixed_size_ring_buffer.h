//
// Created by forked_franz on 10/02/17.
//

#ifndef FRANZ_FLOW_FIXED_SIZE_RING_BUFFER_H
#define FRANZ_FLOW_FIXED_SIZE_RING_BUFFER_H

#include <stdbool.h>
#include <stdio.h>
#include "index.h"

/**
 * It holds the configuration of a fixed message size ring buffer.
 *
 * To initialize it is necessary to:
 *  * use {@code fixed_size_ring_buffer_capacity} to get the proper size in bytes of the ring buffer (+ embedded trailer header)
 *  * use {@code init_fixed_size_ring_buffer_header} passing it a not null header's pointer to be filled with the configuration
 *
 * The initialized header can be used to modify/access the state of the ring buffer.
 * Some fields hold values freely readable while others point to the ring buffer embedded trailer
 * and can be used only by other methods to inspect the producer/consumer states.
 * The fields that can be readable directly are marked as [readable], [not readable] otherwise.
 *
 */
struct fixed_size_ring_buffer_header {
    uint8_t *producer_position;         /*  producer position sequence                              [not readable]      */
    uint8_t *consumer_cache_position;   /*  last consumer sequence read by the producer             [not readable]      */
    uint8_t *consumer_position;         /*  consumer position sequence                              [not readable]      */
    index_t mask;                       /*  ===(capacity-1) used to speed up modulus operations     [readable]          */
    index_t capacity;                   /*  max number of messages contained in the ring_buffer     [readable]          */
    uint32_t aligned_message_size;      /*  real size in bytes of each message                      [readable]          */
};

/**
 * Returns the requested capacity in bytes of the ring buffer plus the trailer.
 *
 * @param requested_capacity    the max number of messages the ring buffer can hold
 * @param message_size          the size in bytes of each message
 * @returns                     the capacity in bytes of the ring buffer + trailer
 */
static inline index_t fixed_size_ring_buffer_capacity(const index_t requested_capacity, const uint32_t message_size);

/**
 * Initialize the headers fields and the ring buffer trailer to be used by the producer/consumer.
 *
 * @param buffer                a {@code NOT NULL} buffer's pointer to the ring buffer data + trailer, only the trailer will be changed
 * @param header                a {@code NOT NULL} header's pointer, all the fields will be changed
 * @param requested_capacity    the max number of messages the ring buffer can hold
 * @param message_size          the size in bytes of each message
 * @returns                     {@code true} is the header is being initialized, {@code false} otherwise
 */
static inline bool init_fixed_size_ring_buffer_header(
        uint8_t *const buffer,
        struct fixed_size_ring_buffer_header *const header,
        const index_t requested_capacity,
        const uint32_t message_size);

/**
 * Try to claim a new slot inside the ring buffer into which the caller can write a message with zero copy semantics.
 *
 * After writing the message, the caller must call {@code fixed_size_ring_buffer_commit_claim} to make it available to be consumed.
 *
 * @param buffer                a {@code NOT NULL} buffer's pointer to the ring buffer data + trailer, only the trailer will be changed
 * @param header                a {@code NOT NULL} header's pointer, only producer_position and consumer_cache_position will be changed
 * @param max_look_ahead_step   the max size of a batch of offers if there is enough space available; can't be higher than the ring buffer's capacity
 * @param claimed_message       a pointer to the claimed content of the ring buffer: is aligned to 4 bytes and has size equals to @code{(header->aligned_message_size - 4)}
 * @returns                     {@code true} if the buffer is not empty, {@code false} otherwise
 */
static inline bool try_fixed_size_ring_buffer_claim(
        uint8_t *const buffer,
        const struct fixed_size_ring_buffer_header *const header,
        const uint32_t max_look_ahead_step,
        uint8_t **const claimed_message);

static inline bool try_fixed_size_ring_buffer_mp_claim(
        uint8_t *const buffer,
        const struct fixed_size_ring_buffer_header *const header,
        uint8_t **const claimed_message);

static inline void fixed_size_ring_buffer_commit_claim(const uint8_t *const claimed_message_address);

typedef bool(*const fixed_size_message_consumer)(uint8_t *const, void *const);

inline static uint32_t fixed_size_ring_buffer_batch_read(
        uint8_t *const buffer,
        const struct fixed_size_ring_buffer_header *const header,
        const fixed_size_message_consumer consumer,
        const uint32_t count, void *const context);

static inline index_t fixed_size_ring_buffer_size(const struct fixed_size_ring_buffer_header *const header);

#endif //FRANZ_FLOW_FIXED_SIZE_RING_BUFFER_H
