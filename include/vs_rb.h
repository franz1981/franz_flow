//
// Created by forked_franz on 11/03/17.
//

#ifndef FRANZ_FLOW_VS_RB_H
#define FRANZ_FLOW_VS_RB_H

#include <stdbool.h>
#include "index.h"

struct vs_rb_t {
    index_t max_msg_length;
    index_t producer_position_index;
    index_t consumer_cache_position_index;
    index_t consumer_position_index;
    index_t capacity;
};

inline static index_t vs_rb_required_record_capacity(const index_t record_length);

inline static index_t vs_rb_capacity(const index_t requested_capacity);

inline static index_t vs_rb_encoded_msg_offset(const index_t record_offset);

inline static bool new_vs_rb(struct vs_rb_t *const header, const index_t length);

inline static uint64_t vs_rb_load_consumer_position(const struct vs_rb_t *const header, const uint8_t *const buffer);

inline static uint64_t vs_rb_load_producer_position(const struct vs_rb_t *const header, const uint8_t *const buffer);

inline static bool
vs_rb_try_mp_claim(const struct vs_rb_t *const header, const uint8_t *const buffer,
                   const index_t required_capacity,
                   uint64_t *const claimed_position, index_t *const claimed_index);

inline static bool
vs_rb_try_sp_claim(const struct vs_rb_t *const header, const uint8_t *const buffer,
                   const index_t required_capacity,
                   uint64_t *const claimed_position, index_t *const claimed_index);

inline static bool
vs_rb_commit_claim(const uint8_t *const buffer, const index_t msg_index, const uint32_t msg_type_id,
                   const index_t msg_content_length);

//declare a const pointer to a function with this signature
typedef bool(*const vs_rb_message_consumer)(const uint32_t, const uint8_t *const,
                                            const index_t,
                                            const index_t, void *const);

inline static uint32_t vs_rb_read(const struct vs_rb_t *const header, uint8_t *const buffer,
                                  const vs_rb_message_consumer consumer,
                                  const uint32_t count, void *context);

inline static index_t vs_rb_size(const struct vs_rb_t *const header, const uint8_t *const buffer);

#endif //FRANZ_FLOW_VS_RB_H
