//
// Created by forked_franz on 24/01/17.
//

#ifndef FRANZ_FLOW_RECORD_DESCRIPTOR_H
#define FRANZ_FLOW_RECORD_DESCRIPTOR_H

#include <stdint.h>
#include <stdbool.h>
#include "index.h"
#include "bytes_utils.h"


static const index_t RECORD_HEADER_LENGTH = sizeof(uint32_t) * 2;
static const index_t RECORD_ALIGNMENT = sizeof(uint32_t) * 2;
static const index_t RECORD_PADDING_MSG_TYPE_ID = -1;

inline static index_t required_record_capacity(const index_t record_length){
    return align(record_length + RECORD_HEADER_LENGTH, RECORD_ALIGNMENT);
}

inline static index_t length_offset(const index_t record_offset) {
    return record_offset;
}

inline static index_t msg_type_id_offset(const index_t record_offset) {
    return record_offset + sizeof(uint32_t);
}

inline static index_t encoded_msg_offset(const index_t record_offset) {
    return record_offset + RECORD_HEADER_LENGTH;
}

inline static uint64_t make_header(const uint32_t msgTypeId, const index_t length) {
    return (((uint64_t) msgTypeId & 0xFFFFFFFF) << 32) | (length & 0xFFFFFFFF);
}

inline static index_t record_length(const uint64_t header) {
    return (index_t) header;
}

inline static uint32_t message_type_id(const uint64_t header) {
    return (uint32_t) (header >> 32);
}

inline static bool check_msg_type_id(const uint32_t msgTypeId) {
    return (msgTypeId > 0);
}

#endif //FRANZ_FLOW_RECORD_DESCRIPTOR_H