#include <stdio.h>
#include <inttypes.h>
#include <string.h>
#include <pthread.h>
#include "ring_buffer.h"

#define DEFAULT_MSG_TYPE_ID 1
#define DEFAULT_MSG_LENGTH 8

struct ring_buffer_test {
    struct ring_buffer_header *header;
    uint8_t *buffer;
    uint32_t tests;
    uint32_t messages;
};

void *producer(void *arg) {
    struct ring_buffer_test *test = (struct ring_buffer_test *) arg;
    struct ring_buffer_header *header = test->header;
    uint8_t *buffer = test->buffer;
    const uint32_t tests = test->tests;
    const uint32_t messages = test->messages;

    uint64_t claimed_position;
    index_t claimed_index;
    uint64_t msg_content = 0;
    for (uint32_t t = 0; t < tests; t++) {
        struct timespec start_time;
        struct timespec end_time;
        uint64_t total_try = 0;
        clock_gettime(CLOCK_THREAD_CPUTIME_ID, &start_time);
        for (uint32_t m = 0; m < messages; m++) {
            while (!try_ring_buffer_sp_claim(header, buffer, DEFAULT_MSG_LENGTH, &claimed_position, &claimed_index)) {
                __asm__ __volatile__("pause;");
                total_try++;
                //wait strategy
            }
            total_try++;
            //provides better way to perform zero copy!!!!
            uint64_t *content_offset = (uint64_t *) (buffer + encoded_msg_offset(claimed_index));
            *content_offset = msg_content + 1;
            ring_buffer_commit(buffer, claimed_index, DEFAULT_MSG_TYPE_ID, DEFAULT_MSG_LENGTH);
            msg_content++;
        }
        //wait until all the messages get consumed
        while (ring_buffer_size(header, buffer) != 0) {
            __asm__ __volatile__("pause;");
            //employ wait strategy
        }
        clock_gettime(CLOCK_THREAD_CPUTIME_ID, &end_time);
        const uint64_t elapsed_nanos =
                ((end_time.tv_sec - start_time.tv_sec) * 1000000000) + (end_time.tv_nsec - start_time.tv_nsec);
        const uint64_t tpt = (messages * 1000L) / elapsed_nanos;

        printf("%ld ops/sec %ld/%ld failed tries\n", tpt, total_try - messages, (uint64_t) messages);
    }
    return NULL;
}

inline static bool on_message(const uint32_t msg_type_id, const uint8_t *buffer, const index_t msg_content_index,
                              const index_t msg_content_length, void *context) {
    int64_t *expected_content = (int64_t *) context;
    const uint64_t expected_msg_content = *expected_content;
    const uint64_t *msg_content_address = (uint64_t *) (buffer + msg_content_index);
    const uint64_t msg_content = *msg_content_address;
    if (msg_type_id != DEFAULT_MSG_TYPE_ID || expected_msg_content != msg_content ||
        msg_content_length != DEFAULT_MSG_LENGTH) {
        *expected_content = -1;
        return false;
    }
    //change next expected content!
    const uint64_t next_expected_content = expected_msg_content + 1;
    *expected_content = next_expected_content;
    if (next_expected_content < 0) {
        return false;
    }
    return true;
}

void *consumer(void *arg) {
    struct ring_buffer_test *test = (struct ring_buffer_test *) arg;
    struct ring_buffer_header *header = test->header;
    uint8_t *buffer = test->buffer;
    const uint32_t tests = test->tests;
    const uint32_t messages = test->messages;
    const uint32_t batch_size = (header->capacity) / required_record_capacity(DEFAULT_MSG_LENGTH);
    const int64_t total_messages = tests * messages;
    bool (*message_consumer)(const uint32_t, const uint8_t *,
                             const index_t,
                             const index_t, void *) = &on_message;
    int64_t read_messages = 0;
    int64_t expected_content = 1;
    uint64_t failed_read = 0;
    while (read_messages < total_messages && expected_content > 0) {
        const uint32_t read = ring_buffer_batch_read(header, buffer, message_consumer, batch_size, &expected_content);
        if (read == 0) {
            __asm__ __volatile__("pause;");
            failed_read++;
        } else {
            read_messages += read;
        }
    }
    if (expected_content < 0) {
        printf("read %ld messages instead of %ld!", read_messages, total_messages);
    } else {
        printf("%ld/%ld failed reads\n", failed_read, total_messages);
    }

    return NULL;
}

int main() {
    struct ring_buffer_header header;
    const index_t buffer_capacity = ring_buffer_capacity(64 * 1024 * required_record_capacity(RECORD_HEADER_LENGTH));
    if (!init_ring_buffer_header(&header, buffer_capacity)) {
        return 1;
    }
    uint8_t buffer[buffer_capacity];
    //on the stack need memset!!!
    memset(buffer, 0, buffer_capacity);

    pthread_t producer_processor;
    pthread_t consumer_processor;

    struct ring_buffer_test test;
    test.buffer = buffer;
    test.header = &header;
    test.messages = 10000000;
    test.tests = 5;
    pthread_create(&consumer_processor, NULL, consumer, &test);
    pthread_create(&producer_processor, NULL, producer, &test);

    pthread_join(producer_processor, NULL);
    pthread_join(consumer_processor, NULL);

}