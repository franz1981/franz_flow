#include <stdio.h>
#include <inttypes.h>
#include <pthread.h>
#include <stdlib.h>
#include <sys/user.h>
#include "vs_rb.h"
#include "vs_rb.c"

#define DEFAULT_MSG_TYPE_ID 1
#define DEFAULT_MSG_LENGTH 8

struct ring_buffer_test {
    struct vs_rb_t *header;
    uint8_t *buffer;
    uint64_t tests;
    uint64_t messages;
    uint64_t producers;
};

static void *single_producer(void *arg) {
    struct ring_buffer_test *test = (struct ring_buffer_test *) arg;
    struct vs_rb_t *header = test->header;
    uint8_t *buffer = test->buffer;
    const uint64_t tests = test->tests;
    const uint64_t messages = test->messages;

    uint64_t claimed_position = 0;
    index_t claimed_index = 0;
    uint64_t msg_content = 0;
    for (uint64_t t = 0; t < tests; t++) {
        struct timespec start_time;
        struct timespec end_time;
        uint64_t total_try = 0;
        clock_gettime(CLOCK_THREAD_CPUTIME_ID, &start_time);
        for (uint64_t m = 0; m < messages; m++) {
            while (!vs_rb_try_sp_claim(header, buffer, DEFAULT_MSG_LENGTH, &claimed_position, &claimed_index)) {
                __asm__ __volatile__("pause;");
                total_try++;
                //wait strategy
            }
            total_try++;
            //provides better way to perform zero copy!!!!
            uint64_t *content_offset = (uint64_t *) (buffer + vs_rb_encoded_msg_offset(claimed_index));
            *content_offset = msg_content + 1;
            vs_rb_commit_claim(buffer, claimed_index, DEFAULT_MSG_TYPE_ID, DEFAULT_MSG_LENGTH);
            msg_content++;
        }
        //wait until all the messages get consumed
        while (vs_rb_size(header, buffer) != 0) {
            __asm__ __volatile__("pause;");
            //employ wait strategy
        }
        clock_gettime(CLOCK_THREAD_CPUTIME_ID, &end_time);
        const uint64_t elapsed_nanos =
                ((end_time.tv_sec - start_time.tv_sec) * 1000000000) + (end_time.tv_nsec - start_time.tv_nsec);
        const uint64_t tpt = (messages * 1000L) / elapsed_nanos;

        printf("%ldM ops/sec %ld/%ld failed tries\n", tpt, total_try - messages, (uint64_t) messages);
    }
    return NULL;
}

static void *multi_producer(void *arg) {
    const pthread_t thread_id = pthread_self();
    struct ring_buffer_test *test = (struct ring_buffer_test *) arg;
    struct vs_rb_t *header = test->header;
    uint8_t *buffer = test->buffer;
    const uint64_t tests = test->tests;
    const uint64_t messages = test->messages;

    uint64_t claimed_position = 0;
    index_t claimed_index = 0;
    uint64_t msg_content = 0;
    for (uint64_t t = 0; t < tests; t++) {
        struct timespec start_time;
        struct timespec end_time;
        uint64_t total_try = 0;
        clock_gettime(CLOCK_THREAD_CPUTIME_ID, &start_time);
        for (uint64_t m = 0; m < messages; m++) {
            while (!vs_rb_try_mp_claim(header, buffer, DEFAULT_MSG_LENGTH, &claimed_position, &claimed_index)) {
                __asm__ __volatile__("pause;");
                total_try++;
                //wait strategy
            }
            total_try++;
            //provides better way to perform zero copy!!!!
            uint64_t *content_offset = (uint64_t *) (buffer + vs_rb_encoded_msg_offset(claimed_index));
            *content_offset = msg_content + 1;
            vs_rb_commit_claim(buffer, claimed_index, DEFAULT_MSG_TYPE_ID, DEFAULT_MSG_LENGTH);
            msg_content++;
        }
        //wait until the last message is consumed
        const uint64_t last_claimed_position = claimed_position;
        while (load_acquire_consumer_position(header, buffer) <= last_claimed_position) {
            __asm__ __volatile__("pause;");
            //employ wait strategy
        }
        clock_gettime(CLOCK_THREAD_CPUTIME_ID, &end_time);
        const uint64_t elapsed_nanos =
                ((end_time.tv_sec - start_time.tv_sec) * 1000000000) + (end_time.tv_nsec - start_time.tv_nsec);
        const uint64_t tpt = (messages * 1000L) / elapsed_nanos;

        printf("[%ld]\t%ldM ops/sec %ld/%ld failed tries\n", thread_id, tpt, total_try - messages, (uint64_t) messages);
    }
    return NULL;
}

#define PRODUCERS 1

inline static bool on_message(const uint32_t msg_type_id, const uint8_t *buffer, const index_t msg_content_index,
                              const index_t msg_content_length, void *context) {
    int64_t *expected_content = (int64_t *) context;
    const uint64_t expected_msg_content = *expected_content;
    if (msg_type_id != DEFAULT_MSG_TYPE_ID || msg_content_length != DEFAULT_MSG_LENGTH) {
        if (PRODUCERS == 1) {
            const uint64_t *msg_content_address = (uint64_t *) (buffer + msg_content_index);
            const uint64_t msg_content = *msg_content_address;
            if (expected_msg_content != msg_content) {
                *expected_content = -1;
                return false;
            }
        }
    }
    //change next expected content!
    const uint64_t next_expected_content = expected_msg_content + 1;
    *expected_content = next_expected_content;
    if (next_expected_content < 0) {
        return false;
    }
    return true;
}

#undef MAX_LOOK_AHEAD_STEP

static void *consumer(void *arg) {
    struct ring_buffer_test *test = (struct ring_buffer_test *) arg;
    struct vs_rb_t *header = test->header;
    uint8_t *buffer = test->buffer;
    const uint64_t producers = test->producers;
    const uint64_t tests = test->tests;
    const uint64_t messages = test->messages;
    const uint64_t batch_size = (header->capacity) / vs_rb_required_record_capacity(DEFAULT_MSG_LENGTH);
    const uint64_t total_messages = producers * tests * messages;
    const vs_rb_message_consumer consumer = &on_message;
    uint64_t read_messages = 0;
    int64_t expected_content = 1;
    uint64_t failed_read = 0;
    while (read_messages < total_messages && expected_content > 0) {
        const uint32_t read = vs_rb_read(header, buffer, consumer, batch_size, &expected_content);
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
    struct vs_rb_t header;
    const index_t buffer_capacity = vs_rb_capacity(64 * 1024 * vs_rb_required_record_capacity(DEFAULT_MSG_LENGTH));
    if (!new_vs_rb(&header, buffer_capacity)) {
        return 1;
    }
    uint8_t *buffer = aligned_alloc(PAGE_SIZE, buffer_capacity);
    printf("ALLOCATED %d bytes aligned on: %ld\n", buffer_capacity, PAGE_SIZE);
    //on the stack it will need memset!!!
    //memset(buffer, 0, buffer_capacity);
    //check alignment minumum
    const bool is_aligned = (((int64_t) buffer) & 7) == 0;
    if (!is_aligned) {
        return 1;
    }

    struct ring_buffer_test test;
    test.buffer = buffer;
    test.header = &header;
    test.messages = 1000000000;
    test.tests = 10;
    test.producers = PRODUCERS;
    pthread_t consumer_processor;
    pthread_create(&consumer_processor, NULL, consumer, &test);
    if (PRODUCERS > 1) {
        pthread_t producer_processor[PRODUCERS];
        for (int i = 0; i < PRODUCERS; i++) {
            pthread_create(&producer_processor[i], NULL, multi_producer, &test);
        }
        for (int i = 0; i < PRODUCERS; i++) {
            pthread_join(producer_processor[i], NULL);
        }
    } else {
        pthread_t producer_processor;
        pthread_create(&producer_processor, NULL, single_producer, &test);

        pthread_join(producer_processor, NULL);
    }
    pthread_join(consumer_processor, NULL);
    free(buffer);
    return 0;
}