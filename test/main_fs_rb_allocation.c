//
// Created by forked_franz on 22/03/17.
//

#include <stdio.h>
#include <inttypes.h>
#include <pthread.h>
#include <stdlib.h>
#include <sys/user.h>
#include <string.h>
#include "fs_rb.h"
#include "fs_rb.c"

//the initial pad is used to make sure to write/read 8 bytes aligned!
#define MSG_INITIAL_PAD 4
//must be enough to write at least the msg id of 8 bytes!!
#define MSG_LENGTH 1020
#define RB_MSG_LENGTH MSG_LENGTH + MSG_INITIAL_PAD

struct ring_buffer_test {
    struct fs_rb_t *header;
    uint8_t *buffer;
    uint64_t tests;
    uint64_t messages;
    uint64_t producers;
};

#define PRODUCERS 1
#define MAX_LOOKAHEAD_CLAIM 4096
#define ZERO_COPY false

void *producer(void *arg) {
    const pthread_t thread_id = pthread_self();
    struct ring_buffer_test *test = (struct ring_buffer_test *) arg;
    struct fs_rb_t *header = test->header;
    uint8_t *buffer = test->buffer;
    const uint64_t tests = test->tests;
    const uint64_t messages = test->messages;
    uint64_t msg_id = 0;
    uint8_t *message_content = NULL;
    const size_t real_msg_size = MSG_LENGTH;
    uint64_t *const msg = (uint64_t *) malloc(real_msg_size);
    for (uint64_t t = 0; t < tests; t++) {
        struct timespec start_time;
        struct timespec end_produce_time;
        struct timespec end_time;
        uint64_t total_try = 0;
        clock_gettime(CLOCK_THREAD_CPUTIME_ID, &start_time);
        for (uint64_t m = 0; m < messages; m++) {
            const uint64_t next_msg_id = msg_id + 1;
            if (!ZERO_COPY) {
                *msg = next_msg_id;
            }
            if (PRODUCERS == 1) {
                while (!try_fs_rb_sp_claim(buffer, header, MAX_LOOKAHEAD_CLAIM, &message_content)) {
                    __asm__ __volatile__("pause;");
                    total_try++;
                    //wait strategy
                }
            } else {
                while (!try_fs_rb_mp_claim(buffer, header, &message_content)) {
                    __asm__ __volatile__("pause;");
                    total_try++;
                    //wait strategy
                }
            }
            total_try++;
            if (!ZERO_COPY) {
                memcpy((void *) (message_content + MSG_INITIAL_PAD), msg, real_msg_size);
            } else {
                //no need to memset/memcpy, encode directly the value inside the ring buffer
                uint64_t *const zero_copy_msg = (uint64_t *) (message_content + MSG_INITIAL_PAD);
                *zero_copy_msg = next_msg_id;
            }
            fs_rb_commit_claim(message_content);
            msg_id = next_msg_id;
        }
        //to verify the theory of the false sharing when the consumer is too fast...
        clock_gettime(CLOCK_THREAD_CPUTIME_ID, &end_produce_time);
        //wait until all the messages get consumed
        const uint64_t producer_position = fs_rb_load_producer_position(header, buffer);
        while (fs_rb_load_consumer_position(header, buffer) < producer_position) {
            __asm__ __volatile__("pause;");
            //employ wait strategy
        }
        clock_gettime(CLOCK_THREAD_CPUTIME_ID, &end_time);
        const uint64_t wait_nanos =
                ((end_time.tv_sec - end_produce_time.tv_sec) * 1000000000) +
                (end_time.tv_nsec - end_produce_time.tv_nsec);
        const uint64_t elapsed_nanos =
                ((end_time.tv_sec - start_time.tv_sec) * 1000000000) + (end_time.tv_nsec - start_time.tv_nsec);
        const uint64_t tpt = (messages * 1000000L) / elapsed_nanos;

        printf("[%ld]\t%ldK ops/sec %ld/%ld failed tries end latency:%ld ns\n", thread_id, tpt, total_try - messages,
               (uint64_t) messages,
               wait_nanos);
    }
    free(msg);
    return NULL;
}

inline static void check_msg(const uint64_t *const msg_content) {
    const uint64_t msg = *msg_content;
    const size_t sizeOfMsg = sizeof(uint64_t);
    const uint8_t *const rest_msg = (uint8_t *) (msg_content + sizeOfMsg);
    uint64_t total_rest = 0;
    const int msg_length = (MSG_LENGTH - sizeOfMsg);
    for (int i = 0; i < msg_length; i++) {
        total_rest += (uint64_t) *rest_msg;
    }
    const uint64_t total = total_rest + msg;
    if (total != msg) {
        printf("CONSUMER ERROR!\n");
    }
}

inline static bool on_message(uint8_t *const buffer, void *const context) {
    //PAD REQUIRED TO GET 8 BYTES ALIGNED READ
    const uint64_t *msg_content_address = (uint64_t *) (buffer + MSG_INITIAL_PAD);
    check_msg(msg_content_address);
    return true;
}

void *batch_consumer(void *arg) {
    struct ring_buffer_test *test = (struct ring_buffer_test *) arg;
    struct fs_rb_t *header = test->header;
    uint8_t *buffer = test->buffer;
    const uint64_t tests = test->tests;
    const uint64_t messages = test->messages;
    const uint32_t batch_size = header->capacity / 64;
    const uint64_t total_messages = test->producers * tests * messages;
    const fs_rb_message_consumer consumer = &on_message;
    uint64_t read_messages = 0;
    uint64_t failed_read = 0;
    uint64_t success = 0;
    while (read_messages < total_messages) {
        const uint32_t read = fs_rb_read(buffer, header, consumer, batch_size,
                                         NULL);
        if (read == 0) {
            __asm__ __volatile__("pause;");
            failed_read++;
        } else {
            success++;
            read_messages += read;
        }
    }
    printf("avg batch reads:%ld %ld/%ld failed reads\n", read_messages / success, failed_read, total_messages);
    return NULL;
}

int main() {
    if (MSG_LENGTH < sizeof(uint64_t)) {
        return -1;
    }
    const index_t requested_capacity = 64 * 1024;

    const index_t buffer_capacity = fs_rb_capacity(requested_capacity, RB_MSG_LENGTH);

    uint8_t *buffer = aligned_alloc(PAGE_SIZE, buffer_capacity);
    printf("ALLOCATED %d bytes aligned on: %ld\n", buffer_capacity, PAGE_SIZE);

    struct fs_rb_t header;
    if (!new_fs_rb(buffer, &header, requested_capacity, RB_MSG_LENGTH)) {
        return 1;
    }

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
    test.messages = 1000000;
    test.tests = 10;
    test.producers = PRODUCERS;
    pthread_t consumer_processor;
    pthread_create(&consumer_processor, NULL, batch_consumer, &test);
    pthread_t producer_processor[PRODUCERS];
    for (int i = 0; i < PRODUCERS; i++) {
        pthread_create(&producer_processor[i], NULL, producer, &test);
    }
    for (int i = 0; i < PRODUCERS; i++) {
        pthread_join(producer_processor[i], NULL);
    }
    pthread_join(consumer_processor, NULL);
    free(buffer);
    return 0;
}