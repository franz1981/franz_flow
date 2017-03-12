//
// Created by forked_franz on 27/02/17.
//

#include <stdio.h>
#include <inttypes.h>
#include <pthread.h>
#include <stdlib.h>
#include "vs_rb.h"
#include "vs_rb.c"
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/mman.h>

#define DEFAULT_MSG_TYPE_ID 1
#define DEFAULT_MSG_LENGTH 8

int main() {
    struct vs_rb_t header;


    char *mmap_bytes;
    int fd;
    char *file_name = "/dev/shm/shared.ipc";

    fd = open(file_name, O_RDWR, (mode_t) 0600);
    if (fd == -1) {
        perror("open");
        return 1;
    }

    struct stat st;
    stat(file_name, &st);

    const index_t buffer_capacity = st.st_size;
    if (!new_vs_rb(&header, buffer_capacity)) {
        return 1;
    }

    mmap_bytes = mmap(0, buffer_capacity, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    if (mmap_bytes == MAP_FAILED) {
        perror("mmap");
        return 1;
    }

    if (close(fd) == -1) {
        perror("close");
        return 1;
    }

    const pthread_t thread_id = pthread_self();

    uint8_t *buffer = (uint8_t *) mmap_bytes;

    const uint64_t tests = 10;
    const uint64_t messages = 100000000;

    uint64_t claimed_position = 0;
    index_t claimed_index = 0;
    uint64_t msg_content = 0;
    for (uint64_t t = 0; t < tests; t++) {
        struct timespec start_time;
        struct timespec end_time;
        uint64_t total_try = 0;
        clock_gettime(CLOCK_THREAD_CPUTIME_ID, &start_time);
        for (uint64_t m = 0; m < messages; m++) {
            while (!vs_rb_try_sp_claim(&header, buffer, DEFAULT_MSG_LENGTH, &claimed_position, &claimed_index)) {
                __asm__ __volatile__("pause;");
                total_try++;
                //wait strategy
            }
            total_try++;
            uint64_t *content_offset = (uint64_t *) (buffer + vs_rb_encoded_msg_offset(claimed_index));
            *content_offset = msg_content + 1;
            vs_rb_commit_claim(buffer, claimed_index, DEFAULT_MSG_TYPE_ID, DEFAULT_MSG_LENGTH);
            msg_content++;
        }
        //wait until the last message is consumed
        const uint64_t last_claimed_position = claimed_position;
        while (load_acquire_consumer_position(&header, buffer) <= last_claimed_position) {
            __asm__ __volatile__("pause;");
            //employ wait strategy
        }
        clock_gettime(CLOCK_THREAD_CPUTIME_ID, &end_time);
        const uint64_t elapsed_nanos =
                ((end_time.tv_sec - start_time.tv_sec) * 1000000000) + (end_time.tv_nsec - start_time.tv_nsec);
        const uint64_t tpt = (messages * 1000L) / elapsed_nanos;

        printf("[%ld]\t%ldM ops/sec %ld/%ld failed tries\n", thread_id, tpt, total_try - messages, (uint64_t) messages);
    }

    if (munmap(mmap_bytes, buffer_capacity) == -1) {
        perror("munmap");
        return 1;
    }
    return 0;
}