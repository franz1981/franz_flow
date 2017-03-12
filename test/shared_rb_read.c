//
// Created by forked_franz on 26/02/17.
//

#include <stdio.h>
#include <inttypes.h>
#include <stdlib.h>
#include "vs_rb.h"
#include "vs_rb.c"
#include <fcntl.h>
#include <unistd.h>
#include <sys/mman.h>

#define DEFAULT_MSG_TYPE_ID 1
#define DEFAULT_MSG_LENGTH 8

inline static bool on_message(const uint32_t msg_type_id, const uint8_t *buffer, const index_t msg_content_index,
                              const index_t msg_content_length, void *context) {
    int64_t *expected_content = (int64_t *) context;
    const uint64_t expected_msg_content = *expected_content;
    if (msg_type_id != DEFAULT_MSG_TYPE_ID || msg_content_length != DEFAULT_MSG_LENGTH) {
        const uint64_t *msg_content_address = (uint64_t *) (buffer + msg_content_index);
        const uint64_t msg_content = *msg_content_address;
        if (expected_msg_content != msg_content) {
            *expected_content = -1;
            return false;
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

int main() {
    struct vs_rb_t header;
    const index_t buffer_capacity = vs_rb_capacity(128 * 1024 * vs_rb_required_record_capacity(DEFAULT_MSG_LENGTH));
    if (!new_vs_rb(&header, buffer_capacity)) {
        return 1;
    }

    char *mmap_bytes;
    int fd;
    char *file_name = "/dev/shm/shared.ipc";

    fd = open(file_name, O_RDWR | O_CREAT | O_TRUNC, (mode_t) 0600);
    if (fd == -1) {
        perror("open");
        return 1;
    }
    /* Stretch the file size to the size of the (mmapped) array of ints
 */
    int result = lseek(fd, buffer_capacity - 1, SEEK_SET);
    if (result == -1) {
        close(fd);
        perror("Error calling lseek() to 'stretch' the file");
        exit(EXIT_FAILURE);
    }

    /* Something needs to be written at the end of the file to
     * have the file actually have the new size.
     * Just writing an empty string at the current file position will do.
     *
     * Note:
     *  - The current position in the file is at the end of the stretched
     *    file due to the call to lseek().
     *  - An empty string is actually a single '\0' character, so a zero-byte
     *    will be written at the last byte of the file.
     */
    result = write(fd, "", 1);

    mmap_bytes = mmap(0, buffer_capacity, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    if (mmap_bytes == MAP_FAILED) {
        perror("mmap");
        return 1;
    }

    if (close(fd) == -1) {
        perror("close");
        return 1;
    }

    uint8_t *buffer = (uint8_t *) mmap_bytes;

    const uint64_t producers = 1;
    const uint64_t tests = 10;
    const uint64_t messages = 100000000;
    const uint64_t batch_size = header.capacity / vs_rb_required_record_capacity(DEFAULT_MSG_LENGTH);
    const uint64_t total_messages = producers * tests * messages;
    const vs_rb_message_consumer consumer = &on_message;
    uint64_t read_messages = 0;
    int64_t expected_content = 1;
    uint64_t failed_read = 0;
    while (read_messages < total_messages && expected_content > 0) {
        const uint32_t read = vs_rb_read(&header, buffer, consumer, batch_size, &expected_content);
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

    if (munmap(mmap_bytes, buffer_capacity) == -1) {
        perror("munmap");
        return 1;
    }
    return 0;
}