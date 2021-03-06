#ifndef __clusterd_sha256_H__
#define __clusterd_sha256_H__

#include <stdint.h>

#define SHA256_OCTET_LENGTH 32
#define SHA256_STR_LENGTH 64

void calc_sha_256(uint8_t hash[SHA256_OCTET_LENGTH], const void * input, size_t len);
void sha_256_digest(char str[SHA256_STR_LENGTH], uint8_t hash[SHA256_OCTET_LENGTH]);

#endif
