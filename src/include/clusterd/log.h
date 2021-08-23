#ifndef __clusterd_log_H__
#define __clusterd_log_H__

#include <errno.h>
#include <stdio.h>
#include <stdint.h>

#define VA_ARGS(...) , ##__VA_ARGS__
#define CLUSTERD_LOG(lvl, fmt, ...) do {                                \
    int serrno_ ## __LINE__ = errno;                                    \
    if ( lvl < CLUSTERD_LOG_LEVEL) break;                               \
    fprintf(stderr, "[" CLUSTERD_COMPONENT "] " fmt "\n"                \
            VA_ARGS(__VA_ARGS__));                                      \
    errno = serrno_ ## __LINE__;                                        \
  } while (0)

#define CLUSTERD_CRIT     100
#define CLUSTERD_ERROR    80
#define CLUSTERD_WARNING  70
#define CLUSTERD_INFO     60
#define CLUSTERD_DEBUG    50

#define CLUSTERD_LOG_HEXDUMP(lvl, buf, sz)      \
  if ( lvl >= CLUSTERD_LOG_LEVEL ) {             \
    clusterd_hexdump(stderr, buf, sz);          \
  }

static inline void clusterd_hexdump(FILE *f, void *p, size_t sz) {
  uint8_t *b = (uint8_t *)p;
  size_t i = 0;

  for ( i = 0; i < sz; ++i ) {
    if ( i > 0 && (i % 16) == 0 ) {
      fprintf(f, "\n");
    }

    fprintf(f, "%02x ", b[i]);
  }

  fprintf(f, "\n");
}

#endif
