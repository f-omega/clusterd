#ifndef __clusterd_log_H__
#define __clusterd_log_H__

#include <errno.h>
#include <stdio.h>

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

#endif
