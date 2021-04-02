#ifndef __clusterd_common_H__
#define __clusterd_common_H__

#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <inttypes.h>
#include <limits.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>

#define CLUSTERD_PACKED __attribute__(( packed ))

#define CLUSTERD_DEFAULT_RUNTIME_PATH "/var/lib/clusterd"
#define CLUSTERD_DEFAULT_IMAGE_PATH   "/var/lib/clusterd/images"
#define CLUSTERD_DEFAULT_MONITOR_PORT 20000

// By default, we expect pings every 30 seconds
#define CLUSTERD_DEFAULT_PING_INTERVAL 30000

// Pings that come in 5 seconds later than expected are considered on
// time enough. This lets us add some jitter.
#define CLUSTERD_DEFAULT_PING_GRACE_PERIOD 5000

#define NS_PER_SEC 1000000000

typedef uint32_t clusterd_namespace_t;
typedef uint32_t clusterd_service_t;
typedef uint32_t clusterd_pid_t;

#define NS_F   "%u"
#define SVC_F  "%u"
#define PID_F  "%u"

#define CLUSTERD_NTOH_NAMESPACE(ns) be64toh(ns)
#define CLUSTERD_HTON_NAMESPACE(ns) htobe64(ns)
#define CLUSTERD_NTOH_SERVICE(s)    be64toh(s)
#define CLUSTERD_HTON_SERVICE(s)    htobe64(s)
#define CLUSTERD_NTOH_PROCESS(i)    ntohl(i)
#define CLUSTERD_HTON_PROCESS(i)    htonl(i)

#define MKSTR(x) XSTR(x)
#define XSTR(x) #x

#define CLUSTERD_ADDRSTRLEN 128
#define CLUSTERD_ADDR_LENGTH(addr) (((struct sockaddr *)(addr))->sa_family == AF_INET ? sizeof(struct sockaddr_in) : sizeof(struct sockaddr_in6))

#define CLUSTERD_STRUCT_FROM_FIELD(ty, field, p) ((ty *)(((uintptr_t) (p)) - offsetof(ty, field)))

static inline int clusterd_addrcmp(struct sockaddr_storage *a, struct sockaddr_storage *b) {
  struct sockaddr_in *asin, *bsin;
  struct sockaddr_in6 *asin6, *bsin6;

  if ( a->ss_family != b->ss_family ) return -1;

  asin = (struct sockaddr_in *)a;
  bsin = (struct sockaddr_in *)b;

  asin6 = (struct sockaddr_in6 *)a;
  bsin6 = (struct sockaddr_in6 *)b;

  switch ( a->ss_family ) {
  case AF_INET:
    if ( asin->sin_addr.s_addr == bsin->sin_addr.s_addr &&
         asin->sin_port == bsin->sin_port )
      return 0;
    else
      return -1;

  case AF_INET6:
    if ( memcmp(asin6->sin6_addr.s6_addr, bsin6->sin6_addr.s6_addr, 16) == 0 &&
         asin6->sin6_port == bsin6->sin6_port )
      return 0;
    else
      return -1;

  default:
    return -1;
  }
}

static inline void clusterd_addr_render(char *addrstr, const struct sockaddr *addr, int include_port) {
  switch ( addr->sa_family ) {
  case AF_INET:
    inet_ntop(AF_INET, &((struct sockaddr_in *)addr)->sin_addr, addrstr, CLUSTERD_ADDRSTRLEN);
    if ( include_port ) {
      sprintf(addrstr + strlen(addrstr), ":%u", ntohs(((struct sockaddr_in *)addr)->sin_port));
    }
    break;

  case AF_INET6:
    if ( include_port ) {
      addrstr[0] = '[';
      inet_ntop(AF_INET6, &((struct sockaddr_in6 *)addr)->sin6_addr, addrstr + 1, CLUSTERD_ADDRSTRLEN - 1);
      sprintf(addrstr + strlen(addrstr), "]:%u", ntohs(((struct sockaddr_in6 *)addr)->sin6_port));
    } else {
      inet_ntop(AF_INET6, &((struct sockaddr_in6 *)addr)->sin6_addr, addrstr, CLUSTERD_ADDRSTRLEN);
    }
    break;

  default:
    sprintf(addrstr, "unknown");
  }
}

static inline int clusterd_addr_parse(char *addrstr, struct sockaddr_storage *ss, int include_port) {
  int port, family;
  char *addrend, *endptr;
  uint16_t *portptr;
  int err;

  ss->ss_family = AF_UNSPEC;

  if ( *addrstr == '[' ) {
    struct sockaddr_in6 *sin6 = (struct sockaddr_in6 *)ss;

    addrstr ++;
    addrend = strchr(addrstr, ']');
    if ( !addrend ) {
      errno = EINVAL;
      return -1;
    }

    *addrend = '\0';

    err = inet_pton(AF_INET6, addrstr, sin6->sin6_addr.s6_addr);
    *addrend = ']';
    if ( err == 0 ) {
      errno = EINVAL;
      return -1;
    }

    addrend++;
    if ( *addrend != ':' ) {
      if ( include_port ) {
        errno = EINVAL;
        return -1;
      }
    }

    addrend++;

    family = AF_INET6;
    portptr = &sin6->sin6_port;
  } else {
    struct sockaddr_in *sin = (struct sockaddr_in *)ss;
    char old_addrend;

    addrend = strchr(addrstr, ':');
    if ( !addrend ) {
      if ( include_port ) {
        errno = EINVAL;
        return -1;
      } else
        addrend = addrstr + strlen(addrstr);
    }

    old_addrend = *addrend;
    *addrend = '\0';

    err = inet_pton(AF_INET, addrstr, &sin->sin_addr.s_addr);
    *addrend = old_addrend;
    if ( err == 0 ) {
      errno = EINVAL;
      return -1;
    }

    family = AF_INET;
    portptr = &sin->sin_port;

    if ( old_addrend == ':' )
      addrend++;
  }

  if ( include_port ) {
    errno = 0;
    port = strtol(addrend, &endptr, 10);
    if ( errno != 0 ) {
      errno = EINVAL;
      return -1;
    }

    if ( port < 0 || port > 65535 ) {
      errno = EINVAL;
      return -1;
    }

    *portptr = htons(port);
  } else
    endptr = addrend;

  if ( *endptr != '\0' ) {
    errno = EINVAL;
    return -1;
  }

  ss->ss_family = family;

  return 0;
}

static inline int mkdir_recursive(const char *path) {
  char lpath[PATH_MAX], dir_path[PATH_MAX];
  char *cur_path, *saveptr, *cur_comp;
  int err;

  err = snprintf(lpath, sizeof(lpath), "%s", path);
  if ( err >= sizeof(lpath) ) {
    errno = ENAMETOOLONG;
    return -1;
  }

  dir_path[0] = '\0';
  if ( lpath[0] == '/' ) {
    dir_path[0] = '/';
    dir_path[1] = '\0';
  }

  for ( cur_path = lpath; (cur_comp = strtok_r(cur_path, "/", &saveptr)); cur_path = NULL ) {
    size_t dir_sz = strlen(dir_path);
    if ( dir_sz >= sizeof(dir_path) ) {
      errno = ENOSPC;
      return -1;
    }

    strncat(dir_path, cur_comp, sizeof(dir_path) - dir_sz - 1);

    dir_sz = strlen(dir_path);
    if ( dir_sz >= sizeof(dir_path) ) {
      errno = ENOSPC;
      return -1;
    }

    dir_path[dir_sz++] = '/';
    dir_path[dir_sz] = '\0';

    err = mkdir(dir_path, 0755);
    if ( err < 0 && errno != EEXIST )
      return -1;
  }

  return 0;
}

/**
 * Get the clusterd configuration directory, i.e. /etc/clusterd or
 * the CLUSTERD_CONFIG_DIR environment variable if set
 */
static inline const char *clusterd_get_config_dir() {
  const char *homedir;

  homedir = getenv("CLUSTERD_CONFIG_DIR");
  if ( !homedir )
    homedir = "/etc/clusterd";

  return homedir;
}

/**
 * Get the hostname of this clusterd node
 */
static inline const char *clusterd_hostname() {
  static int got_hostname = 0;
  static char hostname[HOST_NAME_MAX];
  const char *name;

  if ( got_hostname ) return hostname;

  name = getenv("CLUSTERD_HOSTNAME");
  if ( name ) {
    if ( strlen(name) >= sizeof(hostname) ) {
      return NULL;
    }

    got_hostname = 1;
    strcpy(hostname, name);
    return hostname;
  }

  if ( gethostname(hostname, sizeof(hostname)) < 0 ) {
    return NULL;
  }

  got_hostname = 1;
  return hostname;
}

/**
 * Get the clusterd runtime directory, i.e. /var/lib/clusterd or
 * CLUSTERD_RUNTIME_DIR
 */
static inline const char *clusterd_get_runtime_dir() {
  const char *homedir;

  homedir = getenv("CLUSTERD_RUNTIME_DIR");
  if ( !homedir )
    homedir = "/var/lib/clusterd";

  return homedir;
}

static inline void timespec_add_ns(struct timespec *ts, long int ns) {
  ts->tv_sec += (ns / NS_PER_SEC);
  ts->tv_nsec += (ns % NS_PER_SEC);

  if ( ts->tv_nsec > NS_PER_SEC ) {
    ts->tv_nsec -= NS_PER_SEC;
    ts->tv_sec++;
  } else if ( ts->tv_nsec < 0 ) {
    ts->tv_nsec += NS_PER_SEC;
    ts->tv_sec--;
  }
}

static inline void timespec_add_ms(struct timespec *ts, long int ms) {
  timespec_add_ns(ts, ms * 1000000);
}

static inline int timespec_cmp(struct timespec *a, struct timespec *b) {
  if ( a->tv_sec == b->tv_sec ) {
    if ( a->tv_nsec == b->tv_nsec ) return 0;
    else if ( a->tv_nsec < b->tv_nsec ) return -1;
    else return 1;
  } else if ( a->tv_sec < b->tv_sec ) return -1;
  else return 1;
}

static inline void timespec_sub(struct timespec *t, struct timespec *b) {
  t->tv_sec -= b->tv_sec;
  t->tv_nsec -= b->tv_nsec;

  if ( t->tv_nsec < 0 ) {
    t->tv_nsec += NS_PER_SEC;
    t->tv_sec --;
  }
}

#endif
