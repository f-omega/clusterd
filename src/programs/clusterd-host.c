/*
 * Copyright (c) 2021 F Omega Enterprises, LLC
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

#define CLUSTERD_COMPONENT "clusterd-host"
#include "clusterd/log.h"
#include "clusterd/common.h"
#include "clusterd/request.h"
#include "config.h"
#include "libclusterctl.h"

#include <string.h>
#include <time.h>
#include <dirent.h>
#include <fcntl.h>
#include <errno.h>
#include <locale.h>
#include <getopt.h>
#include <stdlib.h>
#include <sys/wait.h>
#include <sys/prctl.h>
#include <unistd.h>

#define LOG_CHILD_STATUS(lvl, ec, what)                                  \
  do {                                                                  \
    if ( WIFEXITED(ec) ) {                                              \
      CLUSTERD_LOG(lvl, what " exited with code %d", WEXITSTATUS(ec)); \
    } else if ( WIFSIGNALED(ec) ) {                                     \
      CLUSTERD_LOG(lvl, what " killed by signal %d", WTERMSIG(ec)); \
    } else {                                                        \
      CLUSTERD_LOG(lvl, what " exited for unknown reason");         \
    }                                                               \
  } while ( 0 )

#define MONITOR_COOKIE_LENGTH 64

#define DEFAULT_SIGTERM_GRACE 10000
#define DEFAULT_SIGQUIT_GRACE  3000

typedef enum
  {
   MONITOR_WAITING,
   MONITOR_HEARTBEAT_SENT,

   /* Tried to send the heartbeat, but it was blocked */
   MONITOR_HEARTBEAT_SEND_PENDING
  } monitor_state;

typedef enum
  {
   PROCESS_PREPARING, /* System is preparing to run process */
   PROCESS_STARTED,   /* Process has started and is running */
   PROCESS_FAILURE,   /* Process has failed, failure script running */
   PROCESS_RECOVERED,  /* Failure script has run, but we're in the mandatory cooloff period before restarts */
   PROCESS_DYING,      /* Process has been signaledto quit and we're in grace period */
   PROCESS_COMPLETE    /* Process is done */
  } ps_state;

typedef struct monitor {
  struct sockaddr_storage addr;
  socklen_t addrlen;

  int local_sk;
  struct timespec next_hb;

  // Count of consecutive failures in pinging the monitor
  int failures;

  monitor_state state;

  char random_cookie[MONITOR_COOKIE_LENGTH];

  struct monitor *next;
} monitor;

int CLUSTERD_LOG_LEVEL = CLUSTERD_INFO;

ps_state g_state = PROCESS_PREPARING;
struct timespec g_next_start;
struct timespec g_next_kill;
monitor *g_monitors = NULL;
char g_ps_path[PATH_MAX] = "";

clusterd_namespace_t  g_nsid = 0;
clusterd_service_t    g_sid  = 0;
clusterd_pid_t        g_pid  = 0;
sig_atomic_t   g_down_signal = -1;

int                g_socket4 = -1;
int                g_socket6 = -1;
int             g_max_socket = 0;
int       g_max_mon_failures = 3;
int      g_cooloff_period_ms = 1000;

uid_t         g_ns_uid_lower = 0;
unsigned int  g_ns_uid_count = 0;

uid_t             g_root_uid = 0;

gid_t          g_clusterd_gid = 0;

unsigned int g_ping_interval = CLUSTERD_DEFAULT_PING_INTERVAL;
FILE *g_urandom = NULL;

const char *get_service_path_lua =
  "ns_id = clusterd.resolve_namespace(params.namespace)\n"
  "if ns_id == nil then\n"
  "  error('namespace ' .. params.namespace .. ' does not exist')\n"
  "end\n"
  "s_id = clusterd.resolve_service(ns_id, params.service)\n"
  "if s_id == nil then\n"
  "  error('service ' .. params.service .. ' does not exist')\n"
  "end\n"
  "svc = clusterd.get_service(ns_id, s_id)\n"
  "clusterd.output(ns_id)\n"
  "clusterd.output(s_id)\n"
  "clusterd.output(svc.s_path)\n";

static void usage() {
  fprintf(stderr, "clusterd-host - supervise a clusterd process\n");
  fprintf(stderr, "Usage: clusterd-host -vh [-n NAMESPACE] [-m MONITOR...] -p PID -i INTERVAL\n");
  fprintf(stderr, "         SERVICEID args...\n\n");
  fprintf(stderr, "   -n NAMESPACE   Execute the service in the given namespace.\n");
  fprintf(stderr, "                  If not specified, defaults to the default namespace.\n");
  fprintf(stderr, "   -m MONITOR     Specify one or more monitor nodes. If none\n");
  fprintf(stderr, "                  specified, the service will not be restarted\n");
  fprintf(stderr, "                  if this node fails\n");
  fprintf(stderr, "   -i INTERVAL    How often (in seconds) to send a monitor request\n");
  fprintf(stderr, "   -p PID         Clusterd process ID for this host\n");
  fprintf(stderr, "   -v             Display verbose debug output\n");
  fprintf(stderr, "   -h             Show this help menu\n");
  fprintf(stderr, "   SERVICEID      The name or ID of the service to start\n\n");
  fprintf(stderr, "All arguments after the service ID are passed directly to the service run script\n\n");
  fprintf(stderr, "Please reports bugs to support@f-omega.com\n");
}

static int parse_service_details(char *info, clusterd_namespace_t *ns, clusterd_service_t *svc,
                                 char *path, size_t pathsz) {
  char *cur, *nl, *dend;
  size_t len;

  cur = info;

  /* Namespace */
  nl = strchr(cur, '\n');
  if ( !nl ) {
    CLUSTERD_LOG(CLUSTERD_ERROR, "Could not find namespace id in controller return");
    errno = EPROTO;
    return -1;
  }

  *nl = '\0';
  errno = 0;
  *ns = strtoll(cur, &dend, 10);
  if ( errno != 0 ) {
    CLUSTERD_LOG(CLUSTERD_ERROR, "Could not parse namespace id: %s", strerror(errno));
    return -1;
  }
  if ( dend != nl ) {
    CLUSTERD_LOG(CLUSTERD_ERROR, "Garbage after namespace id");
    errno = EPROTO;
    return -1;
  }

  cur = dend + 1;

  nl = strchr(cur, '\n');
  if ( !nl ) {
    CLUSTERD_LOG(CLUSTERD_ERROR, "Could not find service id in controller return");
    errno = EPROTO;
    return -1;
  }

  *nl = '\0';
  errno = 0;
  *svc = strtoll(cur, &dend, 10);
  if ( errno != 0 ) {
    CLUSTERD_LOG(CLUSTERD_ERROR, "Could not parse service id: %s", strerror(errno));
    return -1;
  }
  if ( dend != nl ) {
    CLUSTERD_LOG(CLUSTERD_ERROR, "Garbage after service id");
    errno = EPROTO;
    return -1;
  }

  cur = dend + 1;

  nl = strchr(cur, '\n');
  if ( !nl ) {
    CLUSTERD_LOG(CLUSTERD_ERROR, "Could not find service path in controller return");
    errno = EPROTO;
    return -1;
  }

  *nl = '\0';
  len = snprintf(path, pathsz, "%s", cur);
  if ( len >= pathsz ) {
    errno = ENAMETOOLONG;
    return -1;
  }

  return 0;
}

static int make_status_pipe() {
  char pipe_path[PATH_MAX];
  int err;

  err = snprintf(pipe_path, sizeof(pipe_path), "%s/cmd", g_ps_path);
  if ( err >= sizeof(pipe_path) ) {
    CLUSTERD_LOG(CLUSTERD_CRIT, "Process command pipe would be too long");

    errno = ENAMETOOLONG;
    return -1;
  }

  err = mkfifo(pipe_path, 0400);
  if ( err < 0 ) {
    CLUSTERD_LOG(CLUSTERD_CRIT, "Could not create fifo: %s", strerror(errno));
    return -1;
  }

  // Now open the fifo in nonblock mode
  err = open(pipe_path, O_RDONLY | O_NONBLOCK | O_CLOEXEC);
  if ( err < 0 ) {
    int serrno = errno;

    CLUSTERD_LOG(CLUSTERD_CRIT, "Could not open pipe: %s", strerror(errno));

    err = unlink(pipe_path);
    if ( err < 0 ) {
      CLUSTERD_LOG(CLUSTERD_CRIT, "Could not unlink bad pipe: %s", strerror(errno));
    }

    errno = serrno;
    return -1;
  }

  return err;
}

static int rm_recursive(DIR *dir) {
  struct dirent *ent;
  DIR *lower;
  int fd, lowerfd, err;

  fd = dirfd(dir);
  if ( fd < 0 ) {
    CLUSTERD_LOG(CLUSTERD_ERROR, "Could not get fd for directory: %s", strerror(errno));
    return -1;
  }

  while ( (errno = 0, ent = readdir(dir)) ) {
    if ( strcmp(ent->d_name, ".") == 0 || strcmp(ent->d_name, "..") == 0 )
      continue;

    switch ( ent->d_type ) {
    case DT_BLK:
    case DT_CHR:
    case DT_FIFO:
    case DT_LNK:
    case DT_REG:
    case DT_SOCK:
      err = unlinkat(fd, ent->d_name, 0);
      if ( err < 0 ) {
        CLUSTERD_LOG(CLUSTERD_ERROR, "Could not remove file %s: %s", ent->d_name, strerror(errno));
        return -1;
      }
      break;

    case DT_DIR:
      lowerfd = openat(fd, ent->d_name, O_DIRECTORY | O_RDONLY);
      if ( lowerfd < 0 ) {
        CLUSTERD_LOG(CLUSTERD_ERROR, "Could not open directory %s: %s", ent->d_name, strerror(errno));
        return -1;
      }

      lower = fdopendir(lowerfd);
      if ( !lower ) {
        CLUSTERD_LOG(CLUSTERD_ERROR, "Could not fdopendir() directory %s: %s", ent->d_name, strerror(errno));
        close(lowerfd);
        return -1;
      }

      lowerfd = -1;

      err = rm_recursive(lower);
      if ( err < 0 ) {
        err = closedir(lower);
        if ( err < 0 ) {
          CLUSTERD_LOG(CLUSTERD_ERROR, "Could not close directory %s: %s", ent->d_name, strerror(errno));
        }
        return -1;
      }

      err = closedir(lower);
      if ( err < 0 ) {
        CLUSTERD_LOG(CLUSTERD_ERROR, "Could not close directory %s: %s", ent->d_name, strerror(errno));
        return -1;
      }

      err = unlinkat(fd, ent->d_name, AT_REMOVEDIR);
      if ( err < 0 ) {
        CLUSTERD_LOG(CLUSTERD_ERROR, "Could not remove directory %s: %s", ent->d_name, strerror(errno));
        return -1;
      }
      break;

    case DT_UNKNOWN:
    default:
      CLUSTERD_LOG(CLUSTERD_WARNING, "Unknown file %s", ent->d_name);
    }
  }
}

static void clean_process_directory() {
  DIR *psdir;
  int err;

  psdir = opendir(g_ps_path);
  if ( !psdir ) {
    CLUSTERD_LOG(CLUSTERD_WARNING, "Could not open() process dir: %s: %s", g_ps_path, strerror(errno));
    return;
  }

  err = rm_recursive(psdir);
  if ( err < 0 ) {
    CLUSTERD_LOG(CLUSTERD_WARNING, "Could not remove process directory: %s", strerror(errno));
    err = closedir(psdir);
    if ( err < 0 )
      CLUSTERD_LOG(CLUSTERD_WARNING, "Could not close process directory: %s", strerror(errno));
    return;
  }

  err = closedir(psdir);
  if ( err < 0 )
    CLUSTERD_LOG(CLUSTERD_WARNING, "Could not close process directory: %s", strerror(errno));

  err = rmdir(g_ps_path);
  if ( err < 0 ) {
    CLUSTERD_LOG(CLUSTERD_WARNING, "Could not remove process directory: %s", strerror(errno));
  }
}

static int check_process_exists() {
  char cmdpipepath[PATH_MAX];
  int err, cmdpipe;

  err = snprintf(cmdpipepath, sizeof(cmdpipepath), "%s/cmd", g_ps_path);
  if ( err >= sizeof(cmdpipepath) ) {
    CLUSTERD_LOG(CLUSTERD_ERROR, "Could not construct command pipe path");

    errno = ENAMETOOLONG;
    return -1;
  }

  cmdpipe = open(cmdpipepath, O_RDONLY | O_NONBLOCK | O_CLOEXEC);
  if ( cmdpipe < 0 ) {
    if ( errno == ENXIO ) {
      // Process does not exist
      return 0;
    } else {
      CLUSTERD_LOG(CLUSTERD_DEBUG, "Could not open command pipe: %s", strerror(errno));
      return -1;
    }
  } else {
    // Since we could open the pipe, the other end exists. A process is running
    close(cmdpipe);
    return 1;
  }
}

static int make_process_directory() {
  int err;
  struct stat dirstat;

  err = snprintf(g_ps_path, sizeof(g_ps_path), "%s/proc/" NS_F "/" PID_F,
                 clusterd_get_runtime_dir(), g_nsid, g_pid);
  if ( err >= sizeof(g_ps_path) ) {
    CLUSTERD_LOG(CLUSTERD_CRIT, "Process directory would be too long");

    errno = ENAMETOOLONG;
    return -1;
  }

  // Check if the path exists. If it's a directory, attempt to
  // communicate with the process inside
  err = stat(g_ps_path, &dirstat);
  if ( err < 0 ) {
    if ( errno != ENOENT ) {
      CLUSTERD_LOG(CLUSTERD_CRIT, "Could not stat process directory %s: %s",
                   g_ps_path, strerror(errno));
      return -1;
    }
  } else {
    // Stat was successful. If it's a directory, we can continue,
    // otherwise it's a critical error.
    if ( dirstat.st_mode & S_IFDIR ) {
      // This may be a process directory, attempt to communicate
      err = check_process_exists();
      if ( err < 0 ) {
        CLUSTERD_LOG(CLUSTERD_CRIT, "Could not determine if existing process directory contains a process: %s",
                     strerror(errno));
        return -1;
      } else if ( err > 0 ) {
        // This contains a process, and it's running
        CLUSTERD_LOG(CLUSTERD_DEBUG, "This process already exists!");
        errno = EEXIST;
        return -1;
      }
    } else {
      CLUSTERD_LOG(CLUSTERD_CRIT, "Process directory %s already exists and is not a directory: %s",
                   g_ps_path, strerror(errno));
      return -1;
    }
  }

  err = mkdir_recursive(g_ps_path);
  if ( err < 0 ) {
    CLUSTERD_LOG(CLUSTERD_CRIT, "Could not create %s: %s", g_ps_path, strerror(errno));
    return -1;
  }

  err = chdir(g_ps_path);
  if ( err < 0 ) {
    CLUSTERD_LOG(CLUSTERD_CRIT, "Could not set process working dir: %s", strerror(errno));
    return -1;
  }

  err = chown(g_ps_path, g_root_uid, g_clusterd_gid);
  if ( err < 0 ) {
    CLUSTERD_LOG(CLUSTERD_CRIT, "Could not set process directory owner: %s", strerror(errno));
    return -1;
  }

  return 0;
}

static int open_monitor_socket(int family) {
  int sk;

  sk = socket(family, SOCK_NONBLOCK | SOCK_DGRAM, IPPROTO_UDP);
  if ( sk < 0 ) {
    CLUSTERD_LOG(CLUSTERD_ERROR, "Could not open socket: %s", strerror(errno));
    return -1;
  }

  return sk;
}

static int reset_monitor_timer(monitor *m) {
  int err, i = 0;
  uint64_t interval, r = 0;

  err = clock_gettime(CLOCK_MONOTONIC, &m->next_hb);
  if ( err < 0 ) {
    CLUSTERD_LOG(CLUSTERD_ERROR, "Could not reset monitor time: %s", strerror(errno));
    return -1;
  }

  interval = g_ping_interval - CLUSTERD_DEFAULT_PING_GRACE_PERIOD / 2;

  for ( i = 0; i < 100; ++i ) {
    r += random() % (CLUSTERD_DEFAULT_PING_GRACE_PERIOD * 2);
  }

  r /= 100;
  interval += r;

  timespec_add_ms(&m->next_hb, interval);

  return 0;
}

static int add_monitor(char *addrstr) {
  int err;
  int *sk;
  struct sockaddr_storage addr;
  socklen_t addrlen;
  monitor *m;

  // TODO use getaddrinfo

  err = clusterd_addr_parse(addrstr, &addr, 1);
  if ( err < 0 ) {
    CLUSTERD_LOG(CLUSTERD_ERROR, "Invalid monitor address: %s", addrstr);
    errno = EINVAL;
    return -1;
  }

  switch ( addr.ss_family ) {
  case AF_INET:
    sk = &g_socket4;
    addrlen = sizeof(struct sockaddr_in);
    break;

  case AF_INET6:
    sk = &g_socket6;
    addrlen = sizeof(struct sockaddr_in6);
    break;

  default:
    CLUSTERD_LOG(CLUSTERD_ERROR, "Invalid monitor address family: %d", addr.ss_family);

    errno = EINVAL;
    return -1;
  }

  m = malloc(sizeof(monitor));
  if ( !m ) {
    CLUSTERD_LOG(CLUSTERD_CRIT, "Could not create monitor: out of memory");
    errno = ENOMEM;
    return -1;
  }

  memcpy(&m->addr, &addr, sizeof(struct sockaddr_storage));
  m->addrlen = addrlen;

  err = clock_gettime(CLOCK_MONOTONIC, &m->next_hb);
  if ( err < 0 ) {
    CLUSTERD_LOG(CLUSTERD_ERROR, "Could not set monitor heartbeat: %s", strerror(errno));
    return -1;
  }

  m->next = g_monitors;
  g_monitors = m;

  if ( *sk < 0 ) {
    err = open_monitor_socket(addr.ss_family);
    if ( err < 0 ) {
      CLUSTERD_LOG(CLUSTERD_ERROR, "Could not open monitor socket: %s", strerror(errno));
      return -1;
    }

    *sk = err;
  }

  m->local_sk = *sk;

  return 0;
}

static void start_process_doctor() {
  CLUSTERD_LOG(CLUSTERD_CRIT, "TODO process doctor not implemented\n");
  exit(100);
}

static int drop_privileges() {
  int err;

  err = setgid(g_clusterd_gid);
  if  ( err < 0 ) {
    CLUSTERD_LOG(CLUSTERD_ERROR, "Could not setgid(%u): %s", g_clusterd_gid, strerror(errno));
    return -1;
  }

  err = setuid(g_root_uid);
  if ( err < 0 ) {
    CLUSTERD_LOG(CLUSTERD_ERROR, "Could not setuid(%u): %s", g_root_uid, strerror(errno));
    return -1;
  }

  return 0;
}

static int download_image(const char *image, char *realpath, size_t realpathsz) {
  char dlimage[PATH_MAX];
  pid_t child;
  const char *imgdir;
  int err;

  if ( realpathsz == 0 ) {
    errno = EINVAL;
    return -1;
  }

  err = snprintf(dlimage, sizeof(dlimage), "%s/dlimage", clusterd_get_config_dir());
  if ( err >= sizeof(dlimage) ) {
    errno = ENAMETOOLONG;
    return -1;
  }

  err = snprintf(realpath, realpathsz, "%s/image", g_ps_path);
  if ( err >= realpathsz ) {
    errno = ENAMETOOLONG;
    return -1;
  }

  CLUSTERD_LOG(CLUSTERD_DEBUG, "Downloading image %s to %s using %s", image, realpath, dlimage);

  child = fork();
  if ( child < 0 ) {
    CLUSTERD_LOG(CLUSTERD_CRIT, "Could not fork to download image: %s", strerror(errno));
    return -1;
  } else if ( child == 0 ) {
    /* In the child... Execute the script via the pipe */
    close(STDIN_FILENO);

    /* Set CLUSTERD_IMAGES environment to default, but don't overwrite
     * if it exists */
    setenv("CLUSTERD_IMAGES", CLUSTERD_DEFAULT_IMAGE_PATH, 0);

    err = execl(dlimage, "dlimage", image, realpath, NULL);
    CLUSTERD_LOG(CLUSTERD_CRIT, "Could not execute dlimage script: %s", strerror(errno));
    exit(101);
  } else {
    int exitstatus;
    pid_t werr;

    werr = waitpid(child, &exitstatus, 0);
    if ( werr != child ) {
      CLUSTERD_LOG(CLUSTERD_CRIT, "Could not get dlimage status: %s", strerror(errno));
      return -1;
    }

    if ( WIFEXITED(exitstatus) &&
         WEXITSTATUS(exitstatus) != 0 ) {
      if ( WEXITSTATUS(exitstatus)
           != 101 ) {
        CLUSTERD_LOG(CLUSTERD_ERROR, "dlimage returned abnormal error code %d",
                     WEXITSTATUS(exitstatus));
      }
      errno = ECOMM;
      return -1;
    } else if ( WIFSIGNALED(exitstatus) ) {
      CLUSTERD_LOG(CLUSTERD_ERROR, "dlimage killed by signal %d",
                   WTERMSIG(exitstatus));
      errno = ECOMM;
      return -1;
    } else if ( !(WIFEXITED(exitstatus)) ) {
      CLUSTERD_LOG(CLUSTERD_ERROR, "dlimage exited abnormally");
      errno = ECOMM;
      return -1;
    }
  }

  return 0;
}

static void random_cookie(unsigned char *out, size_t len) {
  size_t bytes = 0;
  int i = 0;

  if ( g_urandom ) {
    bytes = fread(out, 1, len, g_urandom);
    if ( bytes >= len ) return;
  }

  CLUSTERD_LOG(CLUSTERD_DEBUG, "Sending potentially insecure bytes");

  for ( i = bytes; i < len; ++i ) {
    out[i] = random();
  }
}

static void monitor_failure(monitor *m) {
  m->failures ++;

  if ( m->failures > g_max_mon_failures ) {
    CLUSTERD_LOG(CLUSTERD_CRIT, "TODO: Monitor failure");
    exit(104);
  }
}

static void send_monitor_heartbeat(monitor *m) {
  ssize_t err;

  unsigned char buf[2048];
  off_t bufoffs = 0, attroffs = 0;

  clusterd_attr *attr;

  clusterd_namespace_t n_nsid = CLUSTERD_HTON_NAMESPACE(g_nsid);
  clusterd_service_t   n_sid  = CLUSTERD_HTON_SERVICE(g_sid);
  clusterd_pid_t       n_pid  = CLUSTERD_HTON_PROCESS(g_pid);
  uint32_t          interval  = htonl(g_ping_interval);

  monitor *other;

  if ( m->state == MONITOR_WAITING ) {
    /* Choose new cookie */
    random_cookie(m->random_cookie, MONITOR_COOKIE_LENGTH);
  }

  CLUSTERD_INIT_REQ(buf, bufoffs, sizeof(buf), CLUSTERD_OP_MONITOR);

  CLUSTERD_ADD_ATTR(buf, bufoffs, attroffs, CLUSTERD_ATTR_NAMESPACE);
  CLUSTERD_WRITE_ATTR(buf, bufoffs, &n_nsid, sizeof(n_nsid));

  CLUSTERD_ADD_ATTR(buf, bufoffs, attroffs, CLUSTERD_ATTR_SERVICE);
  CLUSTERD_WRITE_ATTR(buf, bufoffs, &n_sid, sizeof(n_sid));

  CLUSTERD_ADD_ATTR(buf, bufoffs, attroffs, CLUSTERD_ATTR_PROCESS);
  CLUSTERD_WRITE_ATTR(buf, bufoffs, &n_pid, sizeof(n_pid));

  CLUSTERD_ADD_ATTR(buf, bufoffs, attroffs, CLUSTERD_ATTR_INTERVAL);
  CLUSTERD_WRITE_ATTR(buf, bufoffs, &interval, sizeof(interval));

  CLUSTERD_ADD_ATTR(buf, bufoffs, attroffs, CLUSTERD_ATTR_COOKIE);
  CLUSTERD_WRITE_ATTR(buf, bufoffs, m->random_cookie, MONITOR_COOKIE_LENGTH);

  // Now add information for each monitor
  for ( other = g_monitors; other; other = other->next ) {
    switch ( other->addr.ss_family ) {
    case AF_INET:
      do {
        clusterd_ip4_attr ip4;
        struct sockaddr_in *sin = (struct sockaddr_in *)&other->addr;

        ip4.ip4_port = sin->sin_port;
        memcpy(&ip4.ip4_addr, &sin->sin_addr.s_addr, sizeof(ip4.ip4_addr));

        CLUSTERD_ADD_ATTR(buf, bufoffs, attroffs, CLUSTERD_ATTR_MONITOR_V4);
        CLUSTERD_WRITE_ATTR(buf, bufoffs, &ip4, sizeof(ip4));
      } while ( 0 );
      break;

    case AF_INET6:
      do {
        clusterd_ip6_attr ip6;
        struct sockaddr_in6 *sin6 = (struct sockaddr_in6 *)&other->addr;

        ip6.ip6_port = sin6->sin6_port;
        memcpy(ip6.ip6_addr, &sin6->sin6_addr.s6_addr, sizeof(ip6.ip6_addr));

        CLUSTERD_ADD_ATTR(buf, bufoffs, attroffs, CLUSTERD_ATTR_MONITOR_V6);
        CLUSTERD_WRITE_ATTR(buf, bufoffs, &ip6, sizeof(ip6));
      } while ( 0 );
      break;

    default:
      CLUSTERD_LOG(CLUSTERD_CRIT, "Monitor with invalid address family encountered: %d. Skipping",
                   other->addr.ss_family);
      break;
    }
  }

  CLUSTERD_FINISH_REQUEST(buf, bufoffs, attroffs);

  err = sendto(m->local_sk, buf, bufoffs, 0,
               (struct sockaddr *)&m->addr, m->addrlen);
  if ( err < 0 ) {
    // Do nothing. It'll be added to the 'wants write' list
    if ( errno == EAGAIN || errno == EWOULDBLOCK ) {
      m->state = MONITOR_HEARTBEAT_SEND_PENDING;
      return;
    }

    CLUSTERD_LOG(CLUSTERD_ERROR, "Could not send heartbeat to monitor: %s", strerror(err));
    monitor_failure(m);
  }

  m->state = MONITOR_HEARTBEAT_SENT;
  return;

 nospace:
  CLUSTERD_LOG(CLUSTERD_CRIT, "Could not send heartbeat to monitor: no space in message buffer");
  exit(102);
}

static int start_monitoring() {
  monitor *cur;

  for ( cur = g_monitors; cur; cur = cur->next ) {
    // Send the heartbeats out
    send_monitor_heartbeat(cur);
  }

  return 0;
}

static int exec_service(pid_t *ps, sigset_t *oldmask, char *svpath, int svargc, char *const *svargv) {
  char servicerun[PATH_MAX];
  int err, stspipe[2];
  pid_t child;

 // TODO run prepare script the first time around
  err = snprintf(servicerun, sizeof(servicerun), "%s/run", svpath);
  if ( err >= sizeof(servicerun) ) {
    errno = ENAMETOOLONG;
    return -1;
  }

  err = pipe2(stspipe, O_CLOEXEC);
  if ( err < 0 ) {
    CLUSTERD_LOG(CLUSTERD_CRIT, "Could not create status pipe: %s", strerror(errno));
    return -1;
  }

  CLUSTERD_LOG(CLUSTERD_DEBUG, "Starting service");

  child = fork();
  if ( child < 0 ) {
    CLUSTERD_LOG(CLUSTERD_CRIT, "Could not fork() to start service: %s", strerror(errno));
    return -1;
  } else if ( child == 0 ) {
    char ns_path[PATH_MAX];
    unsigned char one = 1;
    close(stspipe[0]);

    strncpy(ns_path, g_ps_path, sizeof(ns_path));
    basename(ns_path);

    setenv("CLUSTERD_NS_DIR", ns_path, 1);
    setenv("CLUSTERD_PS_DIR", g_ps_path, 1);

    err = sigprocmask(SIG_SETMASK, oldmask, NULL);
    if ( err < 0 ) {
      CLUSTERD_LOG(CLUSTERD_CRIT, "Could not reset service mask: %s", strerror(errno));
      exit(100);
    }

    // SIGTERM, SIGQUIT, SIGINT, etc are handled gracefully
    // elsewhere. This is mostly a catch-all
    err = prctl(PR_SET_PDEATHSIG, SIGKILL);
    if ( err < 0 ) {
      CLUSTERD_LOG(CLUSTERD_CRIT, "Could not set parent death signal: %s", strerror(errno));
      exit(100);
    }

    err = drop_privileges();
    if ( err < 0 ) {
      CLUSTERD_LOG(CLUSTERD_CRIT, "Could not drop privileges: %s", strerror(errno));
      exit(100);
    }

    /* Close sockets */
    close(g_socket4);
    close(g_socket6);

    err = execl(servicerun, "start", NULL);
    CLUSTERD_LOG(CLUSTERD_CRIT, "Could not start service process %s: %s", servicerun, strerror(errno));
    exit(100);
  } else {
    unsigned char one;

    close(stspipe[1]);

    CLUSTERD_LOG(CLUSTERD_DEBUG, "Child started with pid %u", child);

    err = read(stspipe[0], &one, 1);
    if ( err < 0 ) {
      int serrno = errno;
      CLUSTERD_LOG(CLUSTERD_CRIT, "Could not read status: %s", strerror(errno));
      kill(child, SIGTERM);
      errno = serrno;
      return -1;
    }

    close(stspipe[0]);

    g_state = PROCESS_STARTED;
    *ps = child;

    return 0;
  }
}

static void set_next_start_delay(int ms) {
  int err;

  err = clock_gettime(CLOCK_MONOTONIC, &g_next_start);
  if ( err < 0 ) {
    CLUSTERD_LOG(CLUSTERD_ERROR, "Could not get next start: %s", strerror(errno));
    return;
  }

  timespec_add_ms(&g_next_start, ms);
}

static void process_recovered() {
  if ( g_state == PROCESS_STARTED )
    set_next_start_delay(g_cooloff_period_ms);

  g_state = PROCESS_RECOVERED;
}

static void process_failure(pid_t *ps, int sts, sigset_t *oldmask) {
  struct stat finish_script_stat;
  const char *what;
  int code, err, stspipe[2];
  char finish_path[PATH_MAX];
  pid_t child;

  if ( CLUSTERD_LOG_LEVEL >= CLUSTERD_DEBUG ) {
    if ( WIFEXITED(sts) ) {
      what = "exited with code";
      code = WEXITSTATUS(sts);
    } else if ( WIFSIGNALED(sts) ) {
      what = "killed by signal";
      code = WTERMSIG(sts);
    } else {
      what = "unknown error";
      code = sts;
    }

    CLUSTERD_LOG(CLUSTERD_DEBUG, "Process failed: %s %d", what, code);
  }

  if ( g_state == PROCESS_FAILURE ) {
    CLUSTERD_LOG(CLUSTERD_WARNING, "Finish script exited abnormally");
    exit(101); // TODO
  }

  err = snprintf(finish_path, sizeof(finish_path), "%s/finish", g_ps_path);
  if ( err >= sizeof(finish_path) ) {
    CLUSTERD_LOG(CLUSTERD_CRIT, "Not enough space for finish command");
    return;
  }

  // Make sure the script exists before running it. If it does not,
  // then go right into PROCESS_RECOVERED
  err = stat(finish_path, &finish_script_stat);
  if ( err < 0 ) {
    if ( errno == ENOENT ) {
      CLUSTERD_LOG(CLUSTERD_INFO, "No finish script for this process image. Starting in 1s");
    } else {
      CLUSTERD_LOG(CLUSTERD_ERROR, "Could not stat %s: %s. Assuming there was no finish script",
                   finish_path, strerror(errno));
    }

    process_recovered();
    return;
  }

  err = pipe2(stspipe, O_CLOEXEC);
  if ( err < 0 ) {
    CLUSTERD_LOG(CLUSTERD_ERROR, "Could not set up status pipe for finish script: %s",
                 strerror(errno));
    process_recovered();
    return;
  }

  CLUSTERD_LOG(CLUSTERD_DEBUG, "Starting finish script %s", finish_path);

  child = fork();
  if ( child < 0 ) {
    CLUSTERD_LOG(CLUSTERD_DEBUG, "Could not start finish script: %s. Assuming recovery", strerror(errno));
    close(stspipe[0]);
    close(stspipe[1]);
    process_recovered();
    return;
  } else if ( child == 0 ) {
    char exitcodestr[20], signalstr[20];
    close(stspipe[0]);

    memset(exitcodestr, 0, sizeof(exitcodestr));
    memset(signalstr, 0, sizeof(signalstr));

    if ( WIFEXITED(sts) )
      sprintf(exitcodestr, "%d", WEXITSTATUS(sts));

    if ( WIFSIGNALED(sts) )
      sprintf(signalstr, "%d", WTERMSIG(sts));

    /* Restore the signal mask */
    err = sigprocmask(SIG_SETMASK, oldmask, NULL);
    if ( err < 0 ) {
      CLUSTERD_LOG(CLUSTERD_CRIT, "Could not set signal mask: %s", strerror(errno));
      goto send_errno;
    }

    err = prctl(PR_SET_PDEATHSIG, SIGKILL);
    if ( err < 0 ) {
      CLUSTERD_LOG(CLUSTERD_CRIT, "Could not set parent death signal: %s", strerror(errno));
      goto send_errno;
    }

    err = drop_privileges();
    if ( err < 0 ) {
      CLUSTERD_LOG(CLUSTERD_CRIT, "Could not drop privileges: %s", strerror(errno));
      goto send_errno;
    }

    execl(finish_path, "finish", exitcodestr, signalstr, NULL);

  send_errno:
    err = errno;
    err = write(stspipe[1], &err, sizeof(err));
    if ( err < 0 ) {
      CLUSTERD_LOG(CLUSTERD_ERROR, "Could not write errno: %s", strerror(errno));
    } else if ( err != sizeof(err) ) {
      CLUSTERD_LOG(CLUSTERD_ERROR, "Could not write all of errno: %s", strerror(errno));
    }
    exit(100);
  } else {
    int ec;
    pid_t werr;

    close(stspipe[1]);

    g_state = PROCESS_FAILURE;
    // Set next_start to be one second from now
    set_next_start_delay(g_cooloff_period_ms);

    CLUSTERD_LOG(CLUSTERD_DEBUG, "Started finish script with PID %u", child);

    err = read(stspipe[0], &ec, sizeof(ec));
    if ( err < 0 ) {
      CLUSTERD_LOG(CLUSTERD_DEBUG, "Could not read error: %s", strerror(errno));
      close(stspipe[0]);
    } else if ( err == 0 ) {
      CLUSTERD_LOG(CLUSTERD_DEBUG, "Finish script started successfully");
      goto done;
    } else if ( err == sizeof(ec) ) {
      CLUSTERD_LOG(CLUSTERD_DEBUG, "Finish script had error: %s", strerror(ec));
      process_recovered();
    } else {
      CLUSTERD_LOG(CLUSTERD_DEBUG, "Could not read errno from finish script");
      process_recovered();
    }

    werr = waitpid(child, &ec, 0);
    if ( werr < 0 ) {
      CLUSTERD_LOG(CLUSTERD_CRIT, "Could not wait on errored finish child: %s", strerror(errno));
    }

    LOG_CHILD_STATUS(CLUSTERD_ERROR, ec, "finish script");
    return;
  }

 done:
  *ps = child;
  CLUSTERD_LOG(CLUSTERD_DEBUG, "Waiting for finish script to end");
}

static void chld_handler(int s) {
  /* TODO if this is from a process grandchild or other descendant,
   * then reap zombies */
}

static void want_down_handler(int signal) {
  g_down_signal = signal;
}

static int setup_signals(sigset_t *mask) {
  int err;
  sigset_t all;
  struct sigaction want_down;

  err = sigfillset(&all);
  if ( err < 0 ) {
    CLUSTERD_LOG(CLUSTERD_CRIT, "Could not run sigfillset(): %s", strerror(errno));
    return -1;
  }

  err = sigprocmask(SIG_BLOCK, &all, mask);
  if ( err < 0 ) {
    CLUSTERD_LOG(CLUSTERD_CRIT, "Could not block all signals: %s", strerror(errno));
    return -1;
  }

  signal(SIGCHLD, chld_handler);
  signal(SIGPIPE, SIG_IGN);

  err = sigfillset(&want_down.sa_mask); // Mask all
  if ( err < 0 ) {
    CLUSTERD_LOG(CLUSTERD_CRIT, "Could not fill handler sigmask: %s", strerror(errno));
    return -1;
  }

  want_down.sa_handler = want_down_handler;
  want_down.sa_flags = 0;

  err = sigaction(SIGTERM, &want_down, NULL);
  if ( err < 0 ) {
    CLUSTERD_LOG(CLUSTERD_CRIT, "Could not set SIGTERM: %s", strerror(errno));
    return -1;
  }

  err = sigaction(SIGQUIT, &want_down, NULL);
  if ( err < 0 ) {
    CLUSTERD_LOG(CLUSTERD_CRIT, "Could not set SIQUIT: %s", strerror(errno));
    return -1;
  }

  err = sigaction(SIGINT, &want_down, NULL);
  if ( err < 0 ) {
    CLUSTERD_LOG(CLUSTERD_CRIT, "Could not set SIGINT: %s", strerror(errno));
    return -1;
  }

  //  signal(SIGHUP, sighup_handler);

  return 0;
}

static void deliver_signal(int s) {
  int err;
  switch (g_state) {
  case PROCESS_STARTED:
    err = kill(g_pid, s);
    if ( err < 0 ) {
      CLUSTERD_LOG(CLUSTERD_DEBUG, "Could not deliver signal %d: %s", s, strerror(errno));
      return;
    }
    break;

  default:
    CLUSTERD_LOG(CLUSTERD_DEBUG, "Could not deliver signal %d: process not running", s);
    break;
  }
}

static void exec_command(char c) {
  switch ( c ) {
  case 't': deliver_signal(SIGTERM); break;
  case 'k': deliver_signal(SIGKILL); break;
  case 'q': deliver_signal(SIGQUIT); break;
  case 'i': deliver_signal(SIGINT); break;
  case '1': deliver_signal(SIGUSR1); break;
  case '2': deliver_signal(SIGUSR2); break;
  case 's': deliver_signal(SIGSTOP); break;
  case 'c': deliver_signal(SIGCONT); break;
  case 'h': deliver_signal(SIGHUP); break;

  default:
    CLUSTERD_LOG(CLUSTERD_CRIT, "Could not figure out command %c", c);
  }
}

static void process_command(int stspipe, fd_set *rfds, fd_set *efds) {
  ssize_t err;

  if ( FD_ISSET(stspipe, efds) ) {
    CLUSTERD_LOG(CLUSTERD_CRIT, "Error reading from status pipe");
    return;
  }

  if ( FD_ISSET(stspipe, rfds) ) {
    // Read command characters from the pipe
    char commands[16];
    int i;

    err = read(stspipe, commands, sizeof(commands));
    if ( err < 0 ) {
      CLUSTERD_LOG(CLUSTERD_CRIT, "Could not read from pipe: %s", strerror(errno));
      return;
    }

    for ( i = 0; i < err; ++i ) {
      exec_command(commands[i]);
    }
  }
}

static void process_socket(int *sk, int family, fd_set *rfd, fd_set *efd) {
  int err;


  if ( *sk < 0 ) return;

  if ( FD_ISSET(*sk, efd) ) {
    int soerr;
    socklen_t soerrlen = sizeof(soerr);

    CLUSTERD_LOG(CLUSTERD_ERROR, "Error on socket: %d", *sk);

    err = getsockopt(*sk, SOL_SOCKET, SO_ERROR, &soerr, &soerrlen);
    if ( err < 0 ) {
      CLUSTERD_LOG(CLUSTERD_ERROR, "Could not get socket error: %s", strerror(errno));
      return;
    } else if ( soerrlen != sizeof(soerr) ) {
      CLUSTERD_LOG(CLUSTERD_ERROR, "Invalid error result returned");
      return;
    } else {
      CLUSTERD_LOG(CLUSTERD_ERROR, "Socket error on %d was: %s", *sk, strerror (soerr));

      exit(98); // TODO. Try to re-open socket
    }
  }

  if ( FD_ISSET(*sk, rfd) ) {
    CLUSTERD_LOG(CLUSTERD_INFO, "Would read monitor socket %d", *sk); // TODO
  }
}

static void kill_process(pid_t ps) {
  int err;

  err = kill(ps, SIGKILL);
  if ( err < 0 ) {
    CLUSTERD_LOG(CLUSTERD_ERROR, "Could not send SIGKILL to %u: %s", ps, strerror(errno));
  }
}

static void relay_signal(pid_t ps) {
  int err;

  if ( g_state == PROCESS_STARTED ) {
    CLUSTERD_LOG(CLUSTERD_DEBUG, "Relay signal %d", g_down_signal);
    kill(ps, g_down_signal);
  } else {
    g_state = PROCESS_DYING; // Don't kill the finish script
    return;
  }

  err = clock_gettime(CLOCK_MONOTONIC, &g_next_kill);
  if( err < 0 ) {
    CLUSTERD_LOG(CLUSTERD_DEBUG, "Could not get clock: %s", strerror(errno));
    kill(ps, SIGKILL);
    g_state = PROCESS_COMPLETE;
    return;
  }


  switch ( g_down_signal ) {
  default: break;

  case SIGTERM:
    g_state = PROCESS_DYING;
    timespec_add_ms(&g_next_kill, DEFAULT_SIGTERM_GRACE);
    break;

  case SIGQUIT:
    g_state = PROCESS_DYING;
    timespec_add_ms(&g_next_kill, DEFAULT_SIGQUIT_GRACE);
    break;
  }
}

static int read_gid_and_uid_ranges(const char *key, const char *value) {
  if ( strcmp(key, CLUSTERD_CONFIG_NS_UID_RANGE_KEY) == 0 ) {
    return clusterd_parse_uid_range(value, &g_ns_uid_lower, &g_ns_uid_count);
  } else if ( strcmp(key, CLUSTERD_CONFIG_GROUP_NAME) == 0 ) {
    return clusterd_parse_group(value, &g_clusterd_gid);
  } else {
    // Typically, we ignore unknown keys
    return 0;
  }
}

static pid_t daemonize() {
  pid_t child, sessid, grandchild;
  int stspipe[2], err;

  err = pipe(stspipe);
  if ( err < 0 ) {
    CLUSTERD_LOG(CLUSTERD_ERROR, "Could not create status pipe in daemonize: %s", strerror(errno));
    return -1;
  }

  child = fork();
  if ( child < 0 ) {
    close(stspipe[0]);
    close(stspipe[1]);
    return child;
  }

  if ( child == 0 ) {
    close(stspipe[0]);

    // The child needs to call setsid to become the session leader, then fork again
    sessid = setsid();
    if ( sessid < 0 ) {
      CLUSTERD_LOG(CLUSTERD_ERROR, "Could not create session: %s", strerror(errno));
      goto send_errno;
    }

    grandchild = fork();
    if ( grandchild < 0 ) {
      CLUSTERD_LOG(CLUSTERD_ERROR, "Could not perform second fork(): %s", strerror(errno));
      goto send_errno;
    }

    if ( grandchild == 0 ) {
      close(stspipe[1]);
      return 0; // We are the grandchild
    }

    errno = 0;
  send_errno:
    // In the child, we first send the success errno (0), then we send
    // the grandchild pid
    err = write(stspipe[1], &errno, sizeof(errno));
    if ( err < 0 ) {
      CLUSTERD_LOG(CLUSTERD_CRIT, "Could not write errno to stspipe");
      exit(EXIT_FAILURE);
    }

    if ( errno != 0 ) {
      exit(EXIT_FAILURE);
    }

    // Send the child PID
    err = write(stspipe[1], &grandchild, sizeof(grandchild));
    if ( err < 0 ) {
      CLUSTERD_LOG(CLUSTERD_CRIT, "Could not write grandchild to stspipe");
      exit(EXIT_FAILURE);
    }

    exit(EXIT_SUCCESS);
  } else {
    int child_errno;

    close(stspipe[1]);

    // Attempt to read errno from child, then wait for child to exit
    err = read(stspipe[0], &child_errno, sizeof(child_errno));
    if ( err < 0 ) {
      CLUSTERD_LOG(CLUSTERD_CRIT, "Could not read child errno: %s", strerror(errno));
      close(stspipe[0]);
      return -1;
    }

    if ( child_errno != 0 ) {
      CLUSTERD_LOG(CLUSTERD_CRIT, "While daemonizing, child got error: %s", strerror(errno));
      close(stspipe[0]);

      errno = child_errno;
      return -1;
    }

    err = read(stspipe[0], &grandchild, sizeof(grandchild));
    if ( err < 0 ) {
      CLUSTERD_LOG(CLUSTERD_CRIT, "Could not read grandchild pid from pipe: %s", strerror(errno));
      close(stspipe[0]);
      return -1;
    }

    close(stspipe[0]);

    CLUSTERD_LOG(CLUSTERD_DEBUG, "Launched grandchild: %u", grandchild);

    return grandchild;
  }
}

int main(int argc, char *const *argv) {
  int c, firstarg = -1, err, svargc, had_pid = 0, ppid, running = 0, stspipe;
  const char *namespace = "default", *service = NULL;
  char realpath[PATH_MAX], path[PATH_MAX], *pidend;
  char s_info[PATH_MAX*3];
  char *const *svargv;
  clusterctl ctl;
  pid_t ps, daemon_pid;
  sigset_t smask;

  setlocale(LC_ALL, "C");
  srandom(time(NULL));

  while ( (c = getopt(argc, argv, "-n:m:p:vh")) != -1 ) {
    switch ( c ) {
    case 1:
      firstarg = optind - 1;
      goto argsdone;

    case 'p':
      errno = 0;
      ppid = strtol(optarg, &pidend, 10);
      if ( errno != 0 || *pidend != '\0' ||
           ppid < 0 || ppid > 65535 ) {
        CLUSTERD_LOG(CLUSTERD_ERROR, "Invalid PID");
        return 1;
      }

      g_pid = ppid;
      had_pid = 1;
      break;

    case 'n':
      namespace = optarg;
      break;

    case 'm':
      err = add_monitor(optarg);
      if ( err < 0 ) {
        CLUSTERD_LOG(CLUSTERD_ERROR, "Could not add monitor %s: %s",
                     optarg, strerror(errno));
        return 1;
      }
      break;

    case 'v':
      CLUSTERD_LOG_LEVEL = CLUSTERD_DEBUG;
      break;

    case 'h':
      usage();
      return 0;

    default:
      usage();
      return 1;
    }
  }

 argsdone:
  if ( !had_pid ) {
    CLUSTERD_LOG(CLUSTERD_ERROR, "Process ID is required");
    usage();
    return 1;
  }

  if ( firstarg < 0 ) {
    CLUSTERD_LOG(CLUSTERD_ERROR, "Service name or ID must be provided on command line");
    usage();
    return 1;
  }

  service = argv[firstarg];
  svargc = argc - firstarg - 1;
  svargv = argv + firstarg + 1;

  CLUSTERD_LOG(CLUSTERD_DEBUG, "Supervising service %s (%p)", service, argv[firstarg]);

  /* Read the system configuration */
  err = clusterd_read_system_config(read_gid_and_uid_ranges);
  if ( err < 0 ) {
    CLUSTERD_LOG(CLUSTERD_ERROR, "Could not parse system configuration: %s", strerror(errno));
    return 1;
  }

  if ( g_ns_uid_lower == 0 ||
       (g_ns_uid_lower + g_ns_uid_count - 1) <= g_ns_uid_lower ) {
    CLUSTERD_LOG(CLUSTERD_ERROR, "Invalid UID range in system configuration: %s", strerror(errno));
    return 1;
  }

  if ( g_clusterd_gid == 0 ) {
    CLUSTERD_LOG(CLUSTERD_ERROR, "No clusterd gid specified in system configuration");
    return 1;
  }

  g_urandom = fopen("/dev/urandom", "rb");
  if ( !g_urandom ) {
    CLUSTERD_LOG(CLUSTERD_ERROR, "Could not open /dev/urandom... Monitor requests may be insecure");
  }

  err = clusterctl_open(&ctl);
  if ( err < 0 ) {
    CLUSTERD_LOG(CLUSTERD_CRIT, "Could not open cluster: %s", strerror(errno));
    fclose(g_urandom);
    return 1;
  }

  /* Find details about the requested service, like the image path */
  err = clusterctl_call_simple(&ctl, CLUSTERCTL_CONSISTENT_READS,
                               get_service_path_lua,
                               s_info, sizeof(s_info),
                               "namespace", namespace,
                               "service", service,
                               NULL);
  if ( err < 0 ) {
    CLUSTERD_LOG(CLUSTERD_CRIT,
                 "Could not fetch service details: %s", strerror(errno));
    fclose(g_urandom);
    return 1;
  }

  // Parse service information
  err = parse_service_details(s_info, &g_nsid, &g_sid, path, sizeof(path));
  if ( err < 0 ) {
    CLUSTERD_LOG(CLUSTERD_CRIT,
                 "Invalid service details returned from controller: %s", strerror(errno));
    fclose(g_urandom);
    return 1;
  }

  CLUSTERD_LOG(CLUSTERD_DEBUG, "Running service " SVC_F " in namespace " NS_F ": path %s",
               g_sid, g_nsid, path);

  if ( g_nsid >= g_ns_uid_count ) {
    CLUSTERD_LOG(CLUSTERD_CRIT, "We do not have enough namespace UIDs to map this namespace's root account");
    CLUSTERD_LOG(CLUSTERD_CRIT, "Only %u namespace UIDs are available", g_ns_uid_count);
    return 1;
  } else {
    g_root_uid = g_ns_uid_lower + g_nsid;
  }

  err = make_process_directory();
  if ( err < 0 ) {
    CLUSTERD_LOG(CLUSTERD_CRIT, "Could not create process directory: %s", strerror(errno));
    fclose(g_urandom);
    return 1;
  }

  stspipe = make_status_pipe();
  if ( stspipe < 0 ) {
    CLUSTERD_LOG(CLUSTERD_CRIT, "Could not make status pipe: %s", strerror(errno));
    fclose(g_urandom);
    return 1;
  }

  /* Steps to run the process:
   *
   *   1. Ensure the image is downloaded by running the user-provided
   *      image fetching utility. Make sure process is in a runnable
   *      state.
   *
   *   2. Contact all process monitors. If monitors cannot be
   *      contacted, the process is degraded. We run
   *      clusterd-process-doctor and die in that case.
   *
   *   3. Execute the service binary and update the service state in
   *      the controller
   *
   *   4. Run healthcheck script to see if service is up. Update
   *      process state when necessary.
   *
   *   5. If process dies (SIGCHLD), run cleanup script, and start
   *      from step 2.
   *
   * On SIGQUIT or SIGTERM, we send the same signal to our child and
   * wait for it to finish.
   */
  err = download_image(path, realpath, sizeof(realpath));
  if ( err < 0 ) {
    CLUSTERD_LOG(CLUSTERD_CRIT, "Could not download image %s: %s",
                 path, strerror(errno));
    fclose(g_urandom);
    return 1;
  }

  CLUSTERD_LOG(CLUSTERD_DEBUG, "Downloaded image to %s", realpath);

  if ( (g_socket4 < 0 &&
        g_socket6 < 0) ||
       !g_monitors ) {
    CLUSTERD_LOG(CLUSTERD_WARNING, "No monitors connected. This service will not run with high-availability");
  }

  /* Send heartbeats to all monitors */
  err = start_monitoring();
  if ( err < 0 ) {
    CLUSTERD_LOG(CLUSTERD_CRIT, "Could not contact monitors: %s", strerror(errno));
    start_process_doctor();
    fclose(g_urandom);
    return 1;
  }

  err = setup_signals(&smask);
  if ( err < 0 ) {
    CLUSTERD_LOG(CLUSTERD_CRIT, "Could not set up signal mask for execution: %s", strerror(errno));
    return 1;
  }

  /* Daemonize now */
  daemon_pid = daemonize();

  if ( daemon_pid < 0 ) {
    CLUSTERD_LOG(CLUSTERD_CRIT, "Could not daemonize process: %s", strerror(errno));
    return 1;
  } else if ( daemon_pid > 0 ) {
    // This is the parent. If there's a wait condition, then wait for it
    close(stspipe); // We don't need these anymore
    close(g_socket4);
    close(g_socket6);

    // Restore the signal mask so that this process is interruptible
    err = sigprocmask(SIG_SETMASK, &smask, 0);
    if ( err < 0 ) {
      CLUSTERD_LOG(CLUSTERD_ERROR, "Could not restore old signal mask");
    }

    // TODO perform any waits necessary.

    return 0;
  } else {
    /* Execute the service binary */
    err = exec_service(&ps, &smask, realpath, svargc, svargv);
    if ( err < 0 ) {
      CLUSTERD_LOG(CLUSTERD_CRIT, "Could not execute service: %s", strerror(errno));
      /* Monitors will start the service doctor */
      return 1;
    }

    g_max_socket = g_socket4;
    if ( g_socket6 > g_max_socket )
      g_max_socket = g_socket6;

    if ( g_max_socket < 0 )
      g_max_socket = 0;

    while ( g_state != PROCESS_COMPLETE ) {
      pid_t werr;
      int sts, nfds, evs;
      monitor *pending_hb;

      fd_set rfds, wfds, efds;

      struct timespec timeout, start;

      CLUSTERD_LOG(CLUSTERD_DEBUG, "Main loop iteration");

      memset(&timeout, 0, sizeof(timeout));

      FD_ZERO(&rfds);
      FD_ZERO(&wfds);
      FD_ZERO(&efds);

      if ( g_socket4 >= 0 ) {
        FD_SET(g_socket4, &rfds);
        FD_SET(g_socket4, &efds);
      }

      if ( g_socket6 >= 0 ) {
        FD_SET(g_socket6, &rfds);
        FD_SET(g_socket6, &efds);
      }

      FD_SET(stspipe, &rfds);
      FD_SET(stspipe, &efds);

      werr = waitpid(ps, &sts, WNOHANG);
      if ( werr < 0 ) {
        CLUSTERD_LOG(CLUSTERD_ERROR, "Could not wait for child: %s", strerror(errno));
        g_state = PROCESS_COMPLETE;
        break;
      } else if ( werr > 0 ) {
        if ( WIFEXITED(sts) && WEXITSTATUS(sts) == 100 ) {
          CLUSTERD_LOG(CLUSTERD_CRIT, "Internal process error: %u", WEXITSTATUS(sts));
          g_state = PROCESS_COMPLETE;
          continue;
        }

        if ( WIFEXITED(sts) )
          CLUSTERD_LOG(CLUSTERD_ERROR, "Process has exited: %u", WEXITSTATUS(sts));
        else if ( WIFSIGNALED(sts) )
          CLUSTERD_LOG(CLUSTERD_ERROR, "Process killed due to signal %d", WTERMSIG(sts));
        else
          CLUSTERD_LOG(CLUSTERD_ERROR, "Process killed because of status: %u", sts);

        if ( g_state == PROCESS_STARTED )
          process_failure(&ps, sts, &smask);
        else if ( g_state == PROCESS_FAILURE ) {
          CLUSTERD_LOG(CLUSTERD_INFO, "Finish script complete");
          g_state = PROCESS_RECOVERED;
        } else if ( g_state == PROCESS_DYING ) {
          g_state = PROCESS_COMPLETE;
          continue;
        } else {
          CLUSTERD_LOG(CLUSTERD_CRIT, "Received SIGCHLD from unknown state %d", g_state);
          g_state = PROCESS_COMPLETE;
        }
      }

      // Get next ring. If any monitor is in state send pending, then
      // add its socket to the write set
      for ( pending_hb = g_monitors; pending_hb; pending_hb = pending_hb->next ) {
        if ( pending_hb->state == MONITOR_HEARTBEAT_SEND_PENDING )
          FD_SET(pending_hb->local_sk, &wfds);
        else {
          if ( (timeout.tv_sec == 0 && timeout.tv_nsec == 0) ||
               timespec_cmp(&pending_hb->next_hb, &timeout) < 0 )
            memcpy(&timeout, &pending_hb->next_hb, sizeof(timeout));
        }
      }

      // If we're in the failure or recovering states then add g_next_start to the timer
      if ( g_state == PROCESS_RECOVERED &&
           ((timeout.tv_sec == 0 && timeout.tv_nsec == 0) ||
            timespec_cmp(&g_next_start, &timeout) < 0) ) {
        memcpy(&timeout, &g_next_start, sizeof(timeout));
      }

      if ( g_state == PROCESS_DYING &&
           ((timeout.tv_sec == 0 && timeout.tv_nsec == 0) ||
            timespec_cmp(&g_next_kill, &timeout) < 0) ) {
        memcpy(&timeout, &g_next_kill, sizeof(timeout));
      }

      // Get timeout from next_hb time
      if ( timeout.tv_sec != 0 || timeout.tv_nsec != 0 ) {
        err = clock_gettime(CLOCK_MONOTONIC, &start);
        if ( err < 0 ) {
          CLUSTERD_LOG(CLUSTERD_CRIT, "Could not get start time: %s", strerror(errno));
        }

        if ( timespec_cmp(&timeout, &start) <= 0 ) {
          evs = 0;
          goto process;
        }

        timespec_sub(&timeout, &start);
      }

      evs = pselect(g_max_socket, &rfds, &wfds, &efds,
                    ((timeout.tv_sec != 0 || timeout.tv_nsec != 0) ? &timeout : NULL),
                    &smask);
    process:
      if ( evs < 0 ) {
        if ( errno != EINTR )
          CLUSTERD_LOG(CLUSTERD_CRIT, "Could not select: %s", strerror(errno));

        if ( g_down_signal >= 0 ) {
          CLUSTERD_LOG(CLUSTERD_DEBUG, "Received signal %d", g_down_signal);
          relay_signal(ps);

          g_down_signal = -1;
        }
      } else {
        struct timespec now;

        err = clock_gettime(CLOCK_MONOTONIC, &now);
        if ( err < 0 ) {
          CLUSTERD_LOG(CLUSTERD_CRIT, "Could not get time: %s", strerror(errno));
          return 99;
        }

        // Send out any pending heartbeats, if possible
        for ( pending_hb = g_monitors; pending_hb; pending_hb = pending_hb->next ) {
          if ( pending_hb->state == MONITOR_HEARTBEAT_SEND_PENDING ||
               timespec_cmp(&now, &pending_hb->next_hb) >= 0 /* check if time has passed */ ) {
            if ( pending_hb->state == MONITOR_HEARTBEAT_SENT ) {
              // Failure
              monitor_failure(pending_hb);
              pending_hb->state = MONITOR_WAITING;
            }

            send_monitor_heartbeat(pending_hb);
          }
        }

        // Respond to events
        if ( g_socket4 >= 0 )
          process_socket(&g_socket4, AF_INET, &rfds, &efds);

        if ( g_socket6 >= 0 )
          process_socket(&g_socket6, AF_INET6, &rfds, &efds);

        process_command(stspipe, &rfds, &efds);

        // If the state is PROCESS_RECOVERED and g_next_start has passed, start the service again
        if ( g_state == PROCESS_RECOVERED &&
             timespec_cmp(&now, &g_next_start) >= 0 ) {
          CLUSTERD_LOG(CLUSTERD_DEBUG, "Cooloff period passed. Restarting service");

          err = exec_service(&ps, &smask, realpath, svargc, svargv);
          if ( err < 0 ) {
            CLUSTERD_LOG(CLUSTERD_CRIT, "Could not restart service: %s", strerror(errno));
            g_state = PROCESS_COMPLETE;
          }

          g_state = PROCESS_STARTED;
        }

        if ( g_state == PROCESS_DYING &&
             timespec_cmp(&now, &g_next_kill) >= 0 ) {
          CLUSTERD_LOG(CLUSTERD_CRIT, "Process did not die on time. Killing");
          kill_process(ps);
          g_state = PROCESS_COMPLETE;
        }
      }
    }

    // TODO need to clean up the process directory and make sure the
    // child is dead.

    // Process directory can be cleaned up now
    close(stspipe);
    clean_process_directory();
  }

  return 0;
}
