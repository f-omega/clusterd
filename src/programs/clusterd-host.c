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
#include "libclusterctl.h"

#include <string.h>
#include <time.h>
#include <fcntl.h>
#include <errno.h>
#include <locale.h>
#include <getopt.h>
#include <stdlib.h>
#include <sys/wait.h>
#include <unistd.h>

#define MONITOR_COOKIE_LENGTH 64

typedef enum
  {
   MONITOR_WAITING,
   MONITOR_HEARTBEAT_SENT,

   /* Tried to send the heartbeat, but it was blocked */
   MONITOR_HEARTBEAT_SEND_PENDING
  } monitor_state;

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

monitor *g_monitors = NULL;
char g_ps_path[PATH_MAX] = "";

clusterd_namespace_t  g_nsid = 0;
clusterd_service_t    g_sid  = 0;
clusterd_pid_t        g_pid  = 0;

int                g_socket4 = -1;
int                g_socket6 = -1;
int             g_max_socket = 0;
int       g_max_mon_failures = 3;

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

static int make_process_directory() {
  int err;

  err = snprintf(g_ps_path, sizeof(g_ps_path), "%s/proc/" NS_F ":" SVC_F ":" PID_F,
                 clusterd_get_runtime_dir(), g_nsid, g_sid, g_pid);
  if ( err >= sizeof(g_ps_path) ) {
    CLUSTERD_LOG(CLUSTERD_CRIT, "Process directory would be too long");

    errno = ENAMETOOLONG;
    return -1;
  }

  err = mkdir_recursive(g_ps_path);
  if ( err < 0 ) {
    CLUSTERD_LOG(CLUSTERD_CRIT, "Could not create %s: %s", g_ps_path, strerror(errno));
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
                     WIFSIGNALED(exitstatus));
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
  err = snprintf(servicerun, sizeof(servicerun), "%s/start", svpath);
  if ( err >= sizeof(servicerun) ) {
    errno = ENAMETOOLONG;
    return -1;
  }

  err = pipe(stspipe);
  if ( err < 0 ) {
    CLUSTERD_LOG(CLUSTERD_CRIT, "Could not create status pipe: %s", strerror(errno));
    return -1;
  }

  child = fork();
  if ( child < 0 ) {
    CLUSTERD_LOG(CLUSTERD_CRIT, "Could not fork() to start service: %s", strerror(errno));
    return -1;
  } else if ( child == 0 ) {
    unsigned char one = 1;
    close(stspipe[0]);

    err = sigprocmask(SIG_SETMASK, oldmask, NULL);
    if ( err < 0 ) {
      CLUSTERD_LOG(CLUSTERD_CRIT, "Could not reset service mask: %s", strerror(errno));
      exit(100);
    }

    err = fcntl(stspipe[1], F_GETFD, 0);
    if ( err < 0 ) {
      CLUSTERD_LOG(CLUSTERD_CRIT, "Could not get pipe flags: %s", strerror(errno));
      exit(100);
    }

    err = fcntl(stspipe[1], F_SETFD, err | FD_CLOEXEC);
    if ( err < 0 ) {
      CLUSTERD_LOG(CLUSTERD_CRIT, "Could not set FD_CLOEXEC: %s", strerror(errno));
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

    err = read(stspipe[0], &one, 1);
    if ( err < 0 ) {
      int serrno = errno;
      CLUSTERD_LOG(CLUSTERD_CRIT, "Could not read status: %s", strerror(errno));
      kill(child, SIGTERM);
      errno = serrno;
      return -1;
    }

    close(stspipe[0]);

    *ps = child;

    return 0;
  }
}

static void chld_handler(int s) {
  /* TODO if this is from a process grandchild or other descendant,
   * then reap zombies */
}

static int setup_signals(sigset_t *mask) {
  int err;
  sigset_t all;
  struct sigaction action;

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

  return 0;
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

int main(int argc, char *const *argv) {
  int c, firstarg = -1, err, svargc, had_pid = 0, ppid, running = 0;
  const char *namespace = "default", *service = NULL;
  char realpath[PATH_MAX], path[PATH_MAX], *pidend;
  char s_info[PATH_MAX*3];
  char *const *svargv;
  clusterctl ctl;
  pid_t ps;
  sigset_t smask;

  setlocale(LC_ALL, "C");

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

  g_urandom = fopen("/dev/urandom", "rb");
  if ( !g_urandom ) {
    CLUSTERD_LOG(CLUSTERD_ERROR, "Could not open /dev/urandom... Monitor requests may be insecure");
  }

  err = clusterctl_open(&ctl, CLUSTERCTL_REQUIRE_CONSISTENT);
  if ( err < 0 ) {
    CLUSTERD_LOG(CLUSTERD_CRIT, "Could not open cluster: %s", strerror(errno));
    fclose(g_urandom);
    return 1;
  }

  /* Find details about the requested service, like the image path */
  err = clusterctl_call_simple(&ctl, get_service_path_lua,
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

  err = make_process_directory();
  if ( err < 0 ) {
    CLUSTERD_LOG(CLUSTERD_CRIT, "Could not create process directory: %s", strerror(errno));
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

  /* Execute the service binary */
  err = exec_service(&ps, &smask, realpath, svargc, svargv);
  if ( err < 0 ) {
    CLUSTERD_LOG(CLUSTERD_CRIT, "Could not execute service: %s", strerror(errno));
    /* Monitors will start the service doctor */
    fclose(g_urandom);
    return 1;
  }

  running = 1;
  g_max_socket = g_socket4;
  if ( g_socket6 > g_max_socket )
    g_max_socket = g_socket6;

  if ( g_max_socket < 0 )
    g_max_socket = 0;

  while ( running ) {
    pid_t werr;
    int sts, nfds, evs;
    monitor *pending_hb;

    fd_set rfds, wfds, efds;

    struct timespec timeout, start;

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

    werr = waitpid(ps, &sts, WNOHANG);
    if ( werr < 0 ) {
      CLUSTERD_LOG(CLUSTERD_ERROR, "Could not wait for child: %s", strerror(errno));
      running = 0;
      break;
    } else if ( werr > 0 ) {
      if ( WIFEXITED(sts) && WEXITSTATUS(sts) == 100 ) {
        CLUSTERD_LOG(CLUSTERD_CRIT, "Internal process error: %u", WEXITSTATUS(sts));
        running = 0;
        continue;
      }

      CLUSTERD_LOG(CLUSTERD_ERROR, "Process has exited: %u", sts);

      CLUSTERD_LOG(CLUSTERD_CRIT, "Not yet able to start failure script");
      exit(50);
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
      CLUSTERD_LOG(CLUSTERD_CRIT, "Could not select: %s", strerror(errno));
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
    }
  }

  return 0;
}
