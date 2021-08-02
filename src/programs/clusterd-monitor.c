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

#include <stdlib.h>
#include <stdio.h>
#include <getopt.h>
#include <uv.h>
#include <unistd.h>
#include <locale.h>

#include "uthash.h"

#define CLUSTERD_COMPONENT "monitor"
#include "clusterd/log.h"
#include "clusterd/request.h"

static int CLUSTERD_LOG_LEVEL = CLUSTERD_INFO;

#define MAX_MESSAGE_LENGTH 1500

// Every service reports its other monitors. This is the max number of
// monitors that we will store for a given process
#define MAX_MONITORS       9

#define PROCESS_STATE_ACTIVE   0x1
#define PROCESS_STATE_DEGRADED 0x2

#define CLUSTERD_UNKNOWN_ATTR_TEXT                                      \
  "Unknown attribute %04x is not optional. Ignoring packet. "           \
  "This may be caused by a version mismatch"

typedef struct {
  clusterd_namespace_t namespace;
  clusterd_pid_t       process;
} process_key;

typedef struct {
  UT_hash_handle hh;
  process_key key;
  int state;
  uint64_t error_count;
  uint64_t success_count;
  uv_timer_t next_expected;
  uv_timer_t failure_timer;

  struct sockaddr_storage addr;

  uint32_t sigordinal;
  int mon_count;
  struct sockaddr_storage monitors[MAX_MONITORS];
} process_record;

static unsigned int g_success_threshold = 10;
static unsigned int g_failure_timeout = 300000;
static int g_port = CLUSTERD_DEFAULT_MONITOR_PORT;
static process_record *g_processes = NULL;

static void usage() {
  fprintf(stderr, "clusterd-monitor -- high-availability monitoring daemon\n");
  fprintf(stderr, "Usage: clusterd-monitor [-P PORT] [-b ADDR] [-n RETRIES] [-t SECONDS]\n\n");
  fprintf(stderr, "   -P PORT             Set the port to bind in future -b\n");
  fprintf(stderr, "   -b ADDR             Bind the given interface. Must be given after -P\n");
  fprintf(stderr, "   -n NUMBER           Number of consecutive successful monitor requests\n");
  fprintf(stderr, "                       before a degraded service is considered successful\n");
  fprintf(stderr, "   -t SECONDS          Services that have not received monitor requests within\n");
  fprintf(stderr, "                       this time will be considered in need of repair\n");
  fprintf(stderr, "   -v                  Show verbose debugging output\n\n");
  fprintf(stderr, "Please report bugs to support@f-omega.com\n");
}

static void close_handle(uv_handle_t *handle) {
  free(handle);
}

static void get_static_buffer(uv_handle_t *handle, size_t suggested_sz, uv_buf_t *buf) {
  static char packet[MAX_MESSAGE_LENGTH];

  buf->base = packet;
  buf->len = MAX_MESSAGE_LENGTH;;
}

static void process_fails(uv_timer_t *hdl) {
  process_record *process = CLUSTERD_STRUCT_FROM_FIELD(process_record, failure_timer, hdl);
  const char *service_doctor = "clusterd-service-doctor";
  char cmdline[2048];
  int cmdlineix = 0, err, i;
  pid_t child;

  CLUSTERD_LOG(CLUSTERD_ERROR, "Process %u:%u failed",
               (unsigned)process->key.namespace,
               (unsigned)process->key.process);

  if ( getenv("CLUSTERD_SERVICE_DOCTOR") ) {
    service_doctor = getenv("CLUSTERD_SERVICE_DOCTOR");
  }

  HASH_DELETE(hh, g_processes, process);

  err = snprintf(cmdline, sizeof(cmdline), "%s -n %u -i %u",
                 service_doctor, process->key.namespace,
                 (unsigned)process->key.process);
  if ( err >= sizeof(cmdline) ) {
    CLUSTERD_LOG(CLUSTERD_CRIT, "Cannot launch service doctor. Not enough space in cmdline");
    goto done;
  } else
    cmdlineix += err;

  for ( i = 0; i < process->mon_count; ++i ) {
    char addrstr[CLUSTERD_ADDRSTRLEN];
    clusterd_addr_render(addrstr, (struct sockaddr *)process->monitors + i, 1);

    err = snprintf(cmdline + cmdlineix, sizeof(cmdline) - cmdlineix, " -m %s", addrstr);
    if ( (cmdlineix + err) >= sizeof(cmdline) ){
      CLUSTERD_LOG(CLUSTERD_CRIT, "Cannot launch service doctor. Not enough space in cmdline");
      goto done;
    } else
      cmdlineix += err;
  }

  CLUSTERD_LOG(CLUSTERD_DEBUG, "Launching service doctor: %s", cmdline);
  child = fork();
  if ( child < 0 ) {
    CLUSTERD_LOG(CLUSTERD_CRIT, "Failure launching service doctor: %s", strerror(errno));
    goto done;
  } else if ( child != 0 ) {
    CLUSTERD_LOG(CLUSTERD_INFO, "Launched service doctor for %u:%u with pid %u",
                 (unsigned)process->key.namespace,
                 (unsigned)process->key.process,
                 (unsigned)child);
  } else {
    char *const argv[] = { "sh", "-c", cmdline, NULL };
    err = execv("/bin/sh", argv);
    CLUSTERD_LOG(CLUSTERD_INFO, "Failure launching service doctor: %s", strerror(errno));
    exit(100);
  }

 done:
  free(process);
}

static void process_timeout(uv_timer_t *hdl) {
  process_record *process = CLUSTERD_STRUCT_FROM_FIELD(process_record, next_expected, hdl);
  int err;

  process->error_count ++;
  process->success_count = 0;

  if ( process->state == PROCESS_STATE_ACTIVE ) {
    CLUSTERD_LOG(CLUSTERD_WARNING, "Process %u:%u degraded",
                 (unsigned)process->key.namespace,
                 (unsigned)process->key.process);
    err = uv_timer_start(&process->failure_timer, process_fails, g_failure_timeout, 0);
    if ( err < 0 ) {
      CLUSTERD_LOG(CLUSTERD_WARNING, "Could nont start failure timer: %s", uv_strerror(err));
    }
  }

  process->state = PROCESS_STATE_DEGRADED;
}

static int touch_process(process_record *process, const struct sockaddr *addr, unsigned int interval,
                         uint32_t sigordinal, struct sockaddr_storage *monitors, int mon_count) {
  int err;

  if ( memcmp(addr, &process->addr, CLUSTERD_ADDR_LENGTH(addr)) != 0 ) {
    CLUSTERD_LOG(CLUSTERD_WARNING, "Process seems to have moved addresses. Not touching the record");
    return -1;
  }

  err = uv_timer_stop(&process->next_expected);
  if ( err < 0 ) {
    CLUSTERD_LOG(CLUSTERD_WARNING, "Could not stop timer: %s", uv_strerror(err));
  }

  err = uv_timer_start(&process->next_expected, process_timeout, (uint64_t) interval, 0);
  if ( err < 0 ) {
    CLUSTERD_LOG(CLUSTERD_WARNING, "Could not start timer: %s", uv_strerror(err));
    return -1;
  }

  process->mon_count = mon_count;
  memcpy(process->monitors, monitors, mon_count * sizeof(struct sockaddr_storage));

  process->error_count = 0;
  process->success_count ++;

  process->sigordinal = 0;
  if ( sigordinal > process->sigordinal )
    process->sigordinal = sigordinal;

  if ( process->success_count >= g_success_threshold &&
       process->state == PROCESS_STATE_DEGRADED ) {
    CLUSTERD_LOG(CLUSTERD_INFO, "Process %u::%u reactivating",
                 (unsigned)process->key.namespace,
                 (unsigned)process->key.process);
    process->state = PROCESS_STATE_ACTIVE;

    err = uv_timer_stop(&process->failure_timer);
    if ( err < 0 ) {
      CLUSTERD_LOG(CLUSTERD_WARNING, "Could not stop failure timer after process reactivation: %s",
                   uv_strerror(err));
    }
  }

  return 0;
}

static int create_process(process_key *key, const struct sockaddr *addr, unsigned int interval,
                          uint32_t sigordinal, struct sockaddr_storage *monitors, int mon_count) {
  process_record *process;

  process = malloc(sizeof(process_record));
  if ( !process ) {
    CLUSTERD_LOG(CLUSTERD_CRIT, "Could not create process: out of memory");
    return -1;
  }

  memcpy(&process->key, key, sizeof(process_key));
  process->state = PROCESS_STATE_ACTIVE;
  process->error_count = 0;
  process->success_count = 1;

  process->sigordinal = 0;
  if ( sigordinal  > process->sigordinal )
    process->sigordinal = sigordinal;

  memcpy(&process->addr, addr, CLUSTERD_ADDR_LENGTH(addr));
  process->mon_count = mon_count;
  memcpy(process->monitors, monitors, sizeof(struct sockaddr_storage) * mon_count);

  HASH_ADD_KEYPTR(hh, g_processes, &process->key, sizeof(process_key), process);

  return 0;
}

static void process_signal_notification(uv_udp_t *handle, const struct sockaddr *addr,
                                        void *buf, clusterd_request *req) {
  char rspbuf[2048];
  off_t off = 0, attroff = 0;

  clusterd_attr *attr;

  process_key key;
  uint32_t sigordinal = 0;

  int has_namespace = 0, has_process = 0;

  uv_buf_t psig_msg;

  int err;

  process_record *process;

  FORALL_CLUSTERD_ATTRS(attr, buf, req) {
    uint16_t attr_len = ntohs(attr->alen);

    switch ( ntohs(attr->atype) ) {
    case CLUSTERD_ATTR_NAMESPACE:
      if ( has_namespace ) {
        CLUSTERD_LOG(CLUSTERD_DEBUG, "Signal notification request contains two or more namespaces. Ignoring");
        return;
      }

      if ( attr_len != sizeof(clusterd_namespace_t) ) {
        CLUSTERD_LOG(CLUSTERD_DEBUG, "Namespace attribute has incorrect length. Ignoring");
        return;
      }

      has_namespace = 1;
      key.namespace = CLUSTERD_NTOH_NAMESPACE(*(clusterd_namespace_t *)CLUSTERD_ATTR_DATA(attr, buf, req));
      break;

    case CLUSTERD_ATTR_PROCESS:
      if ( has_process ) {
        CLUSTERD_LOG(CLUSTERD_DEBUG, "Signal notification request constains two or more processes. Ignoring");
        return;
      }

      if ( attr_len != sizeof(clusterd_pid_t) ) {
        CLUSTERD_LOG(CLUSTERD_DEBUG, "Process ID attribute has incorrect length. Ignoring");
        return;
      }

      has_process = 1;
      key.process = CLUSTERD_NTOH_PROCESS(*(clusterd_pid_t *)CLUSTERD_ATTR_DATA(attr, buf, req));
      break;

    case CLUSTERD_ATTR_SIGORDINAL:
      if ( sigordinal != 0 ) {
        CLUSTERD_LOG(CLUSTERD_DEBUG, "Signal notification request contains multiple ordinals. Ignoring");
        return;
      }

      if ( attr_len != sizeof(sigordinal) ) {
        CLUSTERD_LOG(CLUSTERD_DEBUG, "Signal notification request has incorrect length. Ignoring");
        return;
      }

      sigordinal = ntohl(*(uint32_t *)CLUSTERD_ATTR_DATA(attr, buf, req));
      if ( !sigordinal ) {
        CLUSTERD_LOG(CLUSTERD_DEBUG, "Sig ordinal attribute was zero. Ignoring packet");
        return;
      }
      break;

    default:
      if ( CLUSTERD_ATTR_OPTIONAL(ntohs(attr->atype)) ) continue;
      else {
        CLUSTERD_LOG(CLUSTERD_WARNING, CLUSTERD_UNKNOWN_ATTR_TEXT,
                     ntohs(attr->atype));
        return;
      }
    }
  }

  if ( !has_namespace ) {
    CLUSTERD_LOG(CLUSTERD_DEBUG, "Signal notification is missing namespace");
    return;
  }

  if ( !has_process ) {
    CLUSTERD_LOG(CLUSTERD_DEBUG, "Signal notification is missing process");
    return;
  }

  if ( sigordinal == 0 ) {
    CLUSTERD_LOG(CLUSTERD_DEBUG, "Signal notification is missing signal ordinal");
    return;
  }

  CLUSTERD_INIT_REQ(rspbuf, off, sizeof(rspbuf), CLUSTERD_OP_SIG_ACK);

  // Lookup the process record
  HASH_FIND(hh, g_processes, &key, sizeof(process_key), process);
  if ( !process ) {
    clusterd_error_attr rsperr = CLUSTERD_HTON_ERROR(CLUSTERD_ERR_PROC_NOT_FOUND);

    // Return process not found error
    CLUSTERD_LOG(CLUSTERD_DEBUG, "Process " PID_F " not found in namespace " NS_F " during signal notification",
                 key.process, key.namespace);

    CLUSTERD_ADD_ATTR(rspbuf, off, attroff, CLUSTERD_ATTR_ERROR);
    CLUSTERD_WRITE_ATTR(rspbuf, off, &rsperr, sizeof(rsperr));
  } else {
    // This is it. Just update the sigordinal. On the next monitor
    // request, the sig ordinal will be returned in the response

    clusterd_namespace_t netns = CLUSTERD_HTON_NAMESPACE(key.namespace);
    clusterd_pid_t netps = CLUSTERD_HTON_PROCESS(key.process);
    uint32_t netsigord = htonl(sigordinal);

    if ( sigordinal > process->sigordinal )
      process->sigordinal = sigordinal;

    // Return acknowledgement
    CLUSTERD_ADD_ATTR(rspbuf, off, attroff, CLUSTERD_ATTR_NAMESPACE);
    CLUSTERD_WRITE_ATTR(rspbuf, off, &netns, sizeof(netns));
    CLUSTERD_ADD_ATTR(rspbuf, off, attroff, CLUSTERD_ATTR_PROCESS);
    CLUSTERD_WRITE_ATTR(rspbuf, off, &netps, sizeof(netps));
    CLUSTERD_ADD_ATTR(rspbuf, off, attroff, CLUSTERD_ATTR_SIGORDINAL);
    CLUSTERD_WRITE_ATTR(rspbuf, off, &netsigord, sizeof(netsigord));
  }

  CLUSTERD_FINISH_REQUEST(rspbuf, off, attroff);
  psig_msg.base = rspbuf;
  psig_msg.len = off;

  err = uv_udp_try_send(handle, &psig_msg, 1, addr);
  if ( err < 0 ) {
    CLUSTERD_LOG(CLUSTERD_WARNING, "Could not send signal ack: %s", uv_strerror(err));
    return;
  }

  CLUSTERD_LOG(CLUSTERD_DEBUG, "Delivered signal process " PID_F " in namespace " NS_F ": new ordinal is %u",
               key.process, key.namespace, sigordinal);

  return;

 nospace:
  CLUSTERD_LOG(CLUSTERD_CRIT, "Not enough space to send signal notification for process " PID_F
               " in namespace " NS_F, key.process, key.namespace);
  return;
}

static void process_monitor_request(uv_udp_t *handle, const struct sockaddr *addr,
                                    void *request, clusterd_request *req) {
  char rspbuf[2048];
  off_t off = 0, attroff = 0;

  clusterd_attr *attr;

  uint16_t attr_type;
  size_t attr_len;
  void *attr_value;

  int has_namespace = 0, has_process = 0, mon_count = 0, err;
  unsigned int interval = CLUSTERD_DEFAULT_PING_INTERVAL;

  process_key key;
  uint32_t sigordinal = 0, last_sigordinal = 0;

  clusterd_ip4_attr ip4;
  clusterd_ip6_attr ip6;

  uv_buf_t ack_buf;

  process_record *existing;

  struct sockaddr_storage monitors[MAX_MONITORS]; // One of the monitors may be us

  memset(monitors, 0, sizeof(monitors));

  if ( CLUSTERD_LOG_LEVEL <= CLUSTERD_DEBUG ) {
    char addrstr[CLUSTERD_ADDRSTRLEN];
    clusterd_addr_render(addrstr, addr, 1);

    CLUSTERD_LOG(CLUSTERD_DEBUG, "Got monitor request from %s", addrstr);
  }

  FORALL_CLUSTERD_ATTRS(attr, request, req) {
    attr_type = ntohs(attr->atype);
    attr_len = ntohs(attr->alen);
    attr_value = CLUSTERD_ATTR_DATA(attr, request, req);

    switch ( attr_type ) {
    case CLUSTERD_ATTR_NAMESPACE:
      if ( has_namespace ) {
        CLUSTERD_LOG(CLUSTERD_DEBUG, "Monitor request contains two or more namespaces. Ignoring");
        return;
      }
      if ( attr_len != sizeof(clusterd_namespace_t) ) {
        CLUSTERD_LOG(CLUSTERD_DEBUG, "Namespace attribute has incorrect length. Ignoring");
        return;
      }

      has_namespace = 1;
      key.namespace = CLUSTERD_NTOH_NAMESPACE(*(clusterd_namespace_t *)attr_value);
      break;

    case CLUSTERD_ATTR_PROCESS:
      if ( has_process ) {
        CLUSTERD_LOG(CLUSTERD_DEBUG, "Monitor request contains two or more processs. Ignoring");
        return;
      }
      if ( attr_len != sizeof(clusterd_pid_t) ) {
        CLUSTERD_LOG(CLUSTERD_DEBUG, "Process attribute has incorrect length. Ignoring");
        return;
      }

      has_process = 1;
      key.process = CLUSTERD_NTOH_PROCESS(*(clusterd_pid_t *) attr_value);
      break;

    case CLUSTERD_ATTR_MONITOR_V4:
      if ( attr_len != sizeof(clusterd_ip4_attr) ) {
        CLUSTERD_LOG(CLUSTERD_DEBUG, "Monitor ipv4 attribute not correct. Ignoring");
        return;
      } else if ( mon_count < MAX_MONITORS ) {
        struct sockaddr_in in;
        memcpy(&in.sin_addr.s_addr, &ip4.ip4_addr, sizeof(in.sin_addr.s_addr));
        in.sin_port = ip4.ip4_port;

        memcpy(monitors + mon_count, &in, sizeof(struct sockaddr_in));
        mon_count++;
      } else goto too_many_monitors;
      break;

    case CLUSTERD_ATTR_MONITOR_V6:
      if ( attr_len != sizeof(clusterd_ip6_attr) ) {
        CLUSTERD_LOG(CLUSTERD_DEBUG, "Monitor ipv6 attribute not correct. Ignoring");
        return;
      } else if ( mon_count < MAX_MONITORS ) {
        struct sockaddr_in6 in6;
        memset(&in6, 0, sizeof(in6));
        memcpy(in6.sin6_addr.s6_addr, ip6.ip6_addr, sizeof(in6.sin6_addr.s6_addr));

        memcpy(monitors + mon_count, &in6, sizeof(struct sockaddr_in6));
        mon_count++;
      } else goto too_many_monitors;
      break;

    case CLUSTERD_ATTR_INTERVAL:
      if ( attr_len != sizeof(uint32_t) ) {
        CLUSTERD_LOG(CLUSTERD_DEBUG, "Interval attribute not correct. Ignoring");
        return;
      } else {
        interval = ntohl(*(uint32_t *)attr_value);
      }
      break;

    case CLUSTERD_ATTR_SIGORDINAL:
      if ( sigordinal != 0 ) {
        CLUSTERD_LOG(CLUSTERD_DEBUG, "Sig ordinal given twice in monitor request. Ignoring");
        return;
      } else if ( attr_len != sizeof(sigordinal) ) {
        CLUSTERD_LOG(CLUSTERD_DEBUG, "Sig ordinal is too large. Ignoring");
        return;
      } else {
        sigordinal = ntohl(*(uint32_t *)attr_value);
      }
      break;

    default:
      if ( CLUSTERD_ATTR_OPTIONAL(attr_type) ) continue;
      else {
        CLUSTERD_LOG(CLUSTERD_WARNING, CLUSTERD_UNKNOWN_ATTR_TEXT,
                     attr_type);
        return;
      }
    };
  }

  if ( !has_process ||
       !has_namespace ) {
    CLUSTERD_LOG(CLUSTERD_WARNING, "Monitor request incomplete. Ignoring");
    return;
  }

  // Lookup the process by process key
  HASH_FIND(hh, g_processes, &key, sizeof(process_key), existing);

  if ( !existing ) {
    CLUSTERD_LOG(CLUSTERD_DEBUG, "Process key %u:%u does not exist. Creating...",
                 (unsigned)key.namespace,
                 (unsigned)key.process);
    err = create_process(&key, addr, interval, sigordinal, monitors, mon_count);
    if ( err < 0 ) {
      CLUSTERD_LOG(CLUSTERD_WARNING, "Could not create process");
      return;
    }

    last_sigordinal = sigordinal;
  } else {
    CLUSTERD_LOG(CLUSTERD_DEBUG, "Process key %u:%u already exists. Touching...",
                 (unsigned)key.namespace,
                 (unsigned)key.process);
    err = touch_process(existing, addr, interval, sigordinal, monitors, mon_count);
    if ( err < 0 ) {
      CLUSTERD_LOG(CLUSTERD_WARNING, "Could not touch process");
      return;
    }

    last_sigordinal = existing->sigordinal;
  }

  // Send ACK request
  CLUSTERD_INIT_REQ(rspbuf, off, sizeof(rspbuf), CLUSTERD_OP_MONITOR_ACK);
  if ( last_sigordinal != 0 ) {
    uint32_t netsigord = htonl(last_sigordinal);
    CLUSTERD_ADD_ATTR(rspbuf, off, attroff, CLUSTERD_ATTR_SIGORDINAL);
    CLUSTERD_WRITE_ATTR(rspbuf, off, &netsigord, sizeof(netsigord));
  }
  CLUSTERD_FINISH_REQUEST(rspbuf, off, attroff);

  ack_buf.base = (void *)rspbuf;
  ack_buf.len = off;

  err = uv_udp_try_send(handle, &ack_buf, 1, addr);
  if ( err < 0 ) {
    CLUSTERD_LOG(CLUSTERD_WARNING, "Could not send ACK: %s", uv_strerror(err));
    return;
  }

  CLUSTERD_LOG(CLUSTERD_DEBUG, "Successful monitor request processing");
  return;

 too_many_monitors:
  CLUSTERD_LOG(CLUSTERD_WARNING, "Too many monitors in monitor request. Ignoring");
  return;

 nospace:
  CLUSTERD_LOG(CLUSTERD_DEBUG, "Could not send monitor response: no space");
  return;
}

static void on_monitor_request(uv_udp_t *handle, ssize_t nread, const uv_buf_t *buf,
                               const struct sockaddr *addr, unsigned int flags) {
  clusterd_request req;


  if ( nread < 0 ) {
    CLUSTERD_LOG(CLUSTERD_DEBUG, "Transmission error on handle");
    return;
  }

  if ( nread < sizeof(clusterd_request) ) {
    CLUSTERD_LOG(CLUSTERD_DEBUG, "Received monitor request is too short to be valid");
    return;
  }

  memcpy(&req, buf->base, sizeof(clusterd_request));

  if ( ntohs(req.length) >= nread ) {
    CLUSTERD_LOG(CLUSTERD_DEBUG, "Request is supposedly longer than the number of bytes read");
    return;
  }

  if ( ntohl(req.magic) != CLUSTERD_MAGIC ) {
    CLUSTERD_LOG(CLUSTERD_DEBUG, "Ignoring request because MAGIC does not match");
    return;
  }

  switch ( ntohs(req.op) ) {
  case CLUSTERD_OP_MONITOR:
    process_monitor_request(handle, addr, buf->base, &req);
    break;

  case CLUSTERD_OP_SIG_NOTIFY:
    process_signal_notification(handle, addr, buf->base, &req);
    break;

  default:
    CLUSTERD_LOG(CLUSTERD_DEBUG, "Ignoring request because it's not a MONITOR or signal notification");
    return;
  }
}

static int bind_address(const char *addrstr, uv_loop_t *loop) {
  struct sockaddr_storage addr;

  if ( uv_ip4_addr(addrstr, g_port, (struct sockaddr_in *)&addr)  == 0 ||
       uv_ip6_addr(addrstr, g_port, (struct sockaddr_in6 *)&addr) == 0 ) {
    uv_udp_t *udp;
    int err;

    CLUSTERD_LOG(CLUSTERD_INFO, "Binding to %s:%d", addrstr, g_port);

    udp = malloc(sizeof(uv_udp_t));
    if ( !udp ) {
      CLUSTERD_LOG(CLUSTERD_CRIT, "Could not allocate space for uv_udp_t");
      return -1;
    }

    err = uv_udp_init(loop, udp);
    if ( err < 0 ) {
      CLUSTERD_LOG(CLUSTERD_CRIT, "Could not initialize UDP: %s", uv_strerror(err));
      free(udp);
      return -1;
    }

    err = uv_udp_bind(udp, (struct sockaddr *)&addr, 0);
    if ( err < 0 ) {
      CLUSTERD_LOG(CLUSTERD_CRIT, "Could not bind address %s:%d: %s", addrstr, g_port,
                   uv_strerror(err));
      uv_close((uv_handle_t *)udp, close_handle);
      return -1;
    }

    err = uv_udp_recv_start(udp, get_static_buffer, on_monitor_request);
    if ( err < 0 ) {
      CLUSTERD_LOG(CLUSTERD_CRIT, "Could not receive message: %s", uv_strerror(err));
      uv_close((uv_handle_t *)udp, close_handle);
      return -1;
    }

    return 0;
  } else
    return -1;
}

int main(int argc, char *const *argv) {
  int c;
  uv_loop_t loop;
  char *endptr;
  int addrs = 0, err;

  setlocale(LC_ALL, "C");
  uv_loop_init(&loop);

  while ((c = getopt(argc, argv, "P:b:n:t:hv")) != -1) {
    switch (c) {
    case 'P':
      g_port = strtol(optarg, &endptr, 10);
      if ( *endptr != '\0' ) {
        CLUSTERD_LOG(CLUSTERD_CRIT, "Invalid argument to -P");
        return 1;
      }
      break;

    case 'b':
      err = bind_address(optarg, &loop);
      if ( err < 0 ) {
        CLUSTERD_LOG(CLUSTERD_CRIT, "Could not bind address %s", optarg);
        return 1;
      }
      break;

    case 'n':
      g_success_threshold = (unsigned int)strtol(optarg, &endptr, 10);
      if ( *endptr != '\0' ) {
        CLUSTERD_LOG(CLUSTERD_CRIT, "Invalid argument to -n");
        return 1;
      }
      break;

    case 't':
      g_failure_timeout = (unsigned int)strtol(optarg, &endptr, 10);
      if ( *endptr != '\0' ) {
        CLUSTERD_LOG(CLUSTERD_CRIT, "Invalid argument to -t");
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
      fprintf(stderr, "Unknown option %s\n", argv[optind]);
      usage();
      return 1;
    }
  }

  if ( addrs == 0 ) {
    int err;
    CLUSTERD_LOG(CLUSTERD_WARNING, "No addresses provided. Binding all interfaces");

    err = bind_address("::", &loop);
    if ( err < 0 ) {
      CLUSTERD_LOG(CLUSTERD_WARNING, "Could not bind any IPv6 addresses");
    }
  }

  CLUSTERD_LOG(CLUSTERD_INFO, "Cluster monitor starting");

  uv_run(&loop, UV_RUN_DEFAULT);

  uv_loop_close(&loop);

  return 0;
}
