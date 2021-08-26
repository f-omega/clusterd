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


#define CLUSTERD_COMPONENT "libclusterctl"
#include "clusterd/log.h"
#include "clusterd/common.h"
#include "libclusterctl.h"
#include "sha256.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <limits.h>
#include <netdb.h>
#include <stdarg.h>
#include <fcntl.h>

#include "libclusterctl.h"

extern int CLUSTERD_LOG_LEVEL;

static int random_controller_ix(const char *controller, int *countout) {
  int count = 0, i, ctllen = strlen(controller);

  for ( i = 0; i < ctllen; ++i ) {
    if ( controller[i] == ',' ) count ++;
  }

  if ( count == 0 && ctllen == 0 ) {
    CLUSTERD_LOG(CLUSTERD_ERROR, "CLUSTERD_CONTROLLER environment variable must contain at least one controller");
    return -1;
  } else
    count++;

  if ( countout ) *countout = count;

  return random() % count;
}

static int resolve_controller(struct sockaddr_storage *ss, socklen_t *addrlen,
                              const char *controller, int ix) {
  int i, cur, err, ctllen = strlen(controller), addrcnt;
  const char *ctlend, *hostsep;

  char service[10], host[HOST_NAME_MAX + 1];

  struct addrinfo hints, *addrs, *curaddr;

  for ( i = 0, cur = 0; i < ctllen && cur < ix; ++i ) {
    if ( controller[i] == ',' )
      cur ++;
  }

  if ( i >= ctllen ) {
    errno = ERANGE;
    return -1;
  }

  ctlend = strchr(controller + i, ',');
  if ( !ctlend ) {
    ctlend = controller + ctllen;
  }

  if ( controller[i] == '[' ) {
    // IPv6 addr
    hostsep = memchr(controller + i, ']', ctlend - (controller + i));
    if ( !hostsep ) {
      CLUSTERD_LOG(CLUSTERD_WARNING, "CLUSTERD_CONTROLLER has unterminated IPv6 address");
      errno = ESRCH;
      return -1;
    }

    if ( (hostsep + 1) >= ctlend ) {
      sprintf(service, "clusterd");
      hostsep = ctlend;
    } else if ( hostsep[1] != ':' ) {
      CLUSTERD_LOG(CLUSTERD_WARNING, "Junk at end of IPv6 address");
      errno = EAFNOSUPPORT;
      return -1;
    }
  } else {
    hostsep = memchr(controller + i, ':', ctlend - (controller + i));
    if ( !hostsep ) {
      sprintf(service, "clusterd");
      hostsep = ctlend;
    }
  }

  if ( (ctlend - hostsep - 1) >= sizeof(service) ) {
    errno = ENAMETOOLONG;
    return -1;
  } else if ( (hostsep + 1) < ctlend ) {
    memset(service, 0, sizeof(service));
    memcpy(service, hostsep + 1, ctlend - hostsep - 1);
  }

  if ( (hostsep - (controller + i)) > HOST_NAME_MAX ) {
    CLUSTERD_LOG(CLUSTERD_DEBUG, "Controller hostname is too long");
    errno = ENAMETOOLONG;
    return -1;
  } else {
    memset(host, 0, sizeof(host));
    memcpy(host, controller + i, hostsep - (controller + i));
  }

  // Set up hints
  hints.ai_family = AF_UNSPEC; // Either IPv4 or IPv6 is fine
  hints.ai_socktype = SOCK_STREAM; // Want TCP
  hints.ai_protocol = IPPROTO_TCP;
  hints.ai_flags = AI_ADDRCONFIG | AI_V4MAPPED;

  err = getaddrinfo(host, service, &hints, &addrs);
  if ( err < 0 ) {
    CLUSTERD_LOG(CLUSTERD_ERROR, "Could not resolve controller %s:%s: %s", host, service,
                 gai_strerror(err));

    errno = EINVAL;
    return -1;
  }

  for ( curaddr = addrs, addrcnt = 0;
        curaddr;
        curaddr = curaddr->ai_next, addrcnt++ );

  if ( addrcnt == 0 ) {
    CLUSTERD_LOG(CLUSTERD_ERROR, "Host name not found: %s:%s", host, service);
  }

  i = random() % addrcnt;
  for ( curaddr = addrs, cur = 0;
        cur < i;
        curaddr = curaddr->ai_next, cur++ );

  if ( curaddr->ai_addrlen > sizeof(*ss) ) {
    freeaddrinfo(addrs);

    errno = EFAULT;
    return -1;
  }

  memcpy(ss, curaddr->ai_addr, curaddr->ai_addrlen);
  *addrlen = curaddr->ai_addrlen;

  freeaddrinfo(addrs);

  return 0;
}

static int clusterctl_connect(clusterctl *c, struct sockaddr_storage *ctladdr, socklen_t ctladdrlen) {
  int sk, err;

  sk = socket(ctladdr->ss_family, SOCK_STREAM, IPPROTO_TCP);
  if ( sk < 0 ) {
    CLUSTERD_LOG(CLUSTERD_CRIT, "Could not open socket: %s", strerror(errno));
    return -1;
  }

  err = connect(sk, (struct sockaddr *) ctladdr, ctladdrlen);
  if ( err < 0 ) {
    CLUSTERD_LOG(CLUSTERD_CRIT, "Could not connect to controller: %s", strerror(errno));
    return -1;
  }

  c->sk = sk;

  return 0;
}

static int clusterctl_connect_random(clusterctl *c) {
  struct sockaddr_storage ctladdr;
  socklen_t ctladdrlen;
  const char *controller;
  int err, ctlix, firstctlix, i, count;

  controller = getenv("CLUSTERD_CONTROLLER");
  if ( !controller ) {
    CLUSTERD_LOG(CLUSTERD_CRIT, "CLUSTERD_CONTROLLER environment variable must point to the clusterd controller");

    errno = ENOENT;
    return -1;
  }

  firstctlix = ctlix = random_controller_ix(controller, &count);
  if ( ctlix < 0 ) {
    CLUSTERD_LOG(CLUSTERD_CRIT, "Could not choose controller");
    return -1;
  }

 next_controller:
  // We want to choose a random controller
  err = resolve_controller(&ctladdr, &ctladdrlen, controller, ctlix);
  if ( err < 0 ) {
    CLUSTERD_LOG(CLUSTERD_CRIT, "Could not resolve controller: %s", strerror(errno));
    return -1;
  }

  for ( i = 0; i < c->attempts; ++i ) {
    if ( clusterd_addrcmp(c->attempt_addrs + i, &ctladdr) == 0 ) {
      ctlix = (ctlix + 1) % count;
      if ( ctlix == firstctlix ) {
        errno = ENAVAIL;
        return -1;
      } else
        goto next_controller;
    }
  }

  memcpy(c->attempt_addrs + c->attempts, &ctladdr, ctladdrlen);
  c->attempts++;

  return clusterctl_connect(c, &ctladdr, ctladdrlen);
}

int clusterctl_open(clusterctl *c) {
  int err;

  c->attempts = 0;

  while ( c->attempts < CLUSTERCTL_MAX_ATTEMPTS ) {
    err = clusterctl_connect_random(c);
    if ( err == 0 ) break;

    c->attempts++;
  }

  if ( err < 0 ) return err;

  c->state = CLUSTERCTL_WAIT_COMMAND;
  c->error = 0;
  c->linelen = 0;

  return 0;
}

void clusterctl_close(clusterctl *c) {
  c->state = CLUSTERCTL_CLOSED;
  c->linelen = 0;
  close(c->sk);
}

static int readpline(clusterctl *c, char *buf, size_t bufsz, size_t *linesz) {
  size_t realsz, fullsz;
  char *eol;

  *linesz = 0;

  if ( c->linelen > 0 &&
       (eol = memchr(c->line, '\n', c->linelen)) ) {
    fullsz = realsz = eol - c->line + 1;

    if ( realsz >= bufsz )
      realsz = bufsz - 1;

    memcpy(buf, c->line, realsz);
    buf[realsz] = '\0';

    *linesz = realsz;

    memcpy(c->line, c->line + fullsz, c->linelen - fullsz);
    c->linelen -= fullsz;

    return 0;
  }

  while ( !memchr(c->line, '\n', c->linelen) &&
          c->linelen < sizeof(c->line) ) {
    ssize_t bytes;

    bytes = recv(c->sk, c->line + c->linelen, sizeof(c->line) - c->linelen, 0);
    if ( bytes < 0 ) {
      if ( errno == EINTR ) continue;

      *linesz = 0;
      return -1;
    } else if ( bytes == 0 ) {
      errno = ESHUTDOWN;
      return -1;
    } else
      c->linelen += bytes;
  }

  if ( memchr(c->line, '\n', c->linelen) )
    return readpline(c, buf, bufsz, linesz);
  else {
    clusterctl_close(c);

    errno = EMSGSIZE;
    return -1;
  }
}

static void clusterctl_set_error(clusterctl *c, const char *linebuf, size_t linesz) {
  static const struct {
    const char *error;
    int errcode;
  } errors[] =
      { { "!abort\n", CLUSTERCTL_PROTO },
        { "!invalid-command", CLUSTERCTL_INVALID_COMMAND },
        { "!invalid-pragma", CLUSTERCTL_INVALID_PRAGMA },
        { "!unavailable", CLUSTERCTL_UNAVAILABLE },
        { "!hash-mismatch", CLUSTERCTL_HASH_MISMATCH },
        { "!bad-syntax", CLUSTERCTL_BAD_SYNTAX },
        { "!out-of-memory", CLUSTERCTL_OOM },
        { "!unknown", CLUSTERCTL_UNKNOWN_ERR },
        { NULL, 0 }
      };

  int errc = CLUSTERCTL_UNKNOWN_ERR, i = 0;

  for ( i = 0; errors[i].error; i++ )
    if ( strncmp(linebuf, errors[i].error, linesz) == 0 ) {
      c->state = errors[i].errcode;
      return;
    }

  c->state = CLUSTERCTL_UNKNOWN_ERR;
}

static int clusterctl_reconnect(clusterctl *c, const char *addr) {
  int err, old_sk;
  struct sockaddr_storage ss;
  socklen_t sslen;

  err = clusterd_addr_parse((char *)addr, &ss, 1);
  if ( err < 0 )
    return err;

  old_sk = c->sk;

  switch ( ss.ss_family ) {
  case AF_INET:
    sslen = sizeof(struct sockaddr_in);
    break;

  case AF_INET6:
    sslen = sizeof(struct sockaddr_in6);
    break;

  default:
    errno = EINVAL;
    return -1;
  }

  err = clusterctl_connect(c, &ss, sslen);
  if ( err < 0 ) return err;

  close(old_sk);

  return 0;
}

static int clusterctl_upgrade_level(clusterctl *c, const char *pragma) {
  static const size_t leader_len = strlen("!leader ");
  int err, redirects = 0;
  size_t pragmalen = strlen(pragma);

  char linebuf[1024];
  size_t linesz;

 again:
  if ( redirects > 3 ) {
    CLUSTERD_LOG(CLUSTERD_ERROR, "Request redirected more than %d times", redirects - 1);
    errno = ENAVAIL;
    return -1;
  } else if ( redirects > 0 ) {
    CLUSTERD_LOG(CLUSTERD_DEBUG, "Trying upgrade again with leader");
  }

  // Send the pragma
  err = send(c->sk, pragma, pragmalen, 0);
  if ( err < 0 ) {
    return err;
  } else if ( err != pragmalen ) {
    clusterctl_close(c);

    errno = ESHUTDOWN;
    return -1;
  }

  // Now read back. It should either be +ok or !leader...
  err = readpline(c, linebuf, sizeof(linebuf), &linesz);
  if ( err < 0 ) return err;

  if ( strncmp(linebuf, "+ok\n", linesz) == 0 ) {
    return 0;
  } else if ( linesz >= leader_len &&
              memcmp(linebuf, "!leader ", leader_len) == 0 ) {
    char addr[CLUSTERD_ADDRSTRLEN + 1];
    int nodeid;
    err = sscanf(linebuf, "!leader %" MKSTR(CLUSTERD_ADDRSTRLEN) "s %d", addr, &nodeid);
    if ( err != 2 ) {
      CLUSTERD_LOG(CLUSTERD_ERROR, "Invalid leader line: %.*s", (int)linesz, linebuf);
      clusterctl_close(c);
      errno = EPROTO;
      return -1;
    }

    CLUSTERD_LOG(CLUSTERD_DEBUG, "Re-establishing clusterctl to leader: %s (nodeid=%d)", addr, nodeid);

    err = clusterctl_reconnect(c, addr);
    if ( err < 0 ) {
      CLUSTERD_LOG(CLUSTERD_ERROR, "Could not connect to leader");
      errno = ENAVAIL;
      return -1;
    }

    redirects++;
    goto again;
  } else {
    CLUSTERD_LOG(CLUSTERD_ERROR, "Unknown response to pragma: %.*s", (int)linesz, linebuf);
    clusterctl_close(c);

    errno = EPROTO;
    return -1;
  }
}

static int clusterctl_begin_command(clusterctl *c, const char *cmdstring, clusterctl_tx_level lvl) {
  char linebuf[1024];
  int err;
  size_t linesz;

  if ( c->state != CLUSTERCTL_WAIT_COMMAND ) {
    errno = EINVAL;
    return -1;
  }

  switch ( lvl ) {
  case CLUSTERCTL_STALE_READS:
    break;

  case CLUSTERCTL_CONSISTENT_READS:
    err = clusterctl_upgrade_level(c, "+consistent\n");
    if ( err < 0 ) return -1;
    break;

  case CLUSTERCTL_MAY_WRITE:
    err = clusterctl_upgrade_level(c, "+write\n");
    if ( err < 0 ) return -1;
    break;

  default:
    CLUSTERD_LOG(CLUSTERD_ERROR, "Invalid clusterctl command level: %d", lvl);
    errno = EINVAL;
    return -1;
  }

  linesz = snprintf(linebuf, sizeof(linebuf), "%s\n", cmdstring);
  if ( linesz >= sizeof(linebuf) ) {
    errno = EMSGSIZE;
    return -1;
  }

  err = send(c->sk, linebuf, linesz, 0);
  if ( err < 0 ) {
    return err;
  } else if ( err != linesz ) {
    clusterctl_close(c);

    errno = ESHUTDOWN;
    return -1;
  }

  // Either will be +need-script or +params or an error
  err = readpline(c, linebuf, sizeof(linebuf), &linesz);
  if ( err < 0 ) return err;

  if ( linesz == 0 ) {
    clusterctl_close(c);

    errno = EPROTO;
    return -1;
  } else if ( linebuf[0] == '!' ) {
    clusterctl_set_error(c, linebuf, linesz);

    errno = EINVAL;
    return -1;
  } else if ( strncmp(linebuf, "+need-script\n", linesz) == 0 ) {
    c->state = CLUSTERCTL_WAIT_SCRIPT;
    return CLUSTERCTL_NEEDS_SCRIPT;
  } else if ( strncmp(linebuf, "+params\n", linesz) == 0 ) {
    c->state = CLUSTERCTL_WAIT_PARAMS;
  } else {
    clusterctl_close(c);

    errno = EPROTO;
    return -1;
  }

  return 0;
}

int clusterctl_begin_named(clusterctl *c, const char *hash, clusterctl_tx_level lvl) {
  return clusterctl_begin_command(c, hash, lvl);
}

int clusterctl_begin_custom(clusterctl *c, clusterctl_tx_level lvl) {
  return clusterctl_begin_command(c, "", lvl);
}

int clusterctl_pipe_script(clusterctl *c, int fd) {
  // Read from fd and send to remote
  ssize_t err;
  char buf[1024];

  if ( c->state != CLUSTERCTL_WAIT_SCRIPT ) {
    errno = EINVAL;
    return -1;
  }

  while ( (err = read(fd, buf, sizeof(buf))) > 0 ) {
    err = clusterctl_upload_script(c, buf, err);
    if ( err < 0 )
      break;
  }

  return (err < 0 ? err : 0);
}

int clusterctl_upload_script(clusterctl *c, const char *script, ssize_t scriptlen) {
  static const char final_comment[] = "-- CONTROL END\n";
  int eof = 0;
  ssize_t err;

  if ( scriptlen < 0 ) {
    scriptlen = strlen(script);
  }

  if ( !script ) {
    script = final_comment;
    scriptlen = strlen(final_comment);
    eof = 1;
  }

  if ( scriptlen == 0 )
    return 0;

 again:
  for ( err = send(c->sk, script, scriptlen, 0);
        err < scriptlen && err > 0;
        script += err, scriptlen -= err,
          err = send(c->sk, script, scriptlen, 0) );

  if ( err < 0 ) {
    if ( errno == EINTR ) goto again;
    else return err;
  } else if ( err == 0 ) {
    clusterctl_close(c);

    errno = ESHUTDOWN;
    return -1;
  }

  if ( eof ) {
    char linebuf[1024];
    size_t linesz;

    err = readpline(c, linebuf, sizeof(linebuf), &linesz);
    if ( err < 0 ) {
      int serrno = errno;

      if ( errno == ECONNRESET ) {
        clusterctl_close(c);
      }

      errno = serrno;
      return err;
    }

    if ( linesz == 0 ) {
      clusterctl_close(c);

      errno = EPROTO;
      return -1;
    } else if ( linebuf[0] == '!' ) {
      clusterctl_set_error(c, linebuf, linesz);

      errno = EINVAL;
      return -1;
    } else if ( strncmp(linebuf, "+params\n", linesz) == 0 ) {
      c->state = CLUSTERCTL_WAIT_PARAMS;
    } else {
      clusterctl_close(c);

      errno = EPROTO;
      return -1;
    }
  }

  return 0;
}

int clusterctl_upload_params(clusterctl *c, const char *params, ssize_t paramslen) {
  ssize_t err;

  if ( c->state != CLUSTERCTL_WAIT_PARAMS ) {
    errno = EINVAL;
    return -1;
  }

  if ( paramslen == 0 )
    return 0;

  if ( paramslen < 0 ) {
    paramslen = strlen(params);
  }

 again:
  for ( err = send(c->sk, params, paramslen, 0);
        err < paramslen && err > 0;
        params += err, paramslen -= err,
          err = send(c->sk, params, paramslen, 0) );

  if ( err < 0 ) {
    if ( errno == EINTR ) goto again;
    else return err;
  } else if ( err == 0 ) {
    clusterctl_close(c);

    errno = ESHUTDOWN;
    return -1;
  }

  return 0;
}

int clusterctl_invoke(clusterctl *c) {
  int err;
  char linebuf[1024];
  size_t linesz;

  if ( c->state != CLUSTERCTL_WAIT_PARAMS ) {
    errno = EINVAL;
    return -1;
  }

 again:
  err = send(c->sk, "\n", 1, 0);
  if ( err < 0 ) {
    if ( errno == EINTR ) goto again;
    else return err;
  } else if ( err == 0 ) {
    clusterctl_close(c);

    errno = ESHUTDOWN;
    return -1;
  }

  err = readpline(c, linebuf, sizeof(linebuf), &linesz);
  if ( err < 0 ) {
    int serrno = errno;

    if ( errno == ECONNRESET )
      clusterctl_close(c);

    errno = serrno;
    return err;
  }

  if ( linesz == 0 ) {
    clusterctl_close(c);

    errno = EPROTO;
    return -1;
  } else if ( linebuf[0] == '!' ) {
    clusterctl_set_error(c, linebuf, linesz);

    errno = EINVAL;
    return -1;
  } else if ( strncmp(linebuf, "+processing\n", linesz) == 0 ) {
    c->state = CLUSTERCTL_PROCESSING;
  } else {
    clusterctl_close(c);

    errno = EPROTO;
    return -1;
  }

  return 0;
}

int clusterctl_read_output(clusterctl *c, char *linebuf, size_t *linesz) {
  size_t linelen = *linesz;
  int err;

  if ( c->state != CLUSTERCTL_PROCESSING &&
       c->state != CLUSTERCTL_ERROR ) {
    errno = EINVAL;
    return -1;
  }

  if ( *linesz < 1024 ) {
    *linesz = 0;
    errno = EMSGSIZE;
    return -1;
  }

  err = readpline(c, linebuf, *linesz, linesz);
  if ( err < 0 )
    return err;

  if ( strncmp(linebuf, "--DONE\n", *linesz) == 0 ) {
    c->state = CLUSTERCTL_WAIT_COMMAND;
    return CLUSTERCTL_READ_DONE;
  } else if ( strncmp(linebuf, "!ERROR\n", *linesz) == 0 ) {
    if ( c->state == CLUSTERCTL_ERROR ) {
      clusterctl_close(c);
      errno = EPROTO;
      return -1;
    } else {
      *linesz = linelen;
      c->state = CLUSTERCTL_ERROR;
      return clusterctl_read_output(c, linebuf, linesz);
    }
  } else if ( strncmp(linebuf, "!leadership-lost\n", *linesz) == 0 ) {
    // The transaction failed and must be retried
    c->state = CLUSTERCTL_WAIT_COMMAND;
    return CLUSTERCTL_READ_TXFAILED;
  } else if ( c->state == CLUSTERCTL_PROCESSING ) // User line
    return CLUSTERCTL_READ_LINE;
  else
    return CLUSTERCTL_READ_ERROR;
}

int clusterctl_simple(clusterctl *c, clusterctl_tx_level lvl, const char *script, ...) {
  va_list args;
  int err;

  va_start(args, script);
  err = clusterctl_simplev(c, lvl, script, args);
  va_end(args);

  return err;
}

int clusterctl_simplev(clusterctl *c, clusterctl_tx_level lvl,
                       const char *script, va_list args) {
  char sha256str[SHA256_STR_LENGTH + 1];
  unsigned char sha256[SHA256_OCTET_LENGTH];
  size_t scriptlen, paramlen;
  const char *param_name;
  char param_line[64 * 1024];
  int err;

  memset(sha256str, 0, sizeof(sha256str));

  if ( c->state != CLUSTERCTL_WAIT_COMMAND ) {
    errno = EINVAL;
    return -1;
  }

  scriptlen = strlen(script);
  calc_sha_256(sha256, script, scriptlen);
  sha_256_digest(sha256str, sha256);

  err = clusterctl_begin_named(c, sha256str, lvl);
  if ( err < 0 ) {
    CLUSTERD_LOG(CLUSTERD_DEBUG, "Could not invoke named script %s: %s", sha256str, strerror(errno));
    return err;
  } else if ( err == CLUSTERCTL_NEEDS_SCRIPT ) {
    err = clusterctl_upload_script(c, script, scriptlen);
    if ( err < 0 ) goto upload_error;

    err = clusterctl_upload_script(c, NULL, 0);
    if ( err < 0 ) goto upload_error;
  }

  // Add parameters
  while ( (param_name = va_arg(args, const char *)) ) {
    const char *param_value;

    param_value = va_arg(args, const char *);
    if ( !param_value ) continue;

    paramlen = snprintf(param_line, sizeof(param_line), "%s=%s\n", param_name, param_value);
    if ( paramlen >= sizeof(param_line) ) {
      clusterctl_close(c);

      errno = EMSGSIZE;
      return -1;
    }

    err = clusterctl_upload_params(c, param_line, paramlen);
    if ( err < 0 ) {
      CLUSTERD_LOG(CLUSTERD_DEBUG, "Could not upload parameters: %s", strerror(errno));
      return -1;
    }
  }

  err = clusterctl_invoke(c);
  if ( err < 0 ) {
    CLUSTERD_LOG(CLUSTERD_DEBUG, "Could not invoke command: %s", strerror(errno));
    return -1;
  }

  return 0;

 upload_error:
  CLUSTERD_LOG(CLUSTERD_DEBUG, "Could not upload script: %s", strerror(errno));
  return -1;
}

int clusterctl_flush_output(clusterctl *c, int out_fd, int err_fd, int *was_success) {
  char output[64 * 1024];
  size_t outputsz;
  int err;

  if ( was_success ) *was_success = 1;

  for ( outputsz = sizeof(output), err = clusterctl_read_output(c, output, &outputsz);
        err >= 0 && err != CLUSTERCTL_READ_DONE;
        outputsz = sizeof(output), err = clusterctl_read_output(c, output, &outputsz) ) {
    int line_fd;
    size_t written = 0;

    if ( err == CLUSTERCTL_READ_LINE ) {
      line_fd = out_fd;
    } else if ( err == CLUSTERCTL_READ_ERROR ) {
      if ( was_success ) *was_success = 0;
      line_fd = err_fd;
    } else if ( err == CLUSTERCTL_READ_TXFAILED ) {
      errno = ENAVAIL;
      return -1;
    } else {
      CLUSTERD_LOG(CLUSTERD_CRIT, "clusterd_read_output() returned an invalid value");

      errno = EINVAL;
      return -1;
    }

    while ( written < outputsz) {
      err = write(line_fd, output + written, outputsz - written);
      if ( err < 0 ) {
        if ( errno == EINTR ) continue;
        else {
          CLUSTERD_LOG(CLUSTERD_CRIT, "could not write output: %s", strerror(errno));
          return -1;
        }
      } else if ( err == 0 ) {
        CLUSTERD_LOG(CLUSTERD_CRIT, "output file descriptor closed");
        errno = EPIPE;
        return -1;
      } else
        written += err;
    }
  }

  if ( err < 0 ) {
    CLUSTERD_LOG(CLUSTERD_CRIT, "Could not read output: %s", strerror(errno));
    return err;
  }

  return 0;
}

int clusterctl_call_simple(clusterctl *c, clusterctl_tx_level lvl,
                           const char *lua, char *output, size_t outputsz,
                           ...) {
  char linebuf[64 * 1024];
  size_t linesz;
  va_list args;
  int err, attempts = 0;

 tryagain:
  if ( attempts > 3 ) {
    errno = ENAVAIL;
    return -1;
  } else if ( attempts > 0 ) {
    CLUSTERD_LOG(CLUSTERD_DEBUG, "Attempting script again (try %d)", attempts);
  }

  if ( outputsz > 0 ) output[0] = '\0';

  va_start(args, outputsz);
  err = clusterctl_simplev(c, lvl, lua, args);
  va_end(args);

  // Now attempt to read the output
  for ( linesz = sizeof(linebuf), err = clusterctl_read_output(c, linebuf, &linesz);
        err >= 0 && err == CLUSTERCTL_READ_LINE;
        linesz = sizeof(linebuf), err = clusterctl_read_output(c, linebuf, &linesz) ) {
    if ( outputsz > 1 ) {
      size_t copysz = outputsz - 1;
      if ( linesz < copysz )
        copysz = linesz;

      memcpy(output, linebuf, copysz);
      output[copysz] = '\0';

      outputsz -= copysz;
      output += copysz;
    }
  }

  switch ( err ) {
  case CLUSTERCTL_READ_DONE:
    return 0;

  case CLUSTERCTL_READ_TXFAILED:
    CLUSTERD_LOG(CLUSTERD_DEBUG, "Script failed because the controller we were talking to lost leadership: %s",
                 lua);

    // Retry
    attempts ++;
    goto tryagain;

  case CLUSTERCTL_READ_ERROR:
    if ( outputsz > 0 ) output[0] = '\0';

    do {
      fprintf(stderr, "%.*s", (int)linesz, linebuf);
      linesz = sizeof(linebuf);
    } while ( (err = clusterctl_read_output(c, linebuf, &linesz)) == CLUSTERCTL_READ_ERROR );

    if ( err < 0 ) {
      CLUSTERD_LOG(CLUSTERD_ERROR, "Error reading errore: %s", strerror(errno));
      return -1;
    } else if ( err == CLUSTERCTL_READ_DONE ) {
      errno = EINVAL;
      return -1;
    } else {
      errno = EPROTO;
      return -1;
    }

  default:
    if ( err < 0 ) {
      CLUSTERD_LOG(CLUSTERD_DEBUG, "Could not read output: %s", strerror(errno));
      return err;
    }

    errno = EPROTO;
    return -1;
  }
}
