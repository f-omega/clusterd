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

#define CLUSTERD_COMPONENT "clusterd-dns"
#include "clusterd/log.h"
#include "clusterd/common.h"
#include "libclusterctl.h"

#include <getopt.h>
#include <locale.h>
#include <signal.h>
#include <stdint.h>
#include <time.h>
#include <grp.h>
#include <pwd.h>

#include <sys/select.h>
#include <sys/socket.h>
#include <sys/types.h>

#include <netinet/in.h>

#define MAX_DNS_PACKET_SIZE 512
#define QNAME_LENGTH        256

#define DNS_RESPONSE_F      0xC000
#define DNS_OPCODE_MASK     0x7800
#define DNS_OPCODE_SHIFT    11
#define DNS_AUTHORITATIVE_F 0x0400
#define DNS_TRUNCATED_F     0x0200
#define DNS_RECURSIVE_F     0x0100
#define DNS_RECURS_AVL_F    0x0080
#define DNS_RCODE_MASK      0x000F

#define DNS_OP_QUERY     0
#define DNS_OP_INV_QUERY 1
#define DNS_OP_SVR_STS   2

#define DNS_RCODE_OK     0
#define DNS_RCODE_FORMAT 1
#define DNS_RCODE_SRVFLR 2
#define DNS_RCODE_NMERR  3
#define DNS_RCODE_NOTIMP 4
#define DNS_RCODE_REFUSD 5

#define DNS_TYPE_A       1
#define DNS_TYPE_NS      2
#define DNS_TYPE_MD_OBS  3
#define DNS_TYPE_MF_OBS  4
#define DNS_TYPE_CNAME   5
#define DNS_TYPE_SOA     6
#define DNS_TYPE_WKS     11
#define DNS_TYPE_PTR     12
#define DNS_TYPE_HINFO   13
#define DNS_TYPE_MINFO   14
#define DNS_TYPE_MX      15
#define DNS_TYPE_TXT     16
#define DNS_TYPE_AAAA    28

#define DNS_CLASS_IN     1

struct dnshdr {
  uint16_t pktid;
  uint16_t flags;
  uint16_t qdcnt;
  uint16_t ancnt;
  uint16_t nscnt;
  uint16_t arcnt;
} CLUSTERD_PACKED;

struct dnsanswer {
  uint16_t type;
  uint16_t class;
  uint32_t ttl;
  uint16_t len;
} CLUSTERD_PACKED;

#define CLUSTERD_DOMAIN    "clusterd.local"
#define CLUSTERD_ENDPOINTS "ep.clusterd.local"

#define DNS_ISRESPONSE(hdr) (!!(ntohs((hdr)->flags) & DNS_RESPONSE_F))
#define DNS_RECURSION_REQUESTED(hdr) (!!(ntohs((hdr)->flags) & DNS_RECURSIVE_F))
#define DNS_OPCODE(hdr) (((ntohs((hdr)->flags)) & DNS_OPCODE_MASK) >> DNS_OPCODE_SHIFT)
#define DNS_SETOPCODE(flags, op) (flags) = ((flags) & ~DNS_OPCODE_MASK) | (((op) & 0xF) << DNS_OPCODE_SHIFT)
#define DNS_SETRCODE(flags, rc) (flags) = ((flags) & ~DNS_RCODE_MASK) | ((rc) & DNS_RCODE_MASK)

int CLUSTERD_LOG_LEVEL = CLUSTERD_INFO;

static int *g_endpoints = NULL;
static int g_endpoint_count;
static int g_deduce_namespace = 1;

static int open_dns_socket(char *bindaddr, int proto) {
  int sk, err, *endpoints;
  struct sockaddr_storage ss;
  char addr[CLUSTERD_ADDRSTRLEN];

  err = clusterd_addr_parse(bindaddr, &ss, 1);
  if ( err < 0 ) {
    CLUSTERD_LOG(CLUSTERD_ERROR, "Invalid bind address: %s", bindaddr);
    return -1;
  }

  sk = socket(ss.ss_family, (proto == IPPROTO_TCP ? SOCK_STREAM : SOCK_DGRAM) | SOCK_NONBLOCK, proto );
  if ( sk < 0 ) {
    CLUSTERD_LOG(CLUSTERD_ERROR, "Could not create socket for %s: %s", bindaddr, strerror(errno));
    return -1;
  }

  // Now bind the socket
  err = bind(sk, (struct sockaddr *)&ss, CLUSTERD_ADDR_LENGTH(&ss));
  if ( err < 0 ) {
    CLUSTERD_LOG(CLUSTERD_ERROR, "Could not bind %s: %s", bindaddr, strerror(errno));
    close(sk);
    return -1;
  }

  clusterd_addr_render(addr, (struct sockaddr *)&ss, 1);
  CLUSTERD_LOG(CLUSTERD_DEBUG, "Bound to %s", addr);

  // if tcp, then listen
  if ( proto == IPPROTO_TCP ) {
    err = listen(sk, 5);
    if ( err < 0 ) {
      CLUSTERD_LOG(CLUSTERD_ERROR, "Could not listen() on socket %s: %s", bindaddr, strerror(errno));
      close(sk);
      return -1;
    }
  }

  endpoints = realloc(g_endpoints, (g_endpoint_count + 1) * sizeof(int));
  if ( !endpoints ) {
    close(sk);
    CLUSTERD_LOG(CLUSTERD_ERROR, "Could not listen on %s: out of memory", bindaddr);
    return -1;
  }

  endpoints[g_endpoint_count] = sk;
  g_endpoint_count++;
  g_endpoints = endpoints;

  return sk;
}

static ssize_t qname_parse(char *result, const char *data, size_t datasz) {
  size_t i, ri;

  for ( i = 0, ri = 0; i < datasz && ri < QNAME_LENGTH; ++i ) {
    uint8_t segment_len;

    memcpy(&segment_len, data + i, sizeof(segment_len));
    if ( segment_len == 0 ) {
      result[ri] = '\0';
      i++;
      break;
    }

    CLUSTERD_LOG(CLUSTERD_DEBUG, "Got segment i=%zu ri=%zu len=%u", i, ri, segment_len);
    if ( (segment_len + i) > datasz )
      return -1;

    if ( ri != 0 ) {
      result[ri] = '.';
      ri++;
    }

    memcpy(result + ri, data + i + 1, segment_len);
    ri += segment_len;
    i += segment_len;
  }

  if ( i == datasz || ri == QNAME_LENGTH )
    return -1;

  return i;
}

static int strip_domain(char *qname, const char *domain) {
  size_t domainlen = strlen(domain), qnamelen = strlen(qname);

  if ( qnamelen >= domainlen &&
       strcasecmp(qname + qnamelen - domainlen, domain) == 0 ) {
    if ( qnamelen == domainlen )
      qname[0] = '\0';
    else
      qname[qnamelen - domainlen - 1] = '\0';

    return 0;
  }

  return 1;
}

static int last_component(char *qname, char **last) {
  char *lastdot;

  *last = NULL;

  if ( qname[0] == '\0' )
    return 1;

  lastdot = strrchr(qname, '.');
  if ( lastdot ) {
    *lastdot = '\0';
    *last = lastdot + 1;

    return 0;
  }

  *last = qname;
  return 0;
}

static int deduce_namespace(struct sockaddr_storage *addr, socklen_t addrlen,
                            clusterd_namespace_t *nsid) {
  static const uint8_t clusterd_service_prefix[] =
    { CLUSTERD_PROCESS_PREFIX_ADDR };

  if ( addr->ss_family == AF_INET ) {
    return -1;
  } else if ( addr->ss_family == AF_INET6 ) {
    struct sockaddr_in6 *addr6 = (struct sockaddr_in6 *) addr;

    if ( memcmp(addr6->sin6_addr.s6_addr, clusterd_service_prefix, sizeof(clusterd_service_prefix)) == 0 ) {
      uint32_t ns;
      memcpy(&ns, addr6->sin6_addr.s6_addr + sizeof(clusterd_service_prefix) - 1, sizeof(ns));

      ns = ntohl(ns);

      *nsid = ns;
      return 0;
    } else
      return -1;
  } else
    return -1;
}

static void send_dns_aaaa_answer(int rspfd, struct sockaddr_storage *addr, socklen_t addrlen,
                                 struct dnshdr *hdr, const char *qnameraw, size_t qnamelen,
                                 struct in6_addr *resolved) {
  struct dnshdr rsp;
  struct dnsanswer ans;
  struct msghdr msg;
  struct iovec iov[4];
  ssize_t err;

  rsp.pktid = hdr->pktid;
  rsp.flags = DNS_RESPONSE_F | DNS_AUTHORITATIVE_F;
  DNS_SETOPCODE(rsp.flags, DNS_OPCODE(hdr));
  DNS_SETRCODE(rsp.flags, DNS_RCODE_OK);
  rsp.flags = htons(rsp.flags);

  rsp.qdcnt = rsp.nscnt = rsp.arcnt = 0;

  rsp.ancnt = htons(1);

  ans.class = htons(DNS_CLASS_IN);
  ans.type = htons(DNS_TYPE_AAAA);
  ans.ttl = htonl(300);
  ans.len = htons(16);

  iov[0].iov_base = (void *)&rsp;
  iov[0].iov_len = sizeof(rsp);
  iov[1].iov_base = (void *)qnameraw;
  iov[1].iov_len = qnamelen;
  iov[2].iov_base = (void *)&ans;
  iov[2].iov_len = sizeof(ans);
  iov[3].iov_base = resolved->s6_addr;
  iov[3].iov_len = 16;

  msg.msg_name = (void *)addr;
  msg.msg_namelen = addrlen;
  msg.msg_iov = iov;
  msg.msg_iovlen = sizeof(iov) / sizeof(iov[0]);
  msg.msg_control = NULL;
  msg.msg_controllen = 0;
  msg.msg_flags = 0;

  err = sendmsg(rspfd, &msg, 0);
  if ( err < 0 ) {
    CLUSTERD_LOG(CLUSTERD_WARNING, "Could not send response to %u: %s", htons(hdr->pktid), strerror(errno));
    return;
  }
}

static int resolve_name(const char *ns, const char *qname, struct in6_addr *resolved) {
  static const char resolve_name_lua[] =
    "success, nm = pcall(clusterd.get_endpoint_by_name, params.namespace, params.name)\n"
    "if success and nm ~= nil then\n"
    "  clusterd.output(tostring(nm.namespace) .. ' ' .. tostring(nm.id))\n"
    "end\n";

  clusterctl ctl;
  int err;

  char output[4096];
  clusterd_endpoint_t epid;
  clusterd_namespace_t nsid;

  uint8_t addr[16] =
    { CLUSTERD_ENDPOINT_PREFIX_ADDR, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00 };

  err = clusterctl_open(&ctl);
  if ( err < 0 ) {
    CLUSTERD_LOG(CLUSTERD_ERROR, "Could not open clusterctl: %s", strerror(errno));
    return -1;
  }

  err = clusterctl_call_simple(&ctl, CLUSTERCTL_CONSISTENT_READS,
                               resolve_name_lua, output, sizeof(output),
                               "namespace", ns,
                               "name", qname,
                               NULL);
  if ( err < 0 ) {
    CLUSTERD_LOG(CLUSTERD_ERROR, "Could not resolve name: %s", strerror(errno));
    clusterctl_close(&ctl);
    return -DNS_RCODE_SRVFLR;
  }

  clusterctl_close(&ctl);

  // Output is currently just an endpoint id, or a blank string
  if ( strlen(output) == 0 ) {
    return -DNS_RCODE_NMERR;
  }

  err = sscanf(output, NS_F " " EP_F, &nsid, &epid);
  if ( err != 2 ) {
    CLUSTERD_LOG(CLUSTERD_ERROR, "Could not parse endpoint for query ns=%s, name=%s. Response was %s",
                 ns, qname, output);
    return -DNS_RCODE_SRVFLR;
  }

  epid = htonl(epid);
  nsid = htonl(nsid);

  memcpy(resolved->s6_addr, addr, sizeof(resolved->s6_addr));
  memcpy(resolved->s6_addr + 8, &nsid, sizeof(nsid));
  memcpy(resolved->s6_addr + 12, &epid, sizeof(epid));

  if ( CLUSTERD_LOG_LEVEL <= CLUSTERD_DEBUG ) {
    char addrstr[CLUSTERD_ADDRSTRLEN];
    CLUSTERD_LOG(CLUSTERD_DEBUG, "Resolved ns=%s, nm=%s to %s", ns, qname,
                 inet_ntop(AF_INET6, resolved, addrstr, sizeof(addrstr)));
  }

  return 0;
}

static void resolve(int rspfd, struct dnshdr *hdr, char *qname, int dnstype,
                    char *qnameraw, size_t qnamelen,
                    struct sockaddr_storage *addr, socklen_t addrlen) {
  int err, dnserr = DNS_RCODE_OK;
  uint16_t flags = 0;
  struct dnshdr rsp;
  ssize_t bytessent;

  // First check if qname is part of our SOA. It ought to be in clusterd.local


  if ( strip_domain(qname, CLUSTERD_ENDPOINTS) == 0 ) {
    char nsbuf[16];
    char *ns = NULL;

    CLUSTERD_LOG(CLUSTERD_DEBUG, "Request to resolve endpoint %s", qname);
    flags |= DNS_AUTHORITATIVE_F;

    if ( last_component(qname, &ns) != 0 ) {
      dnserr = DNS_RCODE_NMERR;
      goto error;
    }

    if ( ns[0] == '_' ) {
      ns ++;
      CLUSTERD_LOG(CLUSTERD_DEBUG, "Request to resolve %s in namespace %s", qname, ns);

    } else {
      clusterd_namespace_t nsid;

      if ( ns != qname )
        ns[-1] = '.';

      CLUSTERD_LOG(CLUSTERD_DEBUG, "Request to resolve %s in current namespace", qname);

      // Attempt to guess namespace based on the sending address. If
      // not guessable, then return error
      //
      // We guess by comparing the address to the IPv6 clusterd prefix
      // and attempting to find the namespace
      if ( g_deduce_namespace ) {
        err = deduce_namespace(addr, addrlen, &nsid);
        if ( err < 0 ) {
          char addrstr[CLUSTERD_ADDRSTRLEN];
          clusterd_addr_render(addrstr, (struct sockaddr *)&addr, 1);
          CLUSTERD_LOG(CLUSTERD_WARNING, "Could not deduce address from %s", addrstr);

          dnserr = DNS_RCODE_REFUSD;
          goto error;
        }

        snprintf(nsbuf, sizeof(nsbuf), NS_F, nsid);
        ns = nsbuf;
      }
    }

    if ( !ns ) { // If we could not get a namespace, return refused
      dnserr = DNS_RCODE_REFUSD;
      goto error;
    }

    CLUSTERD_LOG(CLUSTERD_DEBUG, "Need to lookup name %s in namespace %s", qname, ns);

    if ( dnstype == DNS_TYPE_AAAA ) {
      struct in6_addr resolved;

      // TODO don't expose cross-namespace names unless requested
      err = resolve_name(ns, qname, &resolved);
      if ( err < 0 ) {
        dnserr = -err;
        goto error;
      }

      send_dns_aaaa_answer(rspfd, addr, addrlen, hdr, qnameraw, qnamelen, &resolved);
      return;
    } else goto error; // Success, but no records
  } else if ( strip_domain(qname, CLUSTERD_DOMAIN) == 0 ) {
    flags |= DNS_AUTHORITATIVE_F;
    dnserr = DNS_RCODE_NMERR;
    goto error;
  } else {
    CLUSTERD_LOG(CLUSTERD_WARNING, "Don't know how to resolve %s: not part of our domain", qname);

    // Return non-authoritative response
    dnserr = DNS_RCODE_NMERR;
    goto error;
  }

  return;

 error:
  if ( dnserr != DNS_RCODE_OK &&
       dnserr != DNS_RCODE_NMERR )
    CLUSTERD_LOG(CLUSTERD_WARNING, "Sending error for %s: %d", qname, dnserr);
  else if ( dnserr == DNS_RCODE_NMERR )
    CLUSTERD_LOG(CLUSTERD_DEBUG, "Could not find %s", qname);

  rsp.pktid = hdr->pktid;
  rsp.flags = flags | DNS_RESPONSE_F;
  DNS_SETOPCODE(rsp.flags, DNS_OPCODE(hdr));
  DNS_SETRCODE(rsp.flags, dnserr);
  rsp.flags = htons(rsp.flags);

  rsp.qdcnt = rsp.ancnt = rsp.nscnt = rsp.arcnt = 0;

  bytessent = sendto(rspfd, &rsp, sizeof(rsp), 0, (struct sockaddr *)addr, addrlen);
  if ( bytessent < 0 ) {
    if ( errno == EWOULDBLOCK || errno == EAGAIN ) return; // Queue too full.. just wait
    CLUSTERD_LOG(CLUSTERD_ERROR, "Could not respond to packet %u: %s", ntohs(hdr->pktid),
                 strerror(errno));
    return;
  }

  if ( bytessent < sizeof(rsp) ) {
    CLUSTERD_LOG(CLUSTERD_WARNING, "DNS response %u silently truncated", ntohs(hdr->pktid));
    return;
  }
}

static int process_dns_packet(int fd) {
  char pkt[MAX_DNS_PACKET_SIZE];
  ssize_t sz;

  struct sockaddr_storage addr;
  socklen_t addrlen;

  addrlen = sizeof(addr);
  sz = recvfrom(fd, pkt, sizeof(pkt), 0, (struct sockaddr *)&addr, &addrlen);
  if ( sz < 0 ) {
    CLUSTERD_LOG(CLUSTERD_ERROR, "Could not read from %d: %s", fd, strerror(errno));
    return -1;
  } else if ( sz == 0 ) {
    CLUSTERD_LOG(CLUSTERD_ERROR, "Remote disconnect?");
    return -1;
  } else {
    struct dnshdr hdr;
    ssize_t off = sizeof(hdr);
    int qix;

    CLUSTERD_LOG(CLUSTERD_DEBUG, "Received packet of size %zd", sz);

    if ( sz < sizeof(hdr) ) {
      CLUSTERD_LOG(CLUSTERD_DEBUG, "Size %zd is too small for a DNS header", sz);
      return 0;
    }

    memcpy(&hdr, pkt, sizeof(hdr));

    CLUSTERD_LOG(CLUSTERD_DEBUG, "Got dns packet (id=%u, rsp=%d, op=%u, recursive=%d, qdcnt=%u, ancnt=%u, nscnt=%u, arcnt=%u)",
                 hdr.pktid, DNS_ISRESPONSE(&hdr), DNS_OPCODE(&hdr), DNS_RECURSION_REQUESTED(&hdr),
                 ntohs(hdr.qdcnt), ntohs(hdr.ancnt), ntohs(hdr.nscnt), ntohs(hdr.arcnt));

    if ( DNS_ISRESPONSE(&hdr) ) return 0;

    // Parse each query
    for ( qix = 0; off < sz && qix < ntohs(hdr.qdcnt); qix++ ) {
      char qname[QNAME_LENGTH + 1], *qnameraw;
      ssize_t qnamelen;
      uint16_t qtype, qclass;

      qnameraw = pkt + off;
      qnamelen = qname_parse(qname, qnameraw, sz - off);
      if ( qnamelen < 0 ) {
        CLUSTERD_LOG(CLUSTERD_WARNING, "Malformed QNAME in DNS query");
        return 0;
      }

      off += qnamelen;

      if ( (sz - off) < (sizeof(qtype) + sizeof(qclass)) ) {
        CLUSTERD_LOG(CLUSTERD_WARNING, "Query cut short");
        return 0;
      }

      memcpy(&qtype, pkt + off, sizeof(qtype));
      off += sizeof(qtype);
      qtype = ntohs(qtype);

      memcpy(&qclass, pkt + off, sizeof(qclass));
      off += sizeof(qclass);
      qclass = ntohs(qclass);

      CLUSTERD_LOG(CLUSTERD_DEBUG, "Got query for %s (type=%u, class=%u)", qname, qtype, qclass);

      if ( qclass != DNS_CLASS_IN ) {
        CLUSTERD_LOG(CLUSTERD_WARNING, "Invalid DNS class %u", qclass);
        continue;
      }

      resolve(fd, &hdr, qname, qtype, qnameraw, qnamelen, &addr, addrlen);
    }

    return 0;
  }
}

static int change_user(const char *user, const char *group) {
  uid_t uid, origuid;
  gid_t gid, origgid;
  int err;

  origuid = uid = getuid();
  origgid = gid = getgid();

  if ( user ) {
    struct passwd *pw = getpwnam(user);
    if ( !pw ) {
      errno = ENOENT;
      return -1;
    }

    uid = pw->pw_uid;
    gid = pw->pw_gid;
  }

  if ( group ) {
    struct group *gr = getgrnam(group);
    if ( !gr ) {
      errno = ENOENT;
      return -1;
    }

    gid = gr->gr_gid;
  }

  if ( gid != origgid ) {
    err = setgid(gid);
    if ( err < 0 ) {
      CLUSTERD_LOG(CLUSTERD_ERROR, "Could not change group to %s: %s", group, strerror(errno));
      return -1;
    }
  }

  if ( uid != origuid ) {
    err = setuid(uid);
    if ( err < 0 ) {
      CLUSTERD_LOG(CLUSTERD_ERROR, "Could not change user to %s: %s", user, strerror(errno));
      return -1;
    }
  }

  return 0;
}

static void usage() {
  fprintf(stderr, "clusterd-dns -- provide DNS resolution for clusterd namespaces\n");
  fprintf(stderr, "Usage: clusterd-dns -tvh [-u USER] [-g GROUP] [-e ENDPOINT...]\n\n");
  fprintf(stderr, "   -e ENDPOINT    Bind to the given UDP endpoint (IPv4 or IPv6)\n");
  fprintf(stderr, "   -u USER        Switch to this user after binding the socket\n");
  fprintf(stderr, "   -g GROUP       Switch to this group after binding the socket\n");
  fprintf(stderr, "   -t             Require namespaces for endpoint resolution. Disable deduction\n");
  fprintf(stderr, "   -v             Show verbose debugging output\n");
  fprintf(stderr, "   -h             Show this help message\n\n");
  fprintf(stderr, "Please report bugs to support@f-omega.com\n");
}

int main(int argc, char *const *argv) {
  int c, err, i;
  const char *user = NULL, *group = NULL;

  setlocale(LC_ALL, "C");
  srandom(time(NULL));

  while ( (c = getopt(argc, argv, "e:u:g:tvh")) != -1 ) {
    switch ( c ) {
    case 'e':
      err = open_dns_socket(optarg, IPPROTO_UDP);
      if ( err < 0 ) {
        CLUSTERD_LOG(CLUSTERD_ERROR, "Invalid address for endpoint: %s", optarg);
        return 1;
      }

      break;

    case 'u':
      if ( user ) {
        CLUSTERD_LOG(CLUSTERD_ERROR, "-u must be given only once");
        return 1;
      }

      user = optarg;
      break;

    case 'g':
      if ( group ) {
        CLUSTERD_LOG(CLUSTERD_ERROR, "-g must be given only once");
        return 1;
      }

      group = optarg;
      break;

    case 't':
      g_deduce_namespace = 0;
      break;

    case 'h':
      usage();
      return 0;

    case 'v':
      CLUSTERD_LOG_LEVEL = CLUSTERD_DEBUG;
      break;

    default:
      CLUSTERD_LOG(CLUSTERD_ERROR, "Invalid option: %c", optopt);
      usage();
      return 1;
    }
  }

  if ( user || group ) {
    err = change_user(user, group);
    if ( err < 0 ) {
      CLUSTERD_LOG(CLUSTERD_ERROR, "Could not change user to %s:%s: %s", user, group, strerror(errno));
      return 1;
    }
  }

  for (;;) {
    fd_set rfds, wfds, efds;
    int maxfd = 0;

    FD_ZERO(&rfds);
    FD_ZERO(&efds);
    FD_ZERO(&wfds);

    for ( i = 0; i < g_endpoint_count; ++i ) {
      int fd = g_endpoints[i];

      if ( fd > maxfd )
        maxfd = fd;

      FD_SET(fd, &rfds);
      FD_SET(fd, &efds);
    }

    err = select(maxfd + 1, &rfds, &wfds, &efds, NULL);
    if ( err < 0 ) {
      if ( errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR ) continue;
      else {
        CLUSTERD_LOG(CLUSTERD_ERROR, "Could not select: %s", strerror(errno));
        return 1;
      }
    } else {
      for ( i = 0; i < g_endpoint_count; ++i ) {
        int fd = g_endpoints[i];

        if ( FD_ISSET(fd, &rfds) ) {
          err = process_dns_packet(fd);
          if ( err < 0 ) {
            CLUSTERD_LOG(CLUSTERD_WARNING, "Error receiving on socket %d: %s", fd, strerror(errno));
            g_endpoints[i] = -1;
            close(fd);
          }
        }

        if ( FD_ISSET(fd, &efds) ) {
          CLUSTERD_LOG(CLUSTERD_ERROR, "Error on socket %d", fd);
          g_endpoints[i] = -1;
          close(fd);
        }
      }
    }
  }

  return 0;
}
