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

#define CLUSTERD_COMPONENT "clusterd-endpointd"
#include "clusterd/log.h"
#include "clusterd/common.h"
#include "libclusterctl.h"

#include <getopt.h>
#include <locale.h>
#include <signal.h>
#include <time.h>
#include <pthread.h>
#include <byteswap.h>
#include <fcntl.h>
#include <uthash.h>
#include <regex.h>

#include <sys/types.h>
#include <sys/stat.h>

#include <libmnl/libmnl.h>
#include <linux/if_ether.h>
#include <linux/ipv6.h>
#include <linux/ip.h>
#include <linux/tcp.h>
#include <linux/udp.h>
#include <linux/netfilter.h>
#include <linux/netfilter/nfnetlink.h>
#include <linux/types.h>
#include <linux/netfilter/nfnetlink_queue.h>

#include <libnetfilter_queue/libnetfilter_queue.h>

#include <nftables/libnftables.h>

#define CLUSTERD_ENDPOINT_NETWORK_ADDR "fdf0:5f7b:911f:2"
#define CLUSTERD_ENDPOINT_PROCESS_ADDR "fdf0:5f7b:911f:1"

#define HIDWORD(x) ((uint16_t) (((x) >> 16) & 0xFFFF))
#define LODWORD(x) ((uint16_t) ((x) & 0xFFFF))

#define MAX_PENDING_PACKETS 1000
#define MAX_CLUSTERCTL_CONN_KEEPALIVE 30000 // Keep connections to clusterctl alive for 30 seconds at least
#define RULE_TTL_SECONDS 300

// TODO proper ipv6 CIDR
#define CLUSTERD_ENDPOINT_PREFIX_BYTELEN 8
static uint8_t CLUSTERD_ENDPOINT_PREFIX[CLUSTERD_ENDPOINT_PREFIX_BYTELEN] =
  { CLUSTERD_ENDPOINT_PREFIX_ADDR };

struct ep_key {
  clusterd_namespace_t ns_id;
  clusterd_endpoint_t endpoint_id;
};

struct pending_packet {
  uint32_t pkt_id;

  struct ep_key endpoint;
  int proto;
  uint16_t port;

  struct pending_packet *next;
};

struct mapped_endpoint {
  struct ep_key endpoint;
  struct timespec last_updated;
  int ttl, rule_handle;

  UT_hash_handle hh;
};

struct worker_data {
  int nftfd;
  pthread_barrier_t barrier;
};

int CLUSTERD_LOG_LEVEL = CLUSTERD_INFO;

static pthread_mutex_t g_packet_queue_mutex;
static pthread_cond_t g_packet_queue_cond;
static struct pending_packet *g_available_packets = NULL, *g_next_packet = NULL, *g_last_packet = NULL;;

static const char *g_table_name = "clusterd";
static const char *g_endpoint_set_name = "clusterd-endpoints";
static const char *g_namespace_set_name = "clusterd-namespaces";
static const char *g_tcp_vmap_name = "nsports-tcp";
static const char *g_udp_vmap_name = "nsports-udp";
static int g_queue_num = -1;

static sig_atomic_t g_should_terminate = 0;
static sig_atomic_t g_sighup = 0;

static struct mnl_socket *g_netlink_socket;

static pthread_mutex_t g_nft_mutex;

static pthread_mutex_t g_endpoint_mutex;
struct mapped_endpoint *g_endpoints = NULL;

static regex_t g_nft_handle_re;

static int send_verdict(uint32_t pkt_id, int verdict);
static int enqueue_packet(uint32_t pktid, clusterd_namespace_t nsid, clusterd_endpoint_t epid, uint16_t port, int proto);
static void free_packet(struct pending_packet *pkt);
static int run_nft_command(int nftfd, const char *cmdbuf, int cmdsz, int *rule_handle);

// Basic operation... read packets from queue. a new packet means a
// port that wasn't handled, so make that port work by looking it up
// in the cluster controller (use a new connection each time).
//
// On SIGHUP, flush all ports

static int bind_queue(struct mnl_socket *nl) {
  char buf[0xFFFF + (MNL_SOCKET_BUFFER_SIZE/2)];
  struct nlmsghdr *nlh;
  int err, yes = 1;

  // Configure bind message to queue
  nlh = nfq_nlmsg_put(buf, NFQNL_MSG_CONFIG, g_queue_num);
  nfq_nlmsg_cfg_put_cmd(nlh, AF_INET, NFQNL_CFG_CMD_BIND);

  err = mnl_socket_sendto(nl, nlh, nlh->nlmsg_len);
  if ( err < 0 ) return -1;

  // Copy the packet to userspace, but only copy enough for the TCP/IP or UDP/IP header
  nlh = nfq_nlmsg_put(buf, NFQNL_MSG_CONFIG, g_queue_num);

  // 4 is the size of the TCP and UDP header containing the source and destination ports
  nfq_nlmsg_cfg_put_params(nlh, NFQNL_COPY_PACKET,
			   sizeof(struct ethhdr) + sizeof(struct ipv6hdr) + 4);

  // Only receive whole packets
  mnl_attr_put_u32(nlh, NFQA_CFG_FLAGS, htonl(NFQA_CFG_F_GSO));
  mnl_attr_put_u32(nlh, NFQA_CFG_MASK, htonl(NFQA_CFG_F_GSO));

  err = mnl_socket_sendto(nl, nlh, nlh->nlmsg_len);
  if ( err < 0 ) return -1;

  // Turn off ENOBUFS when packets are dropped .... we don't care
  mnl_socket_setsockopt(nl, NETLINK_NO_ENOBUFS, &yes, sizeof(int));

  return 0;
}

static int endpoint_exists(clusterd_namespace_t nsid, clusterd_endpoint_t epid) {
  struct mapped_endpoint *ep = NULL;
  struct ep_key endpoint;

  memset(&endpoint, 0, sizeof(endpoint));
  endpoint.ns_id = nsid;
  endpoint.endpoint_id = epid;

  if ( pthread_mutex_lock(&g_endpoint_mutex) != 0 ) {
    CLUSTERD_LOG(CLUSTERD_WARNING, "Could not lookup endpoint " EP_F ": could not lock mutex", epid);
    return 0;
  }

  HASH_FIND(hh, g_endpoints, &endpoint, sizeof(endpoint), ep);
  if ( ep ) {
    struct timespec now, expiry;

    // TODO think about keeping a global clock, to avoid system calls
    if ( clock_gettime(CLOCK_MONOTONIC, &now) < 0 ) {
      CLUSTERD_LOG(CLUSTERD_WARNING, "Could not get time while looking up endpoint " EP_F ": %s",
                   epid, strerror(errno));
      ep = NULL;
      goto done;
    }

    // Check if the endpoint is still active
    memcpy(&expiry, &ep->last_updated, sizeof(struct timespec));
    timespec_add_ms(&expiry, ep->ttl);

    if ( timespec_cmp(&expiry, &now) <= 0 )
      ep = NULL; // Not found
  }

 done:
  if ( pthread_mutex_unlock(&g_endpoint_mutex) != 0 ) {
    CLUSTERD_LOG(CLUSTERD_WARNING, "Could not unlock endpoint mutex while looking up " EP_F, epid);
  }

  return (ep != NULL);
}

// If the endpoint was previously added, even if it was expired, this function removes it
static void delete_endpoint(int nftfd, clusterd_namespace_t nsid, clusterd_endpoint_t epid) {
  struct mapped_endpoint *ep = NULL;
  struct ep_key endpoint;

  memset(&endpoint, 0, sizeof(endpoint));
  endpoint.ns_id = nsid;
  endpoint.endpoint_id = epid;

  if ( pthread_mutex_lock(&g_endpoint_mutex) != 0 ) {
    CLUSTERD_LOG(CLUSTERD_WARNING, "Could not delete endpoint " EP_F ": could not lock mutex", epid);
    return;
  }

  HASH_FIND(hh, g_endpoints, &endpoint, sizeof(endpoint), ep);
  if ( ep ) {
    // Delete the rule
    HASH_DEL(g_endpoints, ep);
  }

  if ( pthread_mutex_unlock(&g_endpoint_mutex) != 0 ) {
    CLUSTERD_LOG(CLUSTERD_WARNING, "Could not delete endpoint " EP_F ": could not unlock mutex", epid);
  }

  if ( ep && ep->rule_handle >= 0 ) {
    // Run the NFTables command to delete the rule
    char cmdbuf[2048];
    size_t cmdsz;
    int err;

    cmdsz = snprintf(cmdbuf, sizeof(cmdbuf), "delete rule inet %s NAT handle %d",
                     g_table_name, ep->rule_handle);
    if ( cmdsz >= sizeof(cmdbuf) ) {
      CLUSTERD_LOG(CLUSTERD_WARNING, "Could not delete rule %d: buffer overrun", ep->rule_handle);
    } else {
      err = run_nft_command(nftfd, cmdbuf, cmdsz, NULL);
      if ( err != 0 ) {
        CLUSTERD_LOG(CLUSTERD_WARNING, "Could not delete rule %d: %s", ep->rule_handle, strerror(errno));
      }
    }

    free(ep);
  }
}

// This function ensures that the mapped_endpoint record for this
// endpoint is up-to-date (time is updated)
static void update_endpoint(clusterd_namespace_t nsid, clusterd_endpoint_t epid,
                            int ttl_ms, int newrulehdl, int *oldrulehdl) {
  struct mapped_endpoint *ep;
  struct ep_key endpoint;

  memset(&endpoint, 0, sizeof(endpoint));
  endpoint.ns_id = nsid;
  endpoint.endpoint_id = epid;

  if ( pthread_mutex_lock(&g_endpoint_mutex) != 0 ) {
    CLUSTERD_LOG(CLUSTERD_WARNING, "Could not update endpoint " EP_F ": could not lock mutex", epid);
    return;
  }

  HASH_FIND(hh, g_endpoints, &endpoint, sizeof(endpoint), ep);
  if ( !ep ) {
    ep = malloc(sizeof(struct mapped_endpoint));
    if ( ! ep ) {
      CLUSTERD_LOG(CLUSTERD_WARNING, "Could not allocate new endpoint");
      goto done;
    }

    memcpy(&ep->endpoint, &endpoint, sizeof(endpoint));
    ep->rule_handle = -1; // Invalid for now until the rule is added
    HASH_ADD(hh, g_endpoints, endpoint, sizeof(ep->endpoint), ep);
  }

  if ( clock_gettime(CLOCK_MONOTONIC, &ep->last_updated) < 0 ) {
    CLUSTERD_LOG(CLUSTERD_WARNING, "Could not get time while updating endpoint " EP_F ": %s",
                 epid, strerror(errno));
    goto done;
  }

  if ( ttl_ms >= 0 ) {
    ep->ttl = ttl_ms;
  }

  if ( oldrulehdl )
    *oldrulehdl = ep->rule_handle;

  if ( newrulehdl >= 0 )
    ep->rule_handle = newrulehdl;

 done:
  if ( pthread_mutex_unlock(&g_endpoint_mutex) != 0 ) {
    CLUSTERD_LOG(CLUSTERD_WARNING, "Could not unlock endpoint mutex while updating " EP_F, epid);
  }
}

static int init_nft_command() {
  int err;

  err = regcomp(&g_nft_handle_re, "# handle \\([0-9]\\+\\)", 0);
  if ( err != 0 ) {
    char errbuf[2048];
    size_t errsz;

    errsz = regerror(err, &g_nft_handle_re, errbuf, sizeof(errbuf));

    CLUSTERD_LOG(CLUSTERD_CRIT, "Could not compile NFT handle regex: %.*s", (unsigned int)errsz, errbuf);
    return -1;
  }

  return 0;
}

static int run_nft_command(int nftfd, const char *cmdbuf, int cmdsz, int *rule_handle) {
  int err, status, ret = 0;
  char respbuf[8192];
  ssize_t respsz;

  cmdsz ++; // Include nul byte

  if ( rule_handle ) *rule_handle = -1;

  if ( pthread_mutex_lock(&g_nft_mutex) != 0 ) {
    CLUSTERD_LOG(CLUSTERD_CRIT, "Could not lock nft mutex");
    return -1;
  }

 send_cmd_again:
  err = send(nftfd, cmdbuf, cmdsz, 0);
  if ( err < 0 ) {
    if ( errno == EINTR ) goto send_cmd_again;
    else {
      CLUSTERD_LOG(CLUSTERD_CRIT, "Could not send NFTables commands: %s", strerror(errno));
      return -1;
    }
  } else if ( err == 0 ) {
    CLUSTERD_LOG(CLUSTERD_CRIT, "NFTables thread exited");
    exit(EXIT_FAILURE);
  } else if ( err != cmdsz ) {
    CLUSTERD_LOG(CLUSTERD_WARNING, "Command was sent incomplete. Wanted %d bytes, but only sent %d",
                 cmdsz, err);
  } else
    CLUSTERD_LOG(CLUSTERD_DEBUG, "Commands submitted");

 recv_again:
  respsz = recv(nftfd, respbuf, sizeof(respbuf), 0);
  if ( respsz == 0 ) {
    CLUSTERD_LOG(CLUSTERD_CRIT, "NFTables thread exited");
    exit(EXIT_FAILURE);
  } else if ( respsz < 0 ) {
    if ( errno == EINTR || errno == EAGAIN ) goto recv_again;
    else {
      CLUSTERD_LOG(CLUSTERD_CRIT, "Could not receive NFTables output: %s", strerror(errno));
      return -1;
    }
  } else if ( respsz < sizeof(status) ) {
    CLUSTERD_LOG(CLUSTERD_CRIT, "Not enough data returned, needed %zu, got %zd",
                 sizeof(status), respsz);
    exit(EXIT_FAILURE);
  }

  memcpy(&status, respbuf, sizeof(status));

  CLUSTERD_LOG(CLUSTERD_INFO, "Got NFTables response (%s, size %ld) %.*s", status == 0 ? "success" : "error",
               respsz - sizeof(status), (int) (respsz - sizeof(status)), respbuf + sizeof(status));

  if ( rule_handle ) {
    regmatch_t matches[2];

    // NUL-terminate respbuf
    if ( respsz >= sizeof(respbuf) )
      respsz = sizeof(respbuf) - 1;

    respbuf[respsz] = '\0';

    err = regexec(&g_nft_handle_re, respbuf + sizeof(status), 2, matches, 0);
    if ( err == 0 &&
         matches[1].rm_so != -1 ) {
      unsigned int hdl;

      CLUSTERD_LOG(CLUSTERD_DEBUG, "Got raw rule handle %.*s",
                   (int) (matches[1].rm_eo - matches[1].rm_so),
                   respbuf + sizeof(status) + matches[1].rm_so);

      // Got a handle
      respbuf[sizeof(status) + matches[1].rm_eo] = '\0'; // NUL-terminate number
      err = sscanf(respbuf + sizeof(status) + matches[1].rm_so, "%u", &hdl);
      if ( err == 1 ) {
        CLUSTERD_LOG(CLUSTERD_DEBUG, "Got rule handle %u", hdl);
        *rule_handle = hdl;
      }
    }
  }

  if ( status != 0 ) {
    ret = -1;
  }

 done:
  if ( pthread_mutex_unlock(&g_nft_mutex) ) {
      CLUSTERD_LOG(CLUSTERD_CRIT, "Could not unlock nft mutex");
      exit(EXIT_FAILURE);
  }

  return ret;

}

static int apply_rule(int nftfd, clusterd_namespace_t nsid, clusterd_endpoint_t epid,
                      int proto, uint16_t port, char *rulebuf) {
  //  char *saveptr, *line;
  char cmdbuf[16*1024], endpointaddr[INET6_ADDRSTRLEN + 1], *save, *psaddr;
  int err, cmdpos, pscnt, i, natrulehdl, oldrulehdl = -1;

  const char *processes[128];

  update_endpoint(nsid, epid, RULE_TTL_SECONDS * 1000, -1, &oldrulehdl);

  CLUSTERD_LOG(CLUSTERD_DEBUG, "Got rule for epid=" EP_F ", proto=%d, port=%u:\n%s",
	       epid, proto, port, rulebuf);

  err = snprintf(endpointaddr, sizeof(endpointaddr),
                 CLUSTERD_ENDPOINT_NETWORK_ADDR ":%04x:%04x:%04x:%04x",
                 HIDWORD(nsid), LODWORD(nsid), HIDWORD(epid), LODWORD(epid));
  if ( err >= sizeof(endpointaddr) ) goto cmd_overflow;

  memset(processes, 0, sizeof(processes));
  for ( pscnt = 0, psaddr = strtok_r(rulebuf, "\n", &save);
        psaddr && pscnt < (sizeof(processes)/sizeof(processes[0]));
        psaddr = strtok_r(NULL, "\n", &save) ) {
    processes[pscnt++] = psaddr;
  }

  if ( pscnt == 0 ) {
    CLUSTERD_LOG(CLUSTERD_WARNING, "Endpoint " EP_F " had no processes associated with it. Doing nothing",
                 epid);
    return 0;
  }

  // Create a new verdict map named after the endpoint, if needed
#define WRITE_BUFFER(s, ...) do {                                       \
    err = snprintf(cmdbuf + cmdpos, sizeof(cmdbuf) - cmdpos, s VA_ARGS (__VA_ARGS__)); \
    if ( err >= (sizeof(cmdbuf) - cmdpos) ) goto cmd_overflow;          \
    cmdpos += err;                                                      \
  } while (0)
#define RESET_BUFFER() cmdpos = 0

  RESET_BUFFER();
  if ( oldrulehdl >= 0 ) {
    WRITE_BUFFER("replace rule inet %s NAT handle %d ", g_table_name, oldrulehdl);
  } else {
    WRITE_BUFFER("add rule inet %s NAT ", g_table_name);
  }

  if ( pscnt > 1 ) {
    WRITE_BUFFER("ip6 daddr %s dnat ip6 to jhash ip6 saddr . tcp sport mod %d map {",
                 endpointaddr, pscnt);
    for ( i = 0; i < pscnt; ++i ) {
      WRITE_BUFFER("%s%d: %s", i == 0 ? "" : ", ",  i, processes[i]);
    }
    WRITE_BUFFER("}\n");
  } else {
    WRITE_BUFFER("ip6 daddr %s dnat ip6 to %s\n",
                 endpointaddr, processes[0]);
  }

  if ( run_nft_command(nftfd, cmdbuf, cmdpos, &natrulehdl) < 0 ) {
    CLUSTERD_LOG(CLUSTERD_CRIT, "Could not add nftables rule: %s", strerror(errno));
    return -1;
  }

  RESET_BUFFER();
  // Add endpoint address to the set
  WRITE_BUFFER("add element inet %s %s { %s timeout %ds }\n", g_table_name, g_endpoint_set_name,
               endpointaddr, RULE_TTL_SECONDS);

  if ( run_nft_command(nftfd, cmdbuf, cmdpos, NULL) < 0 ) {
    CLUSTERD_LOG(CLUSTERD_CRIT, "Could not add endpoint to endpoint set: %s", strerror(errno));
    return -1;
  }

  update_endpoint(nsid, epid, -1, natrulehdl, NULL);

  return 0;

 cmd_overflow:
  CLUSTERD_LOG(CLUSTERD_WARNING, "Could not write rules for %s: buffer overflow", endpointaddr);
  errno = ENOBUFS;
  return -1;
}

static int find_and_apply_rule(int nftfd, clusterctl *ctl, clusterd_namespace_t nsid,
                               clusterd_endpoint_t epid, int proto, uint16_t port) {
  static const char *find_endpoint_lua =
    "ep = clusterd.get_endpoint(params.namespace, params.endpoint)\n"
    "if ep == nil then\n"
    "  error('could not find endpoint')\n"
    "end\n"
    "\n"
    "psmap = ''\n"
    "for _, p in ipairs(ep.claims) do\n"
    "  ps = clusterd.resolve_process(ep.namespace, p.process)\n"
    "  if ps ~= nil AND ps.ip ~= nil then\n"
    "    psmap = psmap .. ps.ip .. '\\n'\n"
    "  end\n"
    "end\n"
    "clusterd.output(psmap);\n";

  // Now we look up the port mapping by ID, proto, and port
  char rulebuf[16 * 1024];
  int err;

  char *protostr = NULL, epidstr[32], nsidstr[32];

  switch ( proto ) {
  case IPPROTO_TCP:
    protostr = "tcp";
    break;

  case IPPROTO_UDP:
    protostr = "udp";
    break;
  }

  if ( !protostr ) {
    errno = EINVAL;
    return -1;
  }

  err = snprintf(epidstr, sizeof(epidstr), EP_F, epid);
  if ( err >= sizeof(epidstr) ) {
    errno = ENOBUFS;
    return -1;
  }

  err = snprintf(nsidstr, sizeof(nsidstr), NS_F, nsid);
  if ( err >= sizeof(nsidstr) ) {
    errno = ENOBUFS;
    return -1;
  }

  CLUSTERD_LOG(CLUSTERD_DEBUG, "Looking up endpoint %s", epidstr);

  err = clusterctl_call_simple(ctl, CLUSTERCTL_STALE_READS,
			       find_endpoint_lua,
			       rulebuf, sizeof(rulebuf),

			       "namespace", nsidstr,
			       "endpoint", epidstr,
			       "proto", protostr,
			       NULL);
  if ( err < 0 ) {
    CLUSTERD_LOG(CLUSTERD_CRIT, "Could not lookup rule (ns=" NS_F ", endpoint=" EP_F ", proto=%s, port=%u)",
		 nsid, epid, protostr, port);
    delete_endpoint(nftfd, nsid, epid);
    return -1;
  }

  // Each line contains a process that could receive data from this endpoint
  return apply_rule(nftfd, nsid, epid, proto, port, rulebuf);
}

static void *packet_processor_worker(void *data) {
  // Block all signals in this thread
  int err, is_ctl_open = 0, verdict, nftfd;
  struct pending_packet *packet = NULL;
  clusterctl ctl;

  struct worker_data *wdata = (struct worker_data *)data;

  nftfd = wdata->nftfd;

  err = pthread_barrier_wait(&wdata->barrier);
  if ( err == PTHREAD_BARRIER_SERIAL_THREAD ) {
    pthread_barrier_destroy(&wdata->barrier);
    free(wdata);
  }

  for (;;) {
    if ( pthread_mutex_lock(&g_packet_queue_mutex) != 0 ) {
      CLUSTERD_LOG(CLUSTERD_WARNING, "Could not acquire packet mutex.. exiting worker");
      return NULL;
    }

    if ( packet ) {
      // If we completed processing a packet, add it in to the available queue
      packet->next = g_available_packets;
      g_available_packets = packet;
    }

    while ( !g_next_packet ) {
      struct timespec wakeup_time;

      err = clock_gettime(CLOCK_REALTIME, &wakeup_time);
      if ( err < 0 ) {
	CLUSTERD_LOG(CLUSTERD_CRIT, "Could not get time");
	return NULL;
      }

      if ( is_ctl_open )
	wakeup_time.tv_sec += MAX_CLUSTERCTL_CONN_KEEPALIVE;
      else
	wakeup_time.tv_sec += 86400; // One day

      err = pthread_cond_timedwait(&g_packet_queue_cond, &g_packet_queue_mutex, &wakeup_time);
      if ( err != 0 ) {
	if ( errno == ETIMEDOUT ) {
	  clusterctl_close(&ctl);
	  is_ctl_open = 0;
	} else {
	  CLUSTERD_LOG(CLUSTERD_DEBUG, "Could not wait for next packet: %s", strerror(errno));
	}
      }
    }

    packet = g_next_packet;
    g_next_packet = g_next_packet->next;
    if ( !g_next_packet )
      g_last_packet = NULL;

    if ( pthread_mutex_unlock(&g_packet_queue_mutex) != 0 ) {
      CLUSTERD_LOG(CLUSTERD_CRIT, "Could not release packet mutex after acquisition");
      exit(EXIT_FAILURE);
    }

    // Process packet
    CLUSTERD_LOG(CLUSTERD_DEBUG, "Processing packet (id=%u, ns=" NS_F ", endpoint=" EP_F ", proto=%d, port=%u)",
		 packet->pkt_id, packet->endpoint.ns_id, packet->endpoint.endpoint_id, packet->proto, packet->port);

    verdict = NF_ACCEPT;

    // Acquire a clusterctl object and query the endpoint data
    if ( !is_ctl_open ) {
      err = clusterctl_open(&ctl);
      if ( err < 0 ) {
	CLUSTERD_LOG(CLUSTERD_WARNING, "Could not open clusterctl connection. Dropping packets");
	verdict = NF_DROP;
	goto apply_verdict;
      }

      is_ctl_open = 1;
    }

    // Now, request the rule be added
    err = find_and_apply_rule(nftfd, &ctl, packet->endpoint.ns_id, packet->endpoint.endpoint_id,
                              packet->proto, packet->port);
    if ( err < 0 ) {
      CLUSTERD_LOG(CLUSTERD_DEBUG, "Could not find mapping for port (ns=" NS_F ", ep=" EP_F ", proto=%d, port=%u)",
		   packet->endpoint.ns_id, packet->endpoint.endpoint_id, packet->proto, packet->port);
      verdict = NF_DROP;
      goto apply_verdict;
    } else
      verdict = NF_ACCEPT;

  apply_verdict:
    err = send_verdict(packet->pkt_id, verdict);
    if ( err < 0 ) {
      CLUSTERD_LOG(CLUSTERD_WARNING, "Could not send verdict for packet with id %u: %s", packet->pkt_id, strerror(errno));
    }

    free_packet(packet);
  }
}

static int enqueue_packet(uint32_t pktid, clusterd_namespace_t nsid, clusterd_endpoint_t epid,
                          uint16_t port, int proto) {
  int ret = 0;

  if ( pthread_mutex_lock(&g_packet_queue_mutex) != 0 )
    return -1;

  if ( g_available_packets ) {
    struct pending_packet *next_packet = g_available_packets;

    g_available_packets = next_packet->next;

    if ( g_last_packet )
      g_last_packet->next = next_packet;
    else
      g_next_packet = g_last_packet = next_packet;

    next_packet->pkt_id = pktid;
    next_packet->endpoint.ns_id = nsid;
    next_packet->endpoint.endpoint_id = epid;
    next_packet->proto = proto;
    next_packet->port = port;

    next_packet->next = NULL;

    if ( pthread_cond_signal(&g_packet_queue_cond) != 0 ) {
      CLUSTERD_LOG(CLUSTERD_WARNING, "Added packet to queue but could not wake any threads");
      ret = -1;
    }
  } else {
    errno = ENOMEM;
    ret = -1;
  }

  if ( pthread_mutex_unlock(&g_packet_queue_mutex) != 0 ) {
    CLUSTERD_LOG(CLUSTERD_CRIT, "Could not unlock packet mutex after use");
    exit(EXIT_FAILURE);
  }

  return ret;
}

static void free_packet(struct pending_packet *pkt) {
  if ( pthread_mutex_lock(&g_packet_queue_mutex) < 0 ) {
    CLUSTERD_LOG(CLUSTERD_WARNING, "Could not free packet %u: %s", pkt->pkt_id, strerror(errno));
    return;
  }

  pkt->next = g_available_packets;
  g_available_packets = pkt;

  if ( pthread_mutex_unlock(&g_packet_queue_mutex) < 0 ) {
    CLUSTERD_LOG(CLUSTERD_CRIT, "Could not unlock packet mutex while freeing packet %u", pkt->pkt_id);
    exit(EXIT_FAILURE);
  }
}

static int send_verdict(uint32_t pkt_id, int verdict) {
  char buf[MNL_SOCKET_BUFFER_SIZE];
  struct nlmsghdr *nlh;
  struct nlattr *nest;

  CLUSTERD_LOG(CLUSTERD_DEBUG, "Sending verdict for %u: %s (%d)", pkt_id,
	       verdict == NF_DROP ? "NF_DROP" : (verdict == NF_ACCEPT ? "NF_ACCEPT" : "(unknown)"),
	       verdict);

  nlh = nfq_nlmsg_put(buf, NFQNL_MSG_VERDICT, g_queue_num);
  nfq_nlmsg_verdict_put(nlh, pkt_id, verdict);

  if ( mnl_socket_sendto(g_netlink_socket, nlh, nlh->nlmsg_len) < 0 ) {
    CLUSTERD_LOG(CLUSTERD_WARNING, "Could not send verdict for %u: %s", pkt_id, strerror(errno));
    return -1;
  }

  return 0;
}

static int extract_namespace(unsigned char *addr, clusterd_namespace_t *ns) {
  uint32_t netns;

  memcpy(&netns, addr + CLUSTERD_ENDPOINT_PREFIX_BYTELEN, sizeof(netns));

  netns = ntohl(netns);
  *ns = netns;

  return 0;
}

static int process_packet(struct nfqnl_msg_packet_hdr *ph, uint32_t phlen,
			  void *p, uint32_t plen) {

  uint16_t proto;
  int verdict = NF_DROP, err;
  uint32_t id;

  id = ntohl(ph->packet_id);

  proto = ntohs(ph->hw_protocol);
  if ( proto == ETH_P_IPV6 ) {
    struct ipv6hdr hdr;
    memcpy(&hdr, p, sizeof(hdr));

    CLUSTERD_LOG(CLUSTERD_DEBUG, "Got IPv6 packet with IP protocol %x (length %u)", hdr.nexthdr, plen);

    // Check if the ip address prefix matches
    if ( memcmp(hdr.daddr.s6_addr, CLUSTERD_ENDPOINT_PREFIX, CLUSTERD_ENDPOINT_PREFIX_BYTELEN) == 0 ) {
      clusterd_endpoint_t epid;
      clusterd_namespace_t nsid;

      struct udphdr udp;
      struct tcphdr tcp;

      memcpy(&epid, hdr.daddr.s6_addr + CLUSTERD_ENDPOINT_PREFIX_BYTELEN + 4, 4);
      epid = ntohl(epid);

      err = extract_namespace(hdr.daddr.s6_addr, &nsid);
      if ( err < 0 ) {
        verdict = NF_DROP;
        CLUSTERD_LOG(CLUSTERD_ERROR, "Could not extract namespace");
        goto done;
      }

      switch ( hdr.nexthdr ) {
      case IPPROTO_ICMPV6:
	/* Verify that any endpoint exists for the given address, and if so, send pings back */
	CLUSTERD_LOG(CLUSTERD_DEBUG, "Got ICMP6 packet");
	verdict = NF_ACCEPT;
	break;

      case IPPROTO_TCP:
	memcpy(&tcp, (void *) (p + sizeof(hdr)), sizeof(tcp));

	/* Verify that the endpoint exists with the given port */
	CLUSTERD_LOG(CLUSTERD_DEBUG, "Got TCP packet to port %u", ntohs(tcp.dest));
        /* Check if the endpoint is already mapped */
        if ( endpoint_exists(nsid, epid) )
          verdict = NF_ACCEPT;
        else {
          if ( enqueue_packet(id, nsid, epid, ntohs(tcp.dest), IPPROTO_TCP) == 0 ) {
            verdict = -1;
          } else {
            CLUSTERD_LOG(CLUSTERD_DEBUG, "Could not enqueue TCP packet: %s", strerror(errno));
            verdict = NF_DROP;
          }
        }
	break;

      case IPPROTO_UDP:
	memcpy(&udp, (void *) (p + sizeof(hdr)), sizeof(udp));

	/* Same as TCP */
	if ( endpoint_exists(nsid, epid) )
          verdict = NF_ACCEPT;
        else {
          if ( enqueue_packet(id, nsid, epid, ntohs(udp.dest), IPPROTO_UDP) == 0 )
            verdict = -1;
          else
            verdict = NF_DROP;
        }
	break;

      default:
	CLUSTERD_LOG(CLUSTERD_DEBUG, "Unknown packet type");
	break;
      }
    } else {
      clusterd_namespace_t ext;

      err = extract_namespace(hdr.daddr.s6_addr, &ext);
      if ( err < 0 ) {
        verdict = NF_DROP;
      } else {
        CLUSTERD_LOG(CLUSTERD_DEBUG, "Got packet destined for external namespace" NS_F, ext);
      }

      // Set up rule to accept these packets. At this point we should
      // also call the external namespace program, if any, to set up
      // routes.
//      if ( g_external_ns_cmd ) {
//      }
    }
  } else
    CLUSTERD_LOG(CLUSTERD_DEBUG, "Packet received in unknown protocol %x", proto);

 done:
  if ( verdict >= 0 ) {
    err = send_verdict(id, verdict);
    if ( err < 0 ) {
      CLUSTERD_LOG(CLUSTERD_WARNING, "Could not send immediate verdict for packet %u: %s", id, strerror(errno));
    }
  }

  return MNL_CB_OK;
}

static int queue_cb(const struct nlmsghdr *nlh, void *data) {
  int err;
  struct nfqnl_msg_packet_hdr *ph = NULL;
  uint16_t plen;
  struct nlattr *attr[NFQA_MAX+1] = {};
  struct nfgenmsg *nfg;
  uint32_t truncated = 0;
  uint32_t id = 0, skbinfo;

  void *payload;
  uint32_t payload_len;

  err = nfq_nlmsg_parse(nlh, attr);
  if ( err < 0 ) {
    CLUSTERD_LOG(CLUSTERD_WARNING, "Could not process netfilter queue message: %s", strerror(errno));
    return MNL_CB_ERROR;
  }

  nfg = mnl_nlmsg_get_payload(nlh);
  if ( attr[NFQA_PACKET_HDR] == NULL ) {
    CLUSTERD_LOG(CLUSTERD_WARNING, "Packet metaheader not set");
    return MNL_CB_ERROR;
  }

  if ( attr[NFQA_PAYLOAD] == NULL ) {
    CLUSTERD_LOG(CLUSTERD_WARNING, "No packet data present");
    return MNL_CB_ERROR;
  }

  ph = mnl_attr_get_payload(attr[NFQA_PACKET_HDR]);
  plen = mnl_attr_get_payload_len(attr[NFQA_PACKET_HDR]);

  skbinfo = attr[NFQA_SKB_INFO] ? ntohl(mnl_attr_get_u32(attr[NFQA_SKB_INFO])) : 0;

  if ( attr[NFQA_CAP_LEN] ) {
    uint32_t orig_sz = ntohl(mnl_attr_get_u32(attr[NFQA_CAP_LEN]));
    truncated = orig_sz;
  } else
    truncated = plen;

  id = ntohl(ph->packet_id);
  CLUSTERD_LOG(CLUSTERD_DEBUG, "Got packet with id %u of size %u", ph->packet_id, payload_len);

  payload = mnl_attr_get_payload(attr[NFQA_PAYLOAD]);
  payload_len = mnl_attr_get_payload_len(attr[NFQA_PAYLOAD]);

  return process_packet(ph, plen, payload, payload_len);
}

static void usage() {
  fprintf(stderr, "clusterd-endpointd -- manage an on-demand clusterd endpoint IP mapping using netfilter\n");
  fprintf(stderr, "Usage: clusterd-endpointd -vh [-t TABLE] [-N THCNT] [-l QLEN] -s NSFILE -q QUEUENUM\n\n");
  fprintf(stderr, "   -t TABLE     The netfilter table that contains the TCP and UDP port maps\n");
  fprintf(stderr, "   -l QLEN      Number of packets that can be enqueued at the same time (100 by default)\n");
  fprintf(stderr, "   -N THCNT     Number of threads that run to serve packet requests\n");
  fprintf(stderr, "   -s NSFILE    Name of network namespace file\n");
  fprintf(stderr, "   -q QUEUENUM  The netfilter queue which receives traffic for unknown TCP/UDP ports\n");
  fprintf(stderr, "   -d           Daemonize after starting\n");
  fprintf(stderr, "   -v           Show verbose debug output\n");
  fprintf(stderr, "   -h           Show this help message\n\n");
  fprintf(stderr, "Please report bugs to support@f-omega.com\n");
}

static void term_handler(int signal) {
  g_should_terminate = 1;
}

static void hup_handler(int signal) {
  g_sighup = 1;
}

static void reset_tables() {
  CLUSTERD_LOG(CLUSTERD_WARNING, "TODO should dump tables");
}

static ssize_t send_fds(int fd, void *data, size_t datalen,
                        int *fds, unsigned int fdcnt) {
  struct msghdr msg;
  struct iovec iov;
  struct cmsghdr *cmsg;
  char fdbuf[CMSG_SPACE(sizeof(int) * fdcnt)];

  iov.iov_base = data;
  iov.iov_len = datalen;

  msg.msg_name = NULL;
  msg.msg_namelen = 0;
  msg.msg_iov = &iov;
  msg.msg_iovlen = 1;
  msg.msg_control = fdbuf;
  msg.msg_controllen = CMSG_SPACE(sizeof(int) * fdcnt);
  msg.msg_flags = 0;

  cmsg = CMSG_FIRSTHDR(&msg);

  cmsg->cmsg_len = CMSG_LEN(sizeof(int) * fdcnt);
  cmsg->cmsg_level = SOL_SOCKET;
  cmsg->cmsg_type = SCM_RIGHTS;
  memcpy(CMSG_DATA(cmsg), fds, sizeof(int) * fdcnt);

  return sendmsg(fd, &msg, 0);
}

static ssize_t recv_fds(int fd, void *data, size_t datalen,
                        int *fds, unsigned int *fdcnt) {
  struct msghdr msg;
  struct iovec iov;
  struct cmsghdr *cmsg;
  char fdbuf[CMSG_SPACE(sizeof(int) * *fdcnt)];
  ssize_t ret;
  int err, maxfds = *fdcnt;

  iov.iov_base = data;
  iov.iov_len = datalen;

  msg.msg_name = NULL;
  msg.msg_namelen = 0;
  msg.msg_iov = &iov;
  msg.msg_iovlen = 1;
  msg.msg_control = fdbuf;
  msg.msg_controllen = CMSG_SPACE(sizeof(int) * *fdcnt);
  msg.msg_flags = 0;

  ret = recvmsg(fd, &msg, 0);
  if ( ret < 0 ) {
    *fdcnt = 0;
    return -1;
  }

  *fdcnt = 0;
  for ( cmsg = CMSG_FIRSTHDR(&msg);
        cmsg;
        cmsg = CMSG_NXTHDR(&msg, cmsg) ) {
    if ( cmsg->cmsg_level == SOL_SOCKET &&
         cmsg->cmsg_type == SCM_RIGHTS ) {
      size_t fdbuflen = cmsg->cmsg_len - sizeof(*cmsg);
      int fdspresent = fdbuflen / sizeof(*fds);

      *fdcnt = fdspresent;
      if ( *fdcnt > maxfds )
        *fdcnt = maxfds;

      memcpy(fds, CMSG_DATA(cmsg), sizeof(int) * *fdcnt);
      goto done;
    }
  }

 done:
  return ret;
}

static int nft_worker(int workfd, struct nft_ctx *nft) {
  for (;;) {
    int err;
    char cmdbuf[16 * 1024];
    ssize_t cmdsz = recv(workfd, cmdbuf, sizeof(cmdbuf), 0);


    if ( cmdsz < 0 ) {
      if ( errno == EINTR ) continue;
      else if ( errno == EPIPE || errno == ENOTCONN ) return 0;
      else {
        CLUSTERD_LOG(CLUSTERD_CRIT, "NFT worker exiting: recv() returns %s", strerror(errno));
      }
    } else if ( cmdsz == 0 ) return 0;
    else {
      int status = 0;
      struct iovec respv[2] = {
        { .iov_base = &status, .iov_len = sizeof(status) },
        { .iov_base = NULL, .iov_len = 0 }
      };
      struct msghdr msg;

      CLUSTERD_LOG(CLUSTERD_DEBUG, "Running command %.*s", (int) cmdsz, cmdbuf);

      err = nft_ctx_buffer_output(nft);
      if ( err != 0 ) {
        CLUSTERD_LOG(CLUSTERD_CRIT, "Could not enable nft buffering");
        goto respond_error;
      }

      err = nft_run_cmd_from_buffer(nft, cmdbuf);
      if ( err != 0 ) {
        CLUSTERD_LOG(CLUSTERD_WARNING, "Could not run command %s", cmdbuf);
        goto respond_error;
      }

      status = 0;
      respv[1].iov_base = (void *) nft_ctx_get_output_buffer(nft);

      goto respond;

    respond_error:
      status = 1;
      respv[1].iov_base = (void *) nft_ctx_get_error_buffer(nft);

    respond:
      respv[1].iov_len = strlen(respv[1].iov_base);
      CLUSTERD_LOG(CLUSTERD_DEBUG, "Got output of size %ld", respv[1].iov_len);

      msg.msg_name = NULL;
      msg.msg_namelen = 0;
      msg.msg_iov = respv;
      msg.msg_iovlen = 2;
      msg.msg_control = NULL;
      msg.msg_controllen = 0;
      msg.msg_flags = 0;

      err = sendmsg(workfd, &msg, 0);
      if ( err == 0 ) {
        CLUSTERD_LOG(CLUSTERD_INFO, "Main worker process exited, not returning nftables response");
        exit(EXIT_SUCCESS);
      } else if ( err < 0 ) {
        CLUSTERD_LOG(CLUSTERD_WARNING, "Could not return response to worker: %s", strerror(errno));
        exit(EXIT_FAILURE);
      }
    }
  }
}

static int reset_child_streams() {
  int nullfd, err;

  nullfd = open("/dev/null", O_RDONLY);
  if ( nullfd < 0 ) {
    CLUSTERD_LOG(CLUSTERD_CRIT, "Could not open /dev/null: %s", strerror(errno));
    return -1;
  }

  err = dup2(nullfd, STDIN_FILENO);
  if ( err < 0 ) {
    CLUSTERD_LOG(CLUSTERD_CRIT, "Could not redirect stdin to /dev/null: %s", strerror(errno));
    return -1;
  }

  close(nullfd);

  return 0;
}

static struct mnl_socket *start_ns_helper(const char *ns_file, int *nftfd) {
  // Create a new process in the given namespace
  int nsfd, err, serr, sts[2];
  unsigned char csts;
  pid_t child;

  nsfd = open(ns_file, 0);
  if ( nsfd < 0 ) {
    CLUSTERD_LOG(CLUSTERD_CRIT, "Could not open namespace %s: %s", ns_file, strerror(errno));
    return NULL;
  }

  err = socketpair(AF_UNIX, SOCK_SEQPACKET, 0, sts);
  if ( err < 0 ) {
    CLUSTERD_LOG(CLUSTERD_CRIT, "Could not open status pipe: %s", strerror(errno));
    close(nsfd);
    return NULL;
  }

  child = fork();
  if ( child < 0 ) {
    CLUSTERD_LOG(CLUSTERD_CRIT, "Could not fork(): %s", strerror(errno));
    close(nsfd);
    close(sts[0]);
    close(sts[1]);
    return NULL;
  } else if ( child == 0 ) {
    int nl_fd;
    struct mnl_socket *nl;
    struct nft_ctx *nft;
    close(sts[0]);

    err = reset_child_streams();
    if ( err < 0 ) {
      CLUSTERD_LOG(CLUSTERD_CRIT, "Could not re-open streams in child");
      goto report_error;
    }

    err = setns(nsfd, CLONE_NEWNET);
    if ( err < 0 ) {
      CLUSTERD_LOG(CLUSTERD_CRIT, "Could not enter namespace %s: %s", ns_file, strerror(errno));
      goto report_error;
    }
    close(nsfd);

    nft = nft_ctx_new(NFT_CTX_DEFAULT);
    if ( ! nft ) {
      CLUSTERD_LOG(CLUSTERD_CRIT, "Could not create nftables context in worker: %s", strerror(errno));
      goto report_error;
    }

    err = nft_ctx_buffer_output(nft);
    if ( err < 0 ) {
      CLUSTERD_LOG(CLUSTERD_CRIT, "Could not enable buffering output in worker");
      goto report_error;
    }

    err = nft_ctx_buffer_error(nft);
    if ( err < 0 ) {
      CLUSTERD_LOG(CLUSTERD_CRIT, "Could not enable error buffering in worker");
      goto report_error;
    }

    // Ask for the handle to be output
    nft_ctx_output_set_flags(nft,
                             NFT_CTX_OUTPUT_HANDLE | NFT_CTX_OUTPUT_ECHO);

    // Now attempt to open the netlink socket

    nl = mnl_socket_open(NETLINK_NETFILTER);
    if ( !nl ) {
      CLUSTERD_LOG(CLUSTERD_CRIT, "Could not open netlink socket: %s", strerror(errno));
      goto report_error;
    }

    err = mnl_socket_bind(nl, 0, MNL_SOCKET_AUTOPID);
    if ( err < 0 ) {
      CLUSTERD_LOG(CLUSTERD_ERROR, "Could not bind netfilter socket: %s", strerror(errno));
      goto report_error;
    }

    err = bind_queue(nl);
    if ( err < 0 ) {
      CLUSTERD_LOG(CLUSTERD_ERROR, "Could not bind netfilter queue: %s", strerror(errno));
      goto report_error;
    }

    csts = 0;
    nl_fd = mnl_socket_get_fd(nl);
    err = send_fds(sts[1], &csts, 1, &nl_fd, 1);
    if ( err != 1 ) {
      CLUSTERD_LOG(CLUSTERD_CRIT, "Could not report success: %s", strerror(errno));
      exit(EXIT_FAILURE);
    }

    exit(nft_worker(sts[1], nft));
  } else {
    unsigned char childsts;
    int nl_fd;
    unsigned int fdcnt = 1;

    close(sts[1]);
    close(nsfd);

    err = recv_fds(sts[0], &childsts, 1, &nl_fd, &fdcnt);
    if ( err != 1 ) {
      CLUSTERD_LOG(CLUSTERD_CRIT, "Could not recv child status: %s", strerror(errno));
      close(sts[0]);
      return NULL;
    }

    if ( childsts == 0 ) {
      if ( fdcnt != 1 ) {
        CLUSTERD_LOG(CLUSTERD_CRIT, "Did not get netlink file from child");
        close(sts[0]);
        return NULL;
      }

      // Started successfully
      *nftfd = sts[0];
      return mnl_socket_fdopen(nl_fd);
    } else {
      int childerr;

      err = recv(sts[0], &childerr, sizeof(childerr), 0);
      if ( err != sizeof(childerr) ) {
        CLUSTERD_LOG(CLUSTERD_CRIT, "Could not recv child error: %s", strerror(errno));
        close(sts[0]);
        return NULL;
      }

      CLUSTERD_LOG(CLUSTERD_DEBUG, "Child process failed, reporting error");

      errno = childerr;
      return NULL;
    }
  }

 report_error:
  serr = errno;
  csts = 1;

  err = send(sts[1], &csts, 1, 0);
  if ( err != 1 ) {
    CLUSTERD_LOG(CLUSTERD_CRIT, "Could not report error: %s", strerror(errno));
    exit(EXIT_FAILURE);
  }

  err = send(sts[1], &serr, sizeof(serr), 0);
  if ( err != sizeof(serr) ) {
    CLUSTERD_LOG(CLUSTERD_CRIT, "Could not report error: %s", strerror(errno));
    exit(EXIT_FAILURE);
  }

  exit(EXIT_FAILURE);
}

static int allocate_packet_queue(int qlen) {
  struct pending_packet *pkts;
  int i;

  if ( qlen < 5 ) {
    CLUSTERD_LOG(CLUSTERD_WARNING, "Queue length %d might be too short", qlen);
  }

  pkts = calloc(qlen, sizeof(*pkts));
  if ( !pkts )
    return -1;

  for ( i = 0; i < qlen; ++i ) {
    if ( i < (qlen - 1) )
      pkts[i].next = pkts + i + 1;
  }

  g_available_packets = pkts;

  return 0;
}

static int start_threads(int thcnt, int nftfd) {
  int i, n = 0, err;
  struct worker_data *data;

  CLUSTERD_LOG(CLUSTERD_DEBUG, "Starting %d thread(s) for thread pool", thcnt);

  data = malloc(sizeof(*data));
  if ( !data ) {
    CLUSTERD_LOG(CLUSTERD_CRIT, "Could not allocate thread data");
    return -1;
  }

  err = pthread_mutex_init(&g_nft_mutex, NULL);
  if ( err != 0 ) {
    CLUSTERD_LOG(CLUSTERD_CRIT, "Could not create NFTables mutex: %s", strerror(errno));
    free(data);
    return -1;
  }

  err = pthread_mutex_init(&g_endpoint_mutex, NULL);
  if ( err != 0 ) {
    CLUSTERD_LOG(CLUSTERD_CRIT, "Could not create endpoint mutex: %s", strerror(errno));
    free(data);
    return -1;
  }

  err = pthread_mutex_init(&g_packet_queue_mutex, NULL);
  if ( err != 0 ) {
    CLUSTERD_LOG(CLUSTERD_CRIT, "Could not create packet queue mutex: %s", strerror(errno));
    free(data);
    return -1;
  }

  err = pthread_cond_init(&g_packet_queue_cond, NULL);
  if ( err != 0 ) {
    CLUSTERD_LOG(CLUSTERD_CRIT, "Could not create packet queue condition: %s", strerror(errno));
    free(data);
    return -1;
  }

  data->nftfd = nftfd;

  err = pthread_barrier_init(&data->barrier, NULL, thcnt);
  if ( err != 0 ) {
    CLUSTERD_LOG(CLUSTERD_CRIT, "Could not create barrier: %s", strerror(errno));
    free(data);
    return -1;
  }

  for ( i = 0; i < thcnt; ++i ) {
    pthread_t hdl;

    err = pthread_create(&hdl, NULL, packet_processor_worker, data);
    if ( err < 0 ) {
      free(data);
      CLUSTERD_LOG(CLUSTERD_CRIT, "Could not launch a thread in thread pool: %s", strerror(errno));
      return -1;
    }
  }

  return 0;
}

static void setup_daemon() {
  pid_t child, sessid, grandchild;

  child = fork();
  if ( child < 0 ) {
    CLUSTERD_LOG(CLUSTERD_ERROR, "Could not daemonize: %s", strerror(errno));
    exit(EXIT_FAILURE);
  }

  if ( child == 0 ) {
    // We are the child
    sessid = setsid();
    if ( sessid < 0 ) {
      CLUSTERD_LOG(CLUSTERD_ERROR, "Could not create session: %s", strerror(errno));
      exit(EXIT_FAILURE);
    }

    grandchild = fork();
    if ( grandchild < 0 ) {
      CLUSTERD_LOG(CLUSTERD_ERROR, "Could not create grandchild: %s", strerror(errno));
      exit(EXIT_FAILURE);
    }

    if ( grandchild == 0 ) {
      // Reset stdin/stdout/stderr
      int nullfd, err;

      nullfd = open("/dev/null", O_RDONLY);
      if ( nullfd < 0 ) {
        CLUSTERD_LOG(CLUSTERD_ERROR, "Cannot redirect stdin, could not open /dev/null: %s", strerror(errno));
      } else {
        err = dup2(nullfd, STDIN_FILENO);
        if ( err < 0 ) {
          CLUSTERD_LOG(CLUSTERD_ERROR, "Could not redirect stdin: %s", strerror(errno));
        }

        close(nullfd);
      }

      nullfd = open("/dev/null", O_WRONLY);
      if ( nullfd < 0 ) {
        CLUSTERD_LOG(CLUSTERD_ERROR, "Cannot redirect stdout, could not open /dev/null: %s", strerror(errno));
      } else {
        err = dup2(nullfd, STDOUT_FILENO);
        if ( err < 0 ) {
          CLUSTERD_LOG(CLUSTERD_ERROR, "Could not redirect stdout: %s", strerror(errno));
        }

        err = dup2(nullfd, STDERR_FILENO);
        if ( err < 0 ) {
          CLUSTERD_LOG(CLUSTERD_ERROR, "Could not redirect stderr: %s", strerror(errno));
        }

        close(nullfd);
      }

      return; // Only the grand child survives
    } else exit(EXIT_SUCCESS); // Let the child die
  } else {
    CLUSTERD_LOG(CLUSTERD_INFO, "Going into background");
    exit(EXIT_SUCCESS);
  }
}

int main (int argc, char *const *argv) {
  int c, err, pktlen = 100, thcnt = -1, nftfd, daemonize = 0;
  const char *ns_file = NULL;
  struct mnl_socket *nl;
  unsigned int portid;
  sigset_t mask;

  struct sigaction term_action, hup_action;

  sigfillset(&mask);

  err = sigprocmask(SIG_BLOCK, &mask, NULL);
  if ( err < 0 ) {
    CLUSTERD_LOG(CLUSTERD_CRIT, "Could not block all signals: %s", strerror(errno));
    return 1;
  }

  sigemptyset(&mask);

  sigaddset(&mask, SIGINT);
  sigaddset(&mask, SIGTERM);
  sigaddset(&mask, SIGHUP);

  term_action.sa_handler = term_handler;
  hup_action.sa_handler = hup_handler;
  sigfillset(&term_action.sa_mask);
  sigfillset(&hup_action.sa_mask);
  term_action.sa_flags = 0;
  hup_action.sa_flags = 0;

  sigaction(SIGTERM, &term_action, NULL);
  sigaction(SIGTERM, &term_action, NULL);

  sigaction(SIGHUP, &hup_action, NULL);

  setlocale(LC_ALL, "C");
  srandom(time(NULL));

  err = init_nft_command();
  if ( err < 0 ) {
    CLUSTERD_LOG(CLUSTERD_CRIT, "Could not initialize nft regex");
    return 1;
  }

  while ( (c = getopt(argc, argv, "t:q:n:l:N:s:vhd")) != -1 ) {
    switch ( c ) {
    case 't':
      g_table_name = optarg;
      break;

    case 's':
      ns_file = optarg;
      break;

    case 'l':
      err = sscanf(optarg, "%d", &pktlen);
      if ( err != 1 || pktlen < 0 ) {
	CLUSTERD_LOG(CLUSTERD_ERROR, "Invalid packet queue length: %d", pktlen);
	usage();
	return 1;
      }
      break;

    case 'N':
      err = sscanf(optarg, "%d", &thcnt);
      if ( err != 1 || thcnt <= 0 ) {
	CLUSTERD_LOG(CLUSTERD_ERROR, "Invalid thread pool count: %d", thcnt);
	usage();
	return 1;
      }

      if ( thcnt > 32 ) {
	CLUSTERD_LOG(CLUSTERD_ERROR, "Thread pool size %d may be too large", thcnt);
	usage();
	return 1;
      }
      break;

    case 'q':
      if ( g_queue_num >= 0 ) {
	CLUSTERD_LOG(CLUSTERD_ERROR, "Queue number should only be specified once");
	usage();
	return 1;
      } else {
	err = sscanf(optarg, "%d", &g_queue_num);
	if ( err != 1 || g_queue_num < 0 ) {
	  CLUSTERD_LOG(CLUSTERD_ERROR, "Invalid queue number: %s", optarg);
	  usage();
	  return 1;
	}
      }
      break;

    case 'd':
      daemonize = 1;
      break;

    case 'v':
      CLUSTERD_LOG_LEVEL = CLUSTERD_DEBUG;
      break;

    case 'h':
      usage();
      return 0;

    default:
      CLUSTERD_LOG(CLUSTERD_ERROR, "Invalid option: -%c", (char)optopt);
      usage();
      return 1;
    }
  }

  if ( !ns_file ) {
    CLUSTERD_LOG(CLUSTERD_ERROR, "Namespace file required");
    usage();
    return 1;
  }

  if ( g_queue_num < 0 ) {
    CLUSTERD_LOG(CLUSTERD_ERROR, "No netfilter queue provided");
    usage();
    return 1;
  }

  if ( thcnt < 0 ) {
    // Default to number of cores minus 1, but at least 1
    thcnt = sysconf(_SC_NPROCESSORS_ONLN);
    if ( thcnt < 0 ) {
      CLUSTERD_LOG(CLUSTERD_WARNING, "Could not get number of processors: %s", strerror(errno));
    }

    if ( thcnt < 1 )
      thcnt = 1;
  }

  err = allocate_packet_queue(pktlen);
  if ( err < 0 ) {
    CLUSTERD_LOG(CLUSTERD_ERROR, "Could not allocate packet queue: %s", strerror(errno));
    return 1;
  }

  if ( daemonize )
    setup_daemon();


  g_netlink_socket = nl = start_ns_helper(ns_file, &nftfd);
  if ( !nl ) {
    CLUSTERD_LOG(CLUSTERD_ERROR, "Could not open netfilter socket: %s", strerror(errno));
    return 1;
  }

  err = start_threads(thcnt, nftfd);
  if ( err < 0 ) {
    CLUSTERD_LOG(CLUSTERD_ERROR, "Could not start threads: %s", strerror(errno));
    return 1;
  }

  err = sigprocmask(SIG_UNBLOCK, &mask, NULL);
  if ( err < 0 ) {
    CLUSTERD_LOG(CLUSTERD_CRIT, "Could not set signal mask: %s", strerror(errno));
    return 1;
  }

  portid = mnl_socket_get_portid(nl);

  // Now we loop forever. If we receive SIGHUP, then empty the maps
  for (;;) {
    char buf[0xFFFF + (MNL_SOCKET_BUFFER_SIZE/2)];

  again:
    // Now we should read a packet from the netlink queues
    err = mnl_socket_recvfrom(nl, buf, sizeof(buf));
    if ( err == -1 ) {
      CLUSTERD_LOG(CLUSTERD_INFO, "Got error: %s", strerror(errno));
      if ( errno == EINTR ) {
	// Check SIGHUP or SIGINT/SIGTERM
	if ( g_should_terminate ) goto finish_loop;
	if ( g_sighup ) reset_tables();

	g_should_terminate = g_sighup = 0;

	goto again;
      } else {
	CLUSTERD_LOG(CLUSTERD_CRIT, "Could not receive packet from queue: %s", strerror(errno));
	return 1;
      }
    }

    CLUSTERD_LOG(CLUSTERD_DEBUG, "Got packet of size %d", err);

    err = mnl_cb_run(buf, err, 0, portid, queue_cb, NULL);
    if ( err < 0 ) {
      CLUSTERD_LOG(CLUSTERD_CRIT, "Could not run callbacks: %s", strerror(errno));
      return 1;
    }
  }

 finish_loop:
  mnl_socket_close(nl);
  return 0;
}
