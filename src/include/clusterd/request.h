#ifndef __clusterd_request_H__
#define __clusterd_request_H__

#include <clusterd/common.h>

#define CLUSTERD_MAGIC 0xC0DEBEA7

// Clusterd requests
#define CLUSTERD_OP_MONITOR     0x0100
#define CLUSTERD_OP_MONITOR_ACK 0x0101

// Clusterd attributes
#define CLUSTERD_ATTR_NAMESPACE  0x0001
#define CLUSTERD_ATTR_SERVICE    0x0002
#define CLUSTERD_ATTR_PROCESS    0x0003
#define CLUSTERD_ATTR_MONITOR_V4 0x0004
#define CLUSTERD_ATTR_MONITOR_V6 0x0005
#define CLUSTERD_ATTR_INTERVAL   0x0006
#define CLUSTERD_ATTR_COOKIE     0x0007

#define CLUSTERD_ATTR_OPTIONAL_F 0x8000

#define CLUSTERD_ATTR_OPTIONAL(t) (((t) & CLUSTERD_ATTR_OPTIONAL_F) != 0)

typedef struct {
  uint16_t op;
  uint16_t length;
  uint32_t magic;
} CLUSTERD_PACKED clusterd_request;

typedef struct {
  uint16_t atype;
  uint16_t alen;
} CLUSTERD_PACKED clusterd_attr;

typedef struct {
  uint32_t ip4_addr;
  uint16_t ip4_port;
} CLUSTERD_PACKED clusterd_ip4_attr;

typedef struct {
  unsigned char ip6_addr[16];
  uint16_t ip6_port;
} CLUSTERD_PACKED clusterd_ip6_attr;

#define CLUSTERD_FORMAT_REQUEST(req, opnm, len)  \
  do {                                           \
    (req)->op = htons(opnm);                     \
    (req)->length = htons(len);                  \
    (req)->magic = htonl(CLUSTERD_MAGIC);        \
  } while (0)

#define PAD4(x) ((((x) + 3)/4) * 4)
#define CLUSTERD_REQUEST_IF_VALID(base, req, p, sz)                     \
  ((((uintptr_t)(p)) + (sz)) >= (((uintptr_t) (base)) + ntohl((req)->length)) ? \
   NULL : (p))
#define CLUSTERD_REQUEST_FIRST_ATTR(base, req)   \
  CLUSTERD_REQUEST_IF_VALID(base, req, (clusterd_attr *)(((uintptr_t)(req)) + sizeof(clusterd_request)), sizeof(clusterd_attr))
#define CLUSTERD_REQUEST_NEXT_ATTR(attr, base, req)     \
  CLUSTERD_REQUEST_IF_VALID(base, req, (clusterd_attr *)(((uintptr_t)(attr)) + PAD4(ntohs((attr)->alen))), sizeof(clusterd_attr))

#define FORALL_CLUSTERD_ATTRS(attr, base, req)                          \
  for ( (attr) = CLUSTERD_REQUEST_FIRST_ATTR(base, req);                \
        (attr);                                                         \
        (attr) = CLUSTERD_REQUEST_NEXT_ATTR(attr, base, req) )

#define CLUSTERD_ATTR_DATA(attr, base, req)     \
  CLUSTERD_REQUEST_IF_VALID(base, req, (void *) (((uintptr_t)(attr)) + sizeof(clusterd_attr)), \
                            ntohs((attr)->alen) - sizeof(clusterd_attr))

#define CLUSTERD_INIT_REQ(buf, bufoffs, bufsz, rop)     \
  do {                                                  \
    clusterd_request req;                               \
    req.op = htons((rop));                              \
    req.length = (bufsz);                               \
    req.magic = htonl(CLUSTERD_MAGIC);                  \
                                                        \
    if ( sizeof(req) < bufsz ) goto nospace;            \
    memcpy((buf), &req, sizeof(req));                   \
    bufoffs = sizeof(req);                              \
  } while ( 0 )

#define CLUSTERD_SET_ATTRSZ(buf, bufoffs, attroffs)                     \
  do {                                                                  \
    clusterd_attr old_attr;                                             \
    size_t attrsz = (bufoffs) - (attroffs);                             \
    void *pos = (void *)((uintptr_t)(buf) + (bufoffs));                 \
    memcpy(&old_attr, pos, sizeof(old_attr));                           \
    old_attr.alen = htons(attrsz);                                      \
    memcpy(pos, &old_attr, sizeof(old_attr));                           \
  } while ( 0 )

#define CLUSTERD_WRITE_ATTR(buf, bufoffs, d, sz)                \
  do {                                                          \
    clusterd_request req;                                       \
    void *p = (void *)(((uintptr_t)(buf)) + (bufoffs));         \
    memcpy(&req, (buf), sizeof(req));                           \
    if ( ((bufoffs) + (sz)) > req.length ) goto nospace;        \
    memcpy(p, (d), (sz));                                       \
    (bufoffs) += (sz);                                          \
  } while ( 0 )

#define CLUSTERD_ADD_ATTR(buf, bufoffs, attroffs, op)                   \
  do {                                                                  \
    clusterd_attr attr;                                                 \
    clusterd_request req;                                               \
                                                                        \
    memcpy(&req, buf, sizeof(clusterd_request));                        \
                                                                        \
    if ( (attroffs) == 0 ) {                                            \
      (attroffs) = sizeof(clusterd_request);                            \
    } else {                                                            \
      CLUSTERD_SET_ATTRSZ(buf, bufoffs, attroffs);                      \
                                                                        \
      (attroffs) = PAD4(bufoffs);                                       \
    }                                                                   \
                                                                        \
    if ( ((attroffs) + sizeof(attr)) > req.length)                      \
      goto nospace;                                                     \
                                                                        \
    attr.atype = htons(op);                                             \
    attr.alen = 0;                                                      \
    memcpy((void *)(((uintptr_t) (buf)) + (attroffs)),                  \
           &attr, sizeof(attr));                                        \
    (bufoffs) = (attroffs) + sizeof(attr);                              \
    } while ( 0 )

#define CLUSTERD_FINISH_REQUEST(buf, bufoffs, attroffs) \
  do {                                                  \
    clusterd_request req;                               \
                                                        \
    if ( (attroffs) != 0 ) {                            \
      CLUSTERD_SET_ATTRSZ(buf, bufoffs, attroffs);      \
    }                                                   \
                                                        \
    memcpy(&req, (buf), sizeof(req));                   \
    (bufoffs) = PAD4(bufoffs);                          \
    if ( (bufoffs) > req.length )                       \
      goto nospace;                                     \
                                                        \
    req.length = htons((bufoffs));                      \
    memcpy((buf), &req, sizeof(req));                   \
  } while ( 0 )

#endif
