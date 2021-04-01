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
 *
 */

#define CLUSTERD_COMPONENT "controller"
#include "clusterd/log.h"
#include "clusterd/controller.h"
#include "clusterd/common.h"

#include "sha256.h"

#include <stdlib.h>
#include <string.h>

#include <getopt.h>
#include <assert.h>
#include <uv.h>
#include <lua.h>
#include <lualib.h>
#include <lauxlib.h>
#include <locale.h>
#include <raft.h>
#include <raft/uv.h>

#define CLUSTERD_DEFAULT_CONTROLLER_PORT 38019

extern struct raft *lua_tx_raft;

int CLUSTERD_LOG_LEVEL = CLUSTERD_INFO;

typedef enum
  {
   CLIENT_GET_COMMAND,
   CLIENT_ACCEPT_SCRIPT,
   CLIENT_ACCEPT_PARAMETERS,
   CLIENT_ENQUEUED,
   CLIENT_BUSY,
   CLIENT_WAITING_FOR_QUORUM,
   CLIENT_RESPONDING,
   CLIENT_DONE
  } client_state;

struct controller_client;

typedef struct {
  uv_tcp_t tcp;
  struct raft *raft;

  unsigned int log_entries_pending : 1;

  struct controller_client *processing_head, *processing_tail;
} controller_service;

typedef struct controller_client {
  uv_tcp_t tcp;

  char  command_buf[32 * 1024];
  off_t command_offs;

  char response_buf[16 * 1024];

  char script_hash[SHA256_STR_LENGTH + 1];

  unsigned int writing : 1;
  client_state state;

  // This flag is set if the client wants to run a ctlcall that needs
  // write access. This flag is only set on nodes who are the leader
  // (or think they are). Otherwise, it fails.
  unsigned int needs_tx_write : 1;

  lua_State *thread;

  uv_write_t response;

  struct controller_client *prev, *next;
} controller_client;

struct lua_library {
  const char *name;
  lua_CFunction func;
} baselibs[] =
  {
   { "_G", luaopen_base },
   {LUA_LOADLIBNAME, luaopen_package},
   {LUA_COLIBNAME, luaopen_coroutine},
   {LUA_TABLIBNAME, luaopen_table},
   {LUA_STRLIBNAME, luaopen_string},
   {LUA_MATHLIBNAME, luaopen_math},
   {LUA_DBLIBNAME, luaopen_debug},
   {"clusterd", luaopen_clusterd},
   { NULL, NULL }
  };

controller_service *g_service = NULL;
char *g_datadir = NULL;
lua_State *g_lua = NULL;
sqlite3 *g_database = NULL;

static char g_dbfile[PATH_MAX];

static const char final_comment[] = "-- CONTROL END\n";
extern const char rc_json_lua[];

static void client_accept_command(controller_client *client);
static void client_abort(controller_client *client);
static void client_respond(controller_client *client, const char *fmt, ...);
static void client_enqueue(controller_client *client);
static void client_wait_for_write(controller_client *client);
static void client_leader_redirect(controller_client *client, int provisional_ok);

static void service_dequeue_and_process();
static void lua_full_gc();

int clusterd_tx_commit();
void clusterd_tx_free();
int clusterd_apply_tx(char *sql, size_t sqlsz, char **errmsg);
int clusterd_commit(char **errmsg);
int clusterd_begin(char **errmsg, int writable);
int clusterd_rollback();

static void trace_raft(struct raft_tracer *a, const char *file, int line, const char *msg) {
  CLUSTERD_LOG(CLUSTERD_DEBUG, "Raft: %s:%d: %s", file, line, msg);
}

static struct raft_tracer debug_tracer = { NULL, trace_raft };

static int is_valid_sha256(const char *s) {
  int i;

  if ( strlen(s) != SHA256_STR_LENGTH )
    return 0;

  for ( i = 0; i < SHA256_STR_LENGTH; ++i ) {
    if ( (s[i] >= '0' && s[i] <= '9') ||
         (s[i] >= 'a' && s[i] <= 'f') )
      continue;

    return 0;
  }

  return 1;
}

static void usage() {
  fprintf(stderr, "clusterd-controller -- clusterd controller daemon\n");
  fprintf(stderr, "Usage: clusterd-controller -vhB [-j address...] -S ADDR\n");
  fprintf(stderr, "         nodeid/bindaddress /path/to/state/dir\n\n");
  fprintf(stderr, "   -j ADDR          When bootstrapping, specify this option\n");
  fprintf(stderr, "                    for each peer in the cluster\n");
  fprintf(stderr, "   -S SERVICEADDR   Address to serve the local service on\n");
  fprintf(stderr, "   -B               Force a bootstrap on this node, even if state dir exists\n");
  fprintf(stderr, "   -h               Show this help menu\n");
  fprintf(stderr, "   -v               Show verbose debugging output\n\n");
  fprintf(stderr, "Please report bugs to support@f-omega.com\n");
}

static int split_nodeid_addr(const char *addrstr, raft_id *nodeid, char **addrstart) {
  // The address string should be of the form Node ID/IP address:port
  char *endptr;
  int err;

  errno = 0;
  *nodeid = strtoll(addrstr, &endptr, 10);
  if ( errno != 0)
    return -1;

  if ( *endptr != '/' )
    return -1;

  // Now parse the address.
  endptr ++;

  *addrstart = endptr;

  return 0;
}

static int add_peer(char *addrstr, struct raft_configuration *conf) {
  int err;
  raft_id id;
  char *addrstart;

  err = split_nodeid_addr(addrstr, &id, &addrstart);
  if ( err < 0 )
    return err;

  err = raft_configuration_add(conf, id, addrstart, RAFT_VOTER);
  if ( err != 0 ) {
    CLUSTERD_LOG(CLUSTERD_CRIT, "Could not add peer %s: %s", addrstart, raft_strerror(err));
    return -1;
  }

  return 0;
}

static int clusterd_apply(struct raft_fsm *fsm,
                          const struct raft_buffer *buf,
                          void **result) {
  char *nl, *end, *errmsg;
  int version, err;

  CLUSTERD_LOG(CLUSTERD_DEBUG, "[RAFT] Applying %.*s", (int)buf->len, (char *)buf->base);

  nl = memchr(buf->base, '\n', buf->len);
  // Find the version string. It should be --%u
  if ( !nl ||
       buf->len <= 2 ||
       memcmp(buf->base, "--", 2) != 0 ) {
    CLUSTERD_LOG(CLUSTERD_CRIT, "Transaction malformed");
    return RAFT_MALFORMED;
  }

  errno = 0;
  version = strtol(buf->base + 2, &end, 10);
  if ( errno != 0 ) {
    CLUSTERD_LOG(CLUSTERD_ERROR, "Could not parse Raft log entry version: %s", strerror(errno));
    return RAFT_MALFORMED;
  }

  if ( version != 1 ) {
    CLUSTERD_LOG(CLUSTERD_ERROR, "Invalid version: %d", version);
    return RAFT_MALFORMED;
  }

  err = clusterd_apply_tx(buf->base, buf->len, &errmsg);
  if ( err < 0 ) {
    CLUSTERD_LOG(CLUSTERD_ERROR, "Could not apply transaction: %s", errmsg);
    return RAFT_MALFORMED;
  }

  CLUSTERD_LOG(CLUSTERD_DEBUG, "Applied successfully");

  return 0;
}

static void free_buffers(struct raft_buffer *bufs[], unsigned int nbufs) {
  unsigned int i;

  for ( i = 0; i < nbufs; ++i ) {
    struct raft_buffer *buf = *bufs + i;
    free(buf->base);
  }

  free(*bufs);
}

static struct raft_buffer *alloc_buf(ssize_t nextbufsz, struct raft_buffer **bufs,
                                     unsigned int *capacity, unsigned int *nbufs) {
  struct raft_buffer *buf = NULL;

  CLUSTERD_LOG(CLUSTERD_DEBUG, "Capacity %u, size %u", *capacity, *nbufs);

  if ( *capacity <= *nbufs ) {
    struct raft_buffer *next_bufs;
    unsigned int next_capacity = *capacity * 2;

    if ( next_capacity == 0 )
      next_capacity = 4;

    CLUSTERD_LOG(CLUSTERD_DEBUG, "Next capacity %u", next_capacity);
    CLUSTERD_LOG(CLUSTERD_DEBUG, "bufs=%p", bufs);
    CLUSTERD_LOG(CLUSTERD_DEBUG, "*bufs=%p", *bufs);

    if ( *bufs )
      next_bufs = raft_realloc(*bufs, sizeof(struct raft_buffer) * next_capacity);
    else
      next_bufs = raft_malloc(sizeof(struct raft_buffer) * next_capacity);

    if ( !next_bufs ) return NULL;

    CLUSTERD_LOG(CLUSTERD_DEBUG, "Next bufs %p", next_bufs);

    *capacity = next_capacity;
    *bufs = next_bufs;
  }

  // Now we certainly have enough space
  buf = *bufs + *nbufs;

  CLUSTERD_LOG(CLUSTERD_DEBUG, "Next buf %p", buf);

  buf->len = nextbufsz;
  buf->base = raft_malloc(nextbufsz);
  if ( !buf->base )
    return NULL;

  // Add the buffer to the end of the list
  *nbufs ++;
  CLUSTERD_LOG(CLUSTERD_DEBUG, "Got %u bufs", *nbufs);
  return buf;
}

static int clusterd_snapshot(struct raft_fsm *fsm, struct raft_buffer *bufs[], unsigned *n_bufs) {
  int err, ret = 0, dbfd;
  unsigned int buf_capacity = 0;
  size_t total_sz = 0;

  *bufs = NULL;
  *n_bufs = 0;

  CLUSTERD_LOG(CLUSTERD_DEBUG, "Debug. Snapshotting");

  err = clusterd_snapshot_database(g_dbfile);
  if ( err < 0 ) {
    CLUSTERD_LOG(CLUSTERD_CRIT, "Could not snapshot database: %s", strerror(errno));
    return RAFT_CORRUPT;
  }

  CLUSTERD_LOG(CLUSTERD_DEBUG, "Snapshotted database");

  dbfd = open(g_dbfile, O_RDONLY);
  if ( dbfd < 0 ) {
    CLUSTERD_LOG(CLUSTERD_CRIT, "Could not read database file: %s", strerror(errno));
    return RAFT_CORRUPT;
  }

  *bufs = NULL;

  for (;;) {
    ssize_t bytes;
    char buf[1024 * 16];

    bytes = read(dbfd, buf, sizeof(buf));
    if ( bytes == 0 ) {
      // End of file
      break;
    } else if ( bytes < 0 ) {
      CLUSTERD_LOG(CLUSTERD_CRIT, "Could not read database file: %s", strerror(errno));
      ret = RAFT_CORRUPT;

      goto error;
    } else {
      struct raft_buffer *next_buf;

      CLUSTERD_LOG(CLUSTERD_DEBUG, "Read %zd bytes", bytes);

      next_buf = alloc_buf(bytes, bufs, &buf_capacity, n_bufs);
      if ( !next_buf ) {
        CLUSTERD_LOG(CLUSTERD_CRIT, "Could not allocate buffer space. Out of memory");
        ret = RAFT_NOMEM;
        goto error;
      }

      total_sz += bytes;
      memcpy(next_buf->base, buf, bytes);
    }
  }

  close(dbfd);

  CLUSTERD_LOG(CLUSTERD_DEBUG, "Snapshotted %zd bytes", total_sz);

  return ret;

 error:
  free_buffers(bufs, *n_bufs);
  *n_bufs = 0;
  return ret;
}

static int clusterd_restore(struct raft_fsm *fsm, struct raft_buffer *buf) {
  int err;

  CLUSTERD_LOG(CLUSTERD_DEBUG, "Restoring snapshot of size %zd", buf->len);

  err = clusterd_restore_snapshot(g_dbfile, buf->base, buf->len);
  if ( err < 0 ) {
    CLUSTERD_LOG(CLUSTERD_CRIT, "Could not restore snapshot: %s", strerror(errno));
    return RAFT_CORRUPT;
  }

  return 0;
}

static void client_alloc_buffer(uv_handle_t *handle, size_t suggested, uv_buf_t *buf) {
  controller_client *client = CLUSTERD_STRUCT_FROM_FIELD(controller_client, tcp, handle);
  size_t sz = suggested;

  buf->base = NULL;
  buf->len = 0;

  if ( client->command_offs >= sizeof(client->command_buf) )
    return;

  if ( sz > (sizeof(client->command_buf) - client->command_offs) )
    sz = sizeof(client->command_buf) - client->command_offs;

  buf->base = client->command_buf + client->command_offs;
  buf->len = sz;
}

static void free_client(uv_handle_t *tcp) {
  controller_client *client = CLUSTERD_STRUCT_FROM_FIELD(controller_client, tcp, tcp);

  // Remove the client from the lua client table
  lua_getglobal(g_lua, "clients");
  lua_pushnil(g_lua);
  lua_seti(g_lua, -2, (lua_Integer)client);

  free(client);

  lua_full_gc();
}

static void client_close(controller_client *client) {
  CLUSTERD_LOG(CLUSTERD_DEBUG, "Closing client %p", (void *)client);
  if ( client->writing ) {
    CLUSTERD_LOG(CLUSTERD_DEBUG, "Deferring close on %p due to write", (void *)client);
    // Reading is already stopped
    client->state = CLIENT_DONE;
  } else if ( client->state == CLIENT_ENQUEUED ) {
    // Client is enqueued for processing, but hasn't been processed
    // yet (if it did, it'd be CLIENT_BUSY or CLIENT_RESPONDING).

    if ( client == g_service->processing_head ) {
      g_service->processing_head = g_service->processing_tail = NULL;
      client->next = client->prev = NULL;
      goto do_close;
    } else {
      // Client is enqueued to write but hasn't done so yet, remove from queue and continue
      if ( client == g_service->processing_tail )
        g_service->processing_tail = client->prev;

      if ( client->prev )
        client->prev->next = client->next;

      if ( client->next )
        client->next->prev = client->prev;

      goto do_close;
    }
  } else if ( client->state == CLIENT_WAITING_FOR_QUORUM ) {
    // An abort while waiting for quorum. The request was submitted,
    // so we need this client around to wait.
    CLUSTERD_LOG(CLUSTERD_DEBUG, "Deferring close on %p due to quorum", (void *)client);
    client->state = CLIENT_DONE;
  }

do_close:
    uv_close((uv_handle_t *)&client->tcp, free_client);
}

static void do_nothing_close(uv_handle_t *hdl) {
}

static void lua_full_gc() {
  int err;

  err = lua_gc(g_lua, LUA_GCCOLLECT, 0);
  if ( err != LUA_OK ) {
    CLUSTERD_LOG(CLUSTERD_CRIT, "Lua garbage collection failure");
  }
}

static void client_resume(controller_client *client) {
  int err;

  client->state = CLIENT_BUSY;

  err = lua_resume(client->thread, g_lua, 0);
  if ( err == LUA_OK ) {
    int complete = 0;

    client->state = CLIENT_GET_COMMAND;

    // If the client was writing, then submit the raft transaction
    if ( client->needs_tx_write ) {
      if ( client != g_service->processing_head ) {
        CLUSTERD_LOG(CLUSTERD_CRIT, "This client is not at the write head but it was run anyway");
        abort();
      }

      client->needs_tx_write = 0;
      err = clusterd_tx_commit();
      if ( err < 0 ) {
        switch ( errno ) {
        case EAGAIN:
          client->state = CLIENT_WAITING_FOR_QUORUM;
          err = clusterd_rollback();
          if ( err < 0 ) {
            CLUSTERD_LOG(CLUSTERD_CRIT, "Could not rollback transaction while waiting for quorum");
            abort();
          }
          break;

        case EREMOTE:
          // Not leader
          client_leader_redirect(client, 0);
          break;

        default:
          // Report error
          clusterd_tx_free();
          client_respond(client, "!ERROR\nCould not commit transaction\n--DONE\n");
          break;
        }
      } else {
        char *msg;

        // If the raft transaction applied immediately, commit
        err = clusterd_commit(&msg);
        if ( err < 0 ) {
          client_respond(client, "!ERROR\n%s\n--DONE\n", msg);
          return;
        }
      }
    } else {
      err = clusterd_rollback();
      if ( err < 0 ) {
        CLUSTERD_LOG(CLUSTERD_CRIT, "Could not rollback read-only transaction");
        abort();
      }
    }

    if ( client->state == CLIENT_GET_COMMAND ) {
      client_respond(client, "--DONE\n");
      service_dequeue_and_process();
    }
  } else if ( err == LUA_YIELD ) {
    const char *message;

    message = lua_tostring(client->thread, -1);

    if ( message ) {
      lua_pop(client->thread, 1);

      client->state = CLIENT_RESPONDING;
      client_respond(client, "%s\n", message);
    } else {
      CLUSTERD_LOG(CLUSTERD_CRIT, "Unknown value yielded from Lua");
      client->state = CLIENT_GET_COMMAND;
      client->needs_tx_write = 0;
      clusterd_tx_free();
      client_respond(client, "!ERROR\nUnknown lua value\n--DONE\n");
      service_dequeue_and_process();
    }
  } else {
    lua_Debug tb;
    char lua_error[16 * 1024];

    client->state = CLIENT_GET_COMMAND;
    client->needs_tx_write = 0;
    clusterd_tx_free();
    if ( lua_isstring(client->thread, -1) ) {
      CLUSTERD_LOG(CLUSTERD_ERROR, "Lua error: %s", lua_tostring(client->thread, -1));
      snprintf(lua_error, sizeof(lua_error), "%s", lua_tostring(client->thread, -1));
    } else if ( lua_getstack(client->thread, 0, &tb) == 0 ) {
      CLUSTERD_LOG(CLUSTERD_CRIT, "Lua reported error, but there is no error");
      snprintf(lua_error, sizeof(lua_error), "unknown error");
    } else {
      int lerr;

      lerr = lua_getinfo(client->thread, "nSl", &tb);
      if ( lerr == LUA_OK ) {
        CLUSTERD_LOG(CLUSTERD_ERROR, "Lua error: %d: %s (%s): %s:%d: %s",
                     err, tb.name, tb.namewhat, tb.short_src, tb.currentline, tb.what);
        snprintf(lua_error, sizeof(lua_error),
                 "At %s (%s): %s:%d:\n%s",
                 tb.name, tb.namewhat, tb.short_src, tb.currentline, tb.what);
      } else {
        CLUSTERD_LOG(CLUSTERD_ERROR, "Lua error: could not get debug structure");
        snprintf(lua_error, sizeof(lua_error), "unknown error");
      }
    }

    lua_error[sizeof(lua_error) - 1] = '\0';

    client_respond(client, "!ERROR\n%s\n--DONE\n", lua_error);
    err = clusterd_rollback();
    if ( err < 0 ) {
      CLUSTERD_LOG(CLUSTERD_CRIT, "Could not rollback transaction that had error");
      abort();
    }
    service_dequeue_and_process();
  }
}

static void service_process_next() {
  controller_client *client;
  int err;
  char *errmsg;

  CLUSTERD_LOG(CLUSTERD_DEBUG, "Processing next work item");

  while ( g_service->processing_head ) {
    client = g_service->processing_head;

    err = clusterd_begin(&errmsg, client->needs_tx_write);
    if ( err < 0 ) {
      client->state = CLIENT_GET_COMMAND;
      client->needs_tx_write = 0;
      client_respond(client, "!ERROR\ndb error: %s\n--DONE\n", errmsg);

      g_service->processing_head = g_service->processing_head->next;
      continue;
    } else {
      // After this, the client will be resumed and the thread started
      client->state = CLIENT_RESPONDING;
      client_respond(client, "+processing\n");
      return;
    }
  }

  g_service->processing_tail = NULL;
}

static void service_dequeue_and_process() {
  if ( g_service->processing_head ) {
    g_service->processing_head = g_service->processing_head->next;
    if ( !g_service->processing_head )
      g_service->processing_tail = NULL;
  }

  service_process_next();
}

void clusterd_next_write(int status, void *result) {
  controller_client *client;

  if ( !g_service->processing_head ) {
    CLUSTERD_LOG(CLUSTERD_CRIT, "No client was submitting a raft apply, but we're here");
    abort();
  }

  client = g_service->processing_head;

  if ( client->state == CLIENT_DONE ) {
    // Client exited while we were submitting this request. Well, it's either committed now or it's not.
    if ( status == 0 )
      CLUSTERD_LOG(CLUSTERD_INFO, "Client %p disconnected while waiting for quorum. The request was committed",
                   (void *)client);
    else
      CLUSTERD_LOG(CLUSTERD_CRIT, "Client %p disconnected while waiting for quorum. Request was not applied: %s",
                   (void *)client, raft_errmsg(g_service->raft));
  } else {
    client->state = CLIENT_GET_COMMAND;
    client->needs_tx_write = 0;

    if ( status == 0 ) {
      // Resume the client normally
      client_respond(client, "--DONE\n");
    } else if ( status == RAFT_LEADERSHIPLOST ) {
      // Special leadership-lost error
      client_respond(client, "!leadership-lost\n");
    } else {
      client_respond(client, "!ERROR\n%s\n--DONE\n", raft_errmsg(g_service->raft));
    }

    service_dequeue_and_process();
  }
}

static void client_enqueue(controller_client *client) {
  lua_setglobal(client->thread, "params");

  client->state = CLIENT_ENQUEUED;

  // Add the client to the processing_head
  if ( g_service->processing_head ) {
    g_service->processing_tail->next = client;
    client->prev = g_service->processing_tail;

    g_service->processing_tail = client;
    client->next = NULL;
  } else {
    g_service->processing_head = g_service->processing_tail = client;
    client->next = client->prev = NULL;

    service_process_next();
  }
}

static void client_leader_redirect(controller_client *client, int provisional_ok) {
  // If we're here, we are not the leader. There either can be a
  // leader, or we could be in the middle of an election, or the
  // leader could be unavailable.
  raft_id leader_id;
  const char *address;

  raft_leader(g_service->raft, &leader_id, &address);

  if ( address ) {
    struct sockaddr_storage ss;
    int err;

    err = clusterd_addr_parse((char *)address, &ss, 1);
    if ( err < 0 ) {
      client_respond(client, "!unavailable\n");
    } else {
      char adjaddr[CLUSTERD_ADDRSTRLEN];

      switch ( ss.ss_family ) {
      case AF_INET:
        ((struct sockaddr_in *)&ss)->sin_port =
          htons(ntohs(((struct sockaddr_in *)&ss)->sin_port) + 1);
        break;

      case AF_INET6:
        ((struct sockaddr_in6 *)&ss)->sin6_port =
          htons(ntohs(((struct sockaddr_in6 *)&ss)->sin6_port) + 1);
        break;

      default:
        ss.ss_family = AF_UNSPEC;
        client_respond(client, "!unavailable\n");
        break;
      }

      if ( ss.ss_family != AF_UNSPEC ) {
        clusterd_addr_render(adjaddr, (const struct sockaddr *)&ss, 1);
        client_respond(client, "!leader %s %u\n", adjaddr, leader_id);
      }
    }
  } else {
    CLUSTERD_LOG(CLUSTERD_DEBUG, "Got state: %d", raft_state(g_service->raft));
    client_respond(client, "!unavailable\n");
  }
}

static int client_read_command(controller_client *client) {
  char *eol, *bol;
  int err;

  eol = memchr(client->command_buf, '\n', client->command_offs);
  if ( !eol ) {
    errno = EWOULDBLOCK;
    return -1;
  }

  CLUSTERD_LOG(CLUSTERD_DEBUG, "Got command %.*s", (int)(eol - client->command_buf), client->command_buf);

  switch ( client->state ) {
  case CLIENT_GET_COMMAND:
    // The command ought to be the sha256sum of a pre-loaded script in
    // our hash table.
    *eol = '\0';
    if ( strlen(client->command_buf) > 0 &&
         is_valid_sha256(client->command_buf) == 0 ) {
      if ( client->command_buf[0] == '+' ) {
        // Handle special command (like +consistent or +write)
        if ( strcmp(client->command_buf, "+consistent") == 0 ) {
          // Check if we're the leader. If we're not, then return a +leader redirect
          if ( raft_state(g_service->raft) != RAFT_LEADER ) {
            client_leader_redirect(client, 0);
          } else {
            client_respond(client, "+ok\n");
          }
        } else if ( strcmp(client->command_buf, "+write") == 0 ) {
          // Check if we're the leader. If we're not, then return a
          // +leader redirect. Otherwise, place us into the write
          // queue, and mark the client as wanting a write.
          if ( raft_state(g_service->raft) != RAFT_LEADER ) {
            client_leader_redirect(client, 1);
          } else {
            client->needs_tx_write = 1;
            client_respond(client, "+ok\n");
          }
        } else
          client_respond(client, "!invalid-pragma\n");
      } else {
        client_respond(client, "!invalid-command\n");
      }
    } else if ( strlen(client->command_buf) == 0 ) {
      memset(client->script_hash, 0, sizeof(client->script_hash));
      client_respond(client, "+need-script\n");
      client->state = CLIENT_ACCEPT_SCRIPT;
    } else {
      memcpy(client->script_hash, client->command_buf, SHA256_STR_LENGTH);
      client->script_hash[SHA256_STR_LENGTH] = '\0';

      lua_getglobal(client->thread, "commands");
      lua_getfield(client->thread, -1, client->command_buf);
      if ( lua_isnil(client->thread, -1) ) {
        // Does not exist
        client_respond(client, "+need-script\n");
        client->state = CLIENT_ACCEPT_SCRIPT;
        lua_pop(client->thread, 2);
      } else {
        // Run the command
        CLUSTERD_LOG(CLUSTERD_DEBUG, "Would have to run command");

        lua_rotate(client->thread, -2, 1);
        lua_pop(client->thread, 1); // Get rid of commands, but not the function

        lua_newtable(client->thread);
        client->state = CLIENT_ACCEPT_PARAMETERS;
        client_respond(client, "+params\n");

        // stack is now function, table
      }
    }
    break;

  case CLIENT_ACCEPT_SCRIPT:
    // No script was found, the command ought to be lines to write to
    // the script file. Do nothing, unless the newest line is the special line -- CONTROL END
    if ( client->command_offs >= strlen(final_comment) ) {
      if ( memcmp(client->command_buf + client->command_offs - strlen(final_comment), final_comment,
                  strlen(final_comment)) == 0 ) {
        unsigned char hash_out[SHA256_OCTET_LENGTH];
        char hash_str[SHA256_STR_LENGTH];
        int needs_hash = strlen(client->script_hash) > 0;

        // Check the script hash matches
        if ( needs_hash ) {
          calc_sha_256(hash_out, client->command_buf, client->command_offs - strlen(final_comment));
          sha_256_digest(hash_str, hash_out);
        }

        if ( needs_hash && memcmp(hash_str, client->script_hash, SHA256_STR_LENGTH) != 0 ) {
          client->state = CLIENT_GET_COMMAND;
          client->needs_tx_write = 0;
          client_respond(client, "!hash-mismatch\n");
        } else {
          client->command_buf[client->command_offs - 1] = '\0';
          // Script complete
          CLUSTERD_LOG(CLUSTERD_DEBUG, "Loading script: %s", client->command_buf);

          err = luaL_loadstring(client->thread, client->command_buf);
          switch (err) {
          case LUA_OK:
            CLUSTERD_LOG(CLUSTERD_DEBUG, "Loaded script");
            break;

          case LUA_ERRSYNTAX:
            client->state = CLIENT_GET_COMMAND;
            client->needs_tx_write = 0;
            client_respond(client, "!bad-syntax\n");
            return 0;

          case LUA_ERRMEM:
            client->state = CLIENT_GET_COMMAND;
            client->needs_tx_write = 0;
            client_respond(client, "!out-of-memory\n");
            return 0;

          default:
          case LUA_ERRGCMM:
            client->state = CLIENT_GET_COMMAND;
            client->needs_tx_write = 0;
            client_respond(client, "!unknown\n");
            return 0;
          };

          lua_pushvalue(client->thread, -1); // Copy the function

          // Store the function as the script and execute
          if ( needs_hash ) {
            lua_getglobal(client->thread, "commands");
            lua_rotate(client->thread, -2, 1); // Go from [script, commands] to [commands, script]
            lua_setfield(client->thread, -2, client->script_hash); // This gets rid of the duplicate of the function
            lua_pop(client->thread, 1); // Get rid of commands
          }

          lua_newtable(client->thread);

          // Stack is now function, table

          client->state = CLIENT_ACCEPT_PARAMETERS;
          client_respond(client, "+params\n");
          client->command_offs = 0;
          return 0;
        }
      } else {
        errno = EWOULDBLOCK;
        return -1;
      }
    }
    break;

  case CLIENT_ACCEPT_PARAMETERS:
    // Each line is a name=value pair, until an empty line
    if ( eol == client->command_buf ) {
      CLUSTERD_LOG(CLUSTERD_DEBUG, "Enqueueing command");
      client->state = CLIENT_BUSY;
      client_enqueue(client);
      client->command_offs = 0;
      return 0;
    } else {
      char *eq;

      *eol = '\0';

      eq = strchr(client->command_buf, '=');
      if ( !eq ) {
        client->state = CLIENT_GET_COMMAND;
        client->needs_tx_write = 0;
        client_respond(client, "!bad-param\n");
      } else {
        *eq = '\0';

        lua_pushstring(client->thread, client->command_buf);
        lua_pushvalue(client->thread, -1);
        lua_gettable(client->thread, -3);

        // Top of stack is now old value of param
        if ( lua_isnil(client->thread, -1) ) {
          lua_pop(client->thread, 1);

          lua_pushstring(client->thread, eq + 1);
          lua_settable(client->thread, -3);
        } else if ( lua_istable(client->thread, -1) ) {
          int next_index;

          // Stack is param table, key, table

          // An array
          lua_len(client->thread, -1);
          next_index = lua_tonumber(client->thread, -1);
          lua_pop(client->thread, 1); // Pop the length

          lua_rotate(client->thread, -2, 1);

          // Stack is now param table, table, key
          lua_pop(client->thread, 1);

          // Stack is now param table, table
          lua_pushstring(client->thread, eq + 1);
          lua_seti(client->thread, -2, next_index);

          lua_pop(client->thread, 1);

          // Stack is now param table
        } else {
          // Stack is now param table, key, old value
          lua_newtable(client->thread);

          // Stack is now param table, key, old value, new array
          lua_rotate(client->thread, -2, 1);

          // Stack is now param table, key, new array, old value
          lua_seti(client->thread, -2, 1);

          // Stack is now param table, key, new array
          lua_pushstring(client->thread, eq + 1);

          // Stack is now param table, key, new array, new value
          lua_seti(client->thread, -2, 2);

          // Stack is now param table, key, new array
          lua_settable(client->thread, -3);

          // Stack is now param table
        }

      }
    }
    break;

  case CLIENT_BUSY:
  case CLIENT_WAITING_FOR_QUORUM:
  case CLIENT_ENQUEUED:
    // Don't allow interrupts
    break;

  default:
    CLUSTERD_LOG(CLUSTERD_CRIT, "Invalid client state");
    client->command_offs = 0;
    client_abort(client);

    errno = EINVAL;
    return -1;
  }

  CLUSTERD_LOG(CLUSTERD_DEBUG, "Got command offs: %zu %p %p", client->command_offs, eol, client->command_buf);
  client->command_offs -= eol - client->command_buf + 1;
  CLUSTERD_LOG(CLUSTERD_DEBUG, "Got command offs: %zu", client->command_offs);
  memcpy(client->command_buf, eol + 1, client->command_offs);

  return 0;
}

static void client_process_commands(controller_client *client) {
  int err;

  while ( !client->writing ) {
    err = client_read_command(client);
    if ( err < 0 ) {
      if ( errno == EWOULDBLOCK ) {
        break;
      } else {
        client_abort(client);
        break;
      }
    } else {
      // Process command
    }
  }
}

static void client_read_complete(uv_stream_t *handle, ssize_t nread, const uv_buf_t *buf) {
  controller_client *client = CLUSTERD_STRUCT_FROM_FIELD(controller_client, tcp, handle);

  if ( nread < 0 ) {
    if ( nread == UV_ENOBUFS ) {
      // no more space
      // client_overflow(); // TODO
      client_close(client);
    } else {
      if ( nread != UV_EOF )
        CLUSTERD_LOG(CLUSTERD_WARNING, "Could not handle read from client: %s", uv_strerror(nread));
      client_close(client);
    }
  } else if ( nread == 0 ) {
    client_close(client);
  } else if ( client->state == CLIENT_ENQUEUED ) {
    client_abort(client);
  } else {
    CLUSTERD_LOG(CLUSTERD_DEBUG, "Read %zd bytes from client", nread);
    client->command_offs += nread;

    client_process_commands(client);
  }
}

static void client_accept_command(controller_client *client) {
  int err;

  if ( client->state != CLIENT_BUSY &&
       client->state != CLIENT_WAITING_FOR_QUORUM &&
       client->state != CLIENT_RESPONDING &&
       client->state != CLIENT_DONE ) {
    // We allow reads in CLIENT_WAIT_FOR_WRITE so we can detect disconnects
    err = uv_read_start((uv_stream_t *)&client->tcp, client_alloc_buffer, client_read_complete);
    if ( err < 0 ) {
      if ( err != UV_EOF )
        CLUSTERD_LOG(CLUSTERD_CRIT, "Could not read from client: %s", uv_strerror(err));
      client_close(client);
    }
  }

  return;
}

static void client_abort_complete(uv_write_t *w, int status) {
  if ( status < 0 ) {
    CLUSTERD_LOG(CLUSTERD_CRIT, "Could not complete abort: %s", uv_strerror(status));
  }

  client_close(CLUSTERD_STRUCT_FROM_FIELD(controller_client, response, w));
}

static void client_abort(controller_client *client) {
  uv_buf_t buf;
  int err;

  buf.len = snprintf(client->response_buf, sizeof(client->response_buf), "!abort\n");
  buf.base = client->response_buf;

  if ( buf.len >= sizeof(client->response_buf) ) {
    CLUSTERD_LOG(CLUSTERD_CRIT, "Could not send abort");
    client_close(client);
  } else {
    uv_read_stop((uv_stream_t *) &client->tcp);

    err = uv_write(&client->response, (uv_stream_t *)&client->tcp, &buf, 1, client_abort_complete);
    if ( err < 0 ) {
      CLUSTERD_LOG(CLUSTERD_CRIT, "Could not send abort: %s", uv_strerror(err));
      client_close(client);
    }
  }
}

static void client_response_complete(uv_write_t *write, int status) {
  controller_client *client = CLUSTERD_STRUCT_FROM_FIELD(controller_client, response, write);

  client->writing = 0;
  CLUSTERD_LOG(CLUSTERD_DEBUG, "Response complete");
  if ( status < 0 ) {
    CLUSTERD_LOG(CLUSTERD_CRIT, "Could not complete response: %s", uv_strerror(status));

    if ( status == UV_EPIPE ||
         status == UV_ECONNRESET ||
         client->state == CLIENT_DONE )
      client_close(client);
    else
      client_abort(client);
  } else {
    if ( client->state == CLIENT_RESPONDING ) {
      client->state = CLIENT_BUSY;
      client_resume(client);
    } else if ( client->state == CLIENT_DONE ) {
      client_close(client);
    } else if ( client->state != CLIENT_BUSY &&
                client->state != CLIENT_WAITING_FOR_QUORUM ) {
      client_accept_command(client);
    }
  }
}

static void client_respond(controller_client *client, const char *fmt, ...) {
  va_list args;
  int err;
  uv_buf_t buf;

  va_start(args, fmt);

  buf.len = vsnprintf(client->response_buf, sizeof(client->response_buf), fmt, args);
  if ( buf.len >= sizeof(client->response_buf) ) {
    CLUSTERD_LOG(CLUSTERD_CRIT, "Could not send response");

    client_abort(client);
    return;
  }
  buf.base = client->response_buf;

  CLUSTERD_LOG(CLUSTERD_DEBUG, "Start write");
  client->writing = 1;
  err = uv_write(&client->response, (uv_stream_t *)&client->tcp, &buf, 1, client_response_complete);
  if ( err < 0 ) {
    CLUSTERD_LOG(CLUSTERD_CRIT, "Could not send response: %s", uv_strerror(err));
    client_abort(client);
  } else {
    uv_read_stop((uv_stream_t *) &client->tcp);
    CLUSTERD_LOG(CLUSTERD_DEBUG, "Stopped reading");
  }
}

static void free_service(uv_handle_t *h) {
  controller_service *svc = CLUSTERD_STRUCT_FROM_FIELD(controller_service, tcp, h);
  CLUSTERD_LOG(CLUSTERD_DEBUG, "Freeing service");
  free(svc);
}

static void new_service(uv_stream_t *stream, int status) {
  int err;

  if ( status < 0 ) {
    CLUSTERD_LOG(CLUSTERD_CRIT, "Could not accept connection: %s", uv_strerror(status));
    exit(1);
  } else {
    controller_client *client;
    uv_tcp_t tcp;

    client = malloc(sizeof(controller_client));
    if (!client) goto error;

    lua_getglobal(g_lua, "clients");

    client->prev = client->next = NULL;
    client->writing = 0;
    client->thread = lua_newthread(g_lua);
    if ( !client->thread ) {
      lua_pop(g_lua, 1);
      free(client);
      goto error;
    }

    // Save the thread into the global dictionary
    lua_seti(g_lua, -2, (lua_Integer) client);
    lua_pop(g_lua, 1); // Pop the clients dict

    client->command_offs = 0;
    client->state = CLIENT_GET_COMMAND;
    client->needs_tx_write = 0;

    err = uv_tcp_init(stream->loop, &client->tcp);
    if ( err < 0 ) {
      CLUSTERD_LOG(CLUSTERD_CRIT, "Could not initialize TCP to accept connection: %s", uv_strerror(err));
      free(client);
      return;
    }

    err = uv_accept(stream, (uv_stream_t *)&client->tcp);
    if ( err < 0 ) {
      CLUSTERD_LOG(CLUSTERD_CRIT, "Could not accept TCP connection: %s", uv_strerror(err));
      free(client);
      return;
    }

    client_accept_command(client);

    return;

  error:
    CLUSTERD_LOG(CLUSTERD_CRIT, "Could not accept connection: out of memory");

    err = uv_tcp_init(stream->loop, &tcp);
    if ( err < 0 ) {
      CLUSTERD_LOG(CLUSTERD_CRIT, "Could not initialize TCP to discard connection: %s", uv_strerror(err));
      return;
    }

    err = uv_accept(stream, (uv_stream_t *)&tcp);
    if ( err < 0 ) {
      CLUSTERD_LOG(CLUSTERD_CRIT, "Could not accept client to close: %s", uv_strerror(err));
      return;
    }

    uv_close((uv_handle_t *)&tcp, do_nothing_close);
    return;
  }
}

static int start_local_service(char *srvaddrstr, struct raft *raft, uv_loop_t *loop) {
  struct sockaddr_storage addr;
  int err;

  err = clusterd_addr_parse(srvaddrstr, &addr, 1);
  if ( err < 0 ) {
    CLUSTERD_LOG(CLUSTERD_CRIT, "Could not parse local service address: %s: %s", srvaddrstr, strerror(errno));
    return -1;
  }

  g_service = malloc(sizeof(controller_service));
  if ( !g_service ) {
    CLUSTERD_LOG(CLUSTERD_CRIT, "Could not allocate service");
    return -1;
  }

  g_service->raft = raft;
  g_service->processing_head = g_service->processing_tail = NULL;

  err = uv_tcp_init(loop, &g_service->tcp);
  if ( err < 0 ) {
    CLUSTERD_LOG(CLUSTERD_CRIT, "Could not initialize service TCP: %s", uv_strerror(err));
    return -1;
  }

  err = uv_tcp_bind(&g_service->tcp, (struct sockaddr *)&addr, 0);
  if ( err < 0 ) {
    CLUSTERD_LOG(CLUSTERD_CRIT, "Could not bind address: %s", uv_strerror(err));
    uv_close((uv_handle_t *)&g_service->tcp, free_service);
    return -1;
  }

  err = uv_listen((uv_stream_t *)&g_service->tcp, 5, new_service);
  if ( err < 0 ) {
    CLUSTERD_LOG(CLUSTERD_CRIT, "Could not listen: %s", uv_strerror(err));
    uv_close((uv_handle_t *)&g_service->tcp, free_service);
    return -1;
  }

  return 0;
}

static int start_lua() {
  struct lua_library *lib;
  int err;

  g_lua = luaL_newstate();
  if ( !g_lua ) {
    CLUSTERD_LOG(CLUSTERD_CRIT, "No memory for lua interpreter");
    errno = ENOMEM;
    return -1;
  }

  // Open basic library
  for ( lib = baselibs; lib->func; ++lib ) {
    luaL_requiref(g_lua, lib->name, lib->func, 1);
    lua_pop(g_lua, 1);
  }

  // Load json library
  err = luaL_dostring(g_lua, rc_json_lua);
  if ( err != LUA_OK ) {
    CLUSTERD_LOG(CLUSTERD_CRIT, "Could not load json.lua");
    return -1;
  }
  lua_setglobal(g_lua, "json");

  // Now create the global 'commands' table
  lua_newtable(g_lua);
  lua_setglobal(g_lua, "commands");

  lua_newtable(g_lua);
  lua_setglobal(g_lua, "clients");

  return 0;
}

static int open_database() {
  int err;

  err = snprintf(g_dbfile, sizeof(g_dbfile), "%s/clusterd.db", g_datadir);
  if ( err >= sizeof(g_dbfile) ) {
    errno = ENAMETOOLONG;
    return -1;
  }

  g_database = clusterd_open_database(g_dbfile);
  if ( !g_database ) return -1;

  return 0;
}

int main(int argc, char *const *argv) {
  const char *bindaddrstr = NULL;
  char *srvaddrstr = NULL;
  int c, err, peers = 0, needs_bootstrap = 0;

  uv_loop_t loop;

  struct raft_configuration raft_conf;
  struct raft_uv_transport transport;
  struct raft_io io;
  struct raft raft;

  struct raft_fsm fsm;

  raft_id ourid;
  char *ouraddr;

  struct stat ddstat;

  setlocale(LC_ALL, "C");

  raft_configuration_init(&raft_conf);

  uv_loop_init(&loop);

  fsm.version = 1;
  fsm.data = NULL;
  fsm.apply = clusterd_apply;
  fsm.snapshot = clusterd_snapshot;
  fsm.restore = clusterd_restore;

  while ((c = getopt(argc, argv, "-vhBj:S:")) != -1) {
    switch (c) {
    case 1:
      /* Positional argument */
      if ( !g_datadir ) {
        g_datadir = optarg;
      } else if ( !bindaddrstr ) {
        bindaddrstr = g_datadir;
        g_datadir = optarg;
      } else {
        usage();
        return 1;
      }
      break;

    case 'B':
      needs_bootstrap = 1;
      break;

    case 'S':
      if ( srvaddrstr ) {
        CLUSTERD_LOG(CLUSTERD_CRIT, "Only one service address may be specified");
        usage();
        return 1;
      }

      srvaddrstr = optarg;
      break;

    case 'j':
      err = add_peer(optarg, &raft_conf);
      if ( err < 0 ) {
        CLUSTERD_LOG(CLUSTERD_CRIT, "Could not add peer %s", optarg);
        return 1;
      }
      peers++;
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

  if ( !g_datadir ) {
    CLUSTERD_LOG(CLUSTERD_CRIT, "No data directory provided");
    usage();
    return 1;
  }

  if ( !bindaddrstr ) {
    CLUSTERD_LOG(CLUSTERD_CRIT, "No bind address and node ID provided");
    usage();
    return 1;
  }

  err = add_peer((char *)bindaddrstr, &raft_conf);
  if ( err < 0 ) {
    CLUSTERD_LOG(CLUSTERD_CRIT, "Could not add ourselves as a peer");
    return 1;
  }

  err = raft_uv_tcp_init(&transport, &loop);
  if ( err != 0 ) {
    CLUSTERD_LOG(CLUSTERD_CRIT, "Could not set up TCP transport: %s", raft_strerror(err));
    return 1;
  }

  err = raft_uv_init(&io, &loop, g_datadir, &transport);
  if ( err != 0 ) {
    CLUSTERD_LOG(CLUSTERD_CRIT, "Could not initialize raft data store: %s", raft_strerror(err));
    return 1;
  }

  // Either read or generate a unique node ID
  err = split_nodeid_addr(bindaddrstr, &ourid, &ouraddr);
  if ( err < 0 ) {
    CLUSTERD_LOG(CLUSTERD_CRIT, "Invalid bind address");
    return 1;
  }

  CLUSTERD_LOG(CLUSTERD_INFO, "Starting cluster on address: %s (node id=%llu)", ouraddr, ourid);

  // TODO recursively make g_datadir
  err = stat(g_datadir, &ddstat);
  if ( err < 0 ) {
    if ( errno == ENOENT ) {
      char cmd[512 + PATH_MAX];

      if ( peers == 0 ) {
        CLUSTERD_LOG(CLUSTERD_INFO, "Bootstrapping node with no peers.");
        CLUSTERD_LOG(CLUSTERD_INFO, "If this is the first node you're starting, ignore this message");
        CLUSTERD_LOG(CLUSTERD_INFO, "Welcome to Clusterd!");
      }

      // Make the directory and bootstrap
      err = snprintf(cmd, sizeof(cmd), "mkdir -p %s", g_datadir);
      if ( err >= sizeof(cmd) )  {
        CLUSTERD_LOG(CLUSTERD_CRIT, "Data dir path is too long");
        return 1;
      }

      err = system(cmd);
      if ( err != 0 ) {
        CLUSTERD_LOG(CLUSTERD_CRIT, "Could not create %s: mkdir exited with %d", g_datadir, err);
        return 1;
      }

      needs_bootstrap = 1;
    } else {
      CLUSTERD_LOG(CLUSTERD_CRIT, "Could not stat data dir %s: %s", g_datadir, strerror(errno));
      return 1;
    }
  } else if ( peers > 0 && needs_bootstrap == 0 ) {
    CLUSTERD_LOG(CLUSTERD_CRIT, "Peers were given to bootstrap, but this node is already participating in a cluster");
    CLUSTERD_LOG(CLUSTERD_INFO, "Try running without -j");
    return 1;
  }

  err = raft_uv_tcp_init(&transport, &loop);
  if ( err != 0 ) {
    CLUSTERD_LOG(CLUSTERD_CRIT, "Could not set up TCP: %s", raft_strerror(err));
    return 1;
  }

  err = raft_uv_init(&io, &loop, g_datadir, &transport);
  if ( err != 0 ) {
    CLUSTERD_LOG(CLUSTERD_CRIT, "Could not initialize transport: %s", raft_strerror(err));
    return 1;
  }

  lua_tx_raft = &raft;

  err = raft_init(&raft, &io, &fsm, ourid, ouraddr);
  if ( err != 0 ) {
    CLUSTERD_LOG(CLUSTERD_CRIT, "Could not initialize raft: %s (%d)", raft_errmsg(&raft), err);
    return 1;
  }

  if ( CLUSTERD_LOG_LEVEL == CLUSTERD_DEBUG ) {
    raft.tracer = &debug_tracer;
    raft_uv_set_tracer(&io, &debug_tracer);
  }

  if ( needs_bootstrap ) {
    CLUSTERD_LOG(CLUSTERD_DEBUG, "Bootstrapping node");

    err = raft_bootstrap(&raft, &raft_conf);
    if ( err != 0 ) {
      CLUSTERD_LOG(CLUSTERD_CRIT, "Could not bootstrap node: %s", raft_errmsg(&raft));
      return 1;
    }
  }

  raft_set_snapshot_threshold(&raft, 64);
  raft_set_snapshot_trailing(&raft, 16);
  raft_set_election_timeout(&raft, 200);
  raft_set_pre_vote(&raft, true);

  err = raft_start(&raft);
  if ( err != 0 ) {
    CLUSTERD_LOG(CLUSTERD_CRIT, "Could not start raft service: %s", raft_errmsg(&raft));
    CLUSTERD_LOG(CLUSTERD_CRIT, "Returned error: %s", raft_strerror(err));
    return 1;
  }

  if ( srvaddrstr ) {
    err = start_local_service(srvaddrstr, &raft, &loop);
    if ( err < 0 ) {
      CLUSTERD_LOG(CLUSTERD_CRIT, "Could not start local service");
      return 1;
    }
  }

  err = start_lua();
  if ( err < 0 ) {
    CLUSTERD_LOG(CLUSTERD_CRIT, "Could not start Lua");
    return 1;
  }

  err = open_database();
  if ( err < 0 ) {
    CLUSTERD_LOG(CLUSTERD_CRIT, "Could not open SQLite3");
    return 1;
  }

  // Make sure to ignore sigpipe
  if ( signal(SIGPIPE, SIG_IGN) == SIG_ERR )
    CLUSTERD_LOG(CLUSTERD_WARNING, "Could not set SIGPIPE to ignore: %s", strerror(errno));

  uv_run(&loop, UV_RUN_DEFAULT);

  uv_loop_close(&loop);

  return 0;
}
