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
#define CLUSTERD_COMPONENT "clusterd-schedule"
#include "clusterd/log.h"
#include "clusterd/common.h"
#include "libclusterctl.h"

#include <getopt.h>
#include <locale.h>
#include <time.h>
#include <math.h>
#include <jansson.h>
#include <uthash.h>
#include <pthread.h>
#include <signal.h>
#include <stdatomic.h>

#include <sys/wait.h>

/*
 * Basic idea here is that we take a namespace and a service to
 * schedule, as well as extra constraints.
 *
 * We output nodes, sorted by weight.
 *
 * Nodes are weighted based on how free they are in relation to one
 * another. The most utilized node gets weight 1.0.
 */

struct limit_request {
  UT_hash_handle hh;

  const char *resource;

  json_int_t quantity;
  unsigned int strictly_required : 1;

  double weight; // Override limit weight for weightings. Only valid if really a number
};

struct global_resource_constraint {
  const char *resource_name;
  const char *relation;
  unsigned int invert : 1; // If 1, then don't schedule on a node hosting this resource
};

struct affinity_constraint {
  const char *namespace;
  clusterd_pid_t pid;
  unsigned int invert : 1; // If 1, then avoid this process
};

struct avail_resource {
  UT_hash_handle hh;

  struct limit_request *resource;
  unsigned int available : 1; // If 1, then this resource is available, otherwise, it doesn't have this
  double total_quantity;
  double in_use;
};

struct node_entry {
  char nodeid[37]; // Enough space for a UUID + nul byte
  char hostname[256]; // Enough space for a hostname
  char ip[CLUSTERD_ADDRSTRLEN];

  unsigned int resource_fetch_attempts; // Number of times resource information for this node has been fetched

  struct node_entry *next_to_fetch;

  double score;

  struct avail_resource *resources;
};

int CLUSTERD_LOG_LEVEL = CLUSTERD_INFO;

static pthread_t g_main_thread;
static sig_atomic_t g_alarm_sounded = 0;
static struct limit_request *g_global_limits = NULL;

static size_t g_resource_constraints_size = 0, g_resource_constraints_capacity = 0;
static struct global_resource_constraint *g_resource_constraints = NULL;

static pthread_mutex_t g_chosen_node_mutex;
static struct node_entry **g_chosen_nodes;
static unsigned int g_chosen_node_count = 0, g_chosen_node_capacity = 0;

static pthread_mutex_t g_node_candidate_queue_mutex;
static pthread_cond_t g_node_candidate_queue_cond;
static int g_node_threads_started = 0;
static int g_nodes_complete = 0; // Set to 1 when the main thread finishes downloading the node list
static struct node_entry *g_node_candidate_queue = NULL, *g_node_candidate_queue_tail = NULL;

static char *g_clusterd_stats_cmd = NULL;

static const char find_node_lua[] =
  "c = json.decode(params.constraints)\n"
  "nodes = clusterd.list_nodes(c)\n"
  "for _, node in ipairs(nodes) do\n"
  "  clusterd.output(json.encode(node))\n"
  "end\n";

static void free_node_entry(struct node_entry *ent) {
  struct avail_resource *r, *rtmp;

  HASH_ITER(hh, ent->resources, r, rtmp) {
    HASH_DEL(ent->resources, r);
    free(r);
  }

  free(ent);
}

static int update_global_limit(struct limit_request *r, int append) {
  struct limit_request *existing;

  HASH_FIND_STR(g_global_limits, r->resource, existing);
  if ( existing ) {
    if ( append ) {
      existing->strictly_required = existing->strictly_required || r->strictly_required;
      existing->quantity += r->quantity;
    } else {
      CLUSTERD_LOG(CLUSTERD_ERROR, "Duplicate limit request provided: %s", r->resource);
      return -1;
    }
  } else {
    struct limit_request *new_request;

    new_request = malloc(sizeof(*new_request));
    if ( !new_request ) {
      CLUSTERD_LOG(CLUSTERD_CRIT, "Cannot allocate new limit request");
      return -1;
    }

    memcpy(new_request, r, sizeof(*new_request));

    HASH_ADD_KEYPTR(hh, g_global_limits, r->resource, strlen(r->resource), new_request);
  }

  return 0;
}

static int add_global_limit(const char *limitspec) {
  /// Global limits look like name=value[!]
  const char *equals, *afterlimit;
  struct limit_request r;
  int err, slen;

  equals = strchr(limitspec, '=');

  r.weight = 1;

  if ( equals ) {
    r.resource = strndup(limitspec, equals - limitspec);
    if ( !r.resource ) {
      CLUSTERD_LOG(CLUSTERD_CRIT, "Could not add limit %s: out of memory", limitspec);
      return -1;
    }

    err = sscanf(equals, "=%" JSON_INTEGER_FORMAT "%n", &r.quantity, &slen);
    if ( err != 1 ) {
      CLUSTERD_LOG(CLUSTERD_ERROR, "Limit value must be an unsigned integer");
      return -1;
    }

    afterlimit = equals + slen;

    if ( *afterlimit == '\0' ) {
      r.strictly_required = 1;
    }

    if ( *afterlimit == '@' ) {
      // Parse weight
      err = sscanf(afterlimit, "@%lf%n", &r.weight, &slen);
      if ( err != 1 ) {
        CLUSTERD_LOG(CLUSTERD_ERROR, "Limit weighting must be a valid double");
        return -1;
      }

      afterlimit = afterlimit + slen;
    }

    if ( *afterlimit == '!' ) {
      r.strictly_required = 1;
      afterlimit ++;
    } else
      r.strictly_required = 0;

    if ( *afterlimit != '\0' ) {
      CLUSTERD_LOG(CLUSTERD_ERROR, "Junk at end of limit specification");
      return -1;
    }
  } else {
    r.resource = limitspec;
    r.quantity = 0;
    r.strictly_required = 1;
  }

  return update_global_limit(&r, 0);
}

static int add_global_resource_constraint(const char *rcspec) {
  struct global_resource_constraint c;
  const char *namestart = rcspec, *colon;

  if ( *rcspec == '!' ) {
    c.invert = 1;
    namestart ++;
  } else
    c.invert = 0;

  colon = strchr(namestart, ':');

  if ( colon ) {
    c.resource_name = strndup(namestart, colon - namestart);
    if ( !c.resource_name ) {
      CLUSTERD_LOG(CLUSTERD_CRIT, "Could not add global resource constraint %s: out of memory", rcspec);
      return -1;
    }

    c.relation = strdup(colon + 1);
    if ( !c.relation ) {
      CLUSTERD_LOG(CLUSTERD_CRIT, "Could not add global resource constraint %s: out of memory", rcspec);
      return -1;
    }

    if ( strlen(c.relation) == 0 ) {
      CLUSTERD_LOG(CLUSTERD_ERROR, "The relation name in global resource constraint %s must not be blank", rcspec);
      return -1;
    }
  } else {
    c.resource_name = namestart;
    c.relation = "default";
  }

  if ( g_resource_constraints_size >= g_resource_constraints_capacity ) {
    struct global_resource_constraint *new_constraints;

    if ( g_resource_constraints_capacity == 0 )
      g_resource_constraints_capacity = 4;
    else
      g_resource_constraints_capacity *= 2;

    new_constraints = realloc(g_resource_constraints, sizeof(*g_resource_constraints) * g_resource_constraints_capacity);
    if ( !new_constraints ) {
      CLUSTERD_LOG(CLUSTERD_CRIT, "Cannot add global resource constraint %s: out of memory", rcspec);
      return -1;
    }

    g_resource_constraints = new_constraints;
  }

  memcpy(g_resource_constraints + g_resource_constraints_size, &c, sizeof(c));
  g_resource_constraints_size ++;

  return 0;
}

static json_t *format_resource_constraint(struct global_resource_constraint *c) {
  return json_pack("{sssssb}", "resource", c->resource_name, "rel", c->relation,
                   "invert", c->invert);
}

static json_t *format_limit_json(struct limit_request *r) {
  return json_pack("{sssisb}", "resource", r->resource, "quantity", r->quantity,
                   "required", r->strictly_required);
}

static int start_node_collection(clusterctl *ctl) {
  json_t *schedule_constraints, *resource_constraints, *limit_constraints;
  char *constraints_json_string;
  int i, err;

  struct limit_request *limit, *limittmp;

  resource_constraints = json_array();
  if ( !resource_constraints ) goto out_of_memory_json;

  limit_constraints = json_array();
  if ( !limit_constraints ) goto out_of_memory_json;

  for ( i = 0; i < g_resource_constraints_size; ++i ) {
    json_t *resource_json;

    resource_json = format_resource_constraint(g_resource_constraints + i);
    if ( !resource_json ) goto out_of_memory_json;

    if ( json_array_append_new(resource_constraints, resource_json) < 0 )
      goto out_of_memory_json;
  }

  HASH_ITER(hh, g_global_limits, limit, limittmp) {
    json_t *limit_json;

    limit_json = format_limit_json(limit);
    if ( !limit_json ) goto out_of_memory_json;

    if ( json_array_append_new(limit_constraints, limit_json) < 0 )
      goto out_of_memory_json;
  }

  schedule_constraints = json_pack("{soso}",
                                   "resources", resource_constraints,
                                   "limits", limit_constraints);
  if ( !schedule_constraints ) goto out_of_memory_json;

  constraints_json_string = json_dumps(schedule_constraints, 0);
  json_decref(schedule_constraints);

  if ( !constraints_json_string ) goto out_of_memory_json;

  CLUSTERD_LOG(CLUSTERD_DEBUG, "Sending node query %s", constraints_json_string);

  err = clusterctl_simple(ctl, CLUSTERCTL_STALE_READS, find_node_lua,
                          "constraints", constraints_json_string,
                          NULL);
  free(constraints_json_string);
  if ( err < 0 ) {
    CLUSTERD_LOG(CLUSTERD_CRIT, "Could not lookup nodes");
    return -1;
  }

  return 0;

 out_of_memory_json:
  CLUSTERD_LOG(CLUSTERD_ERROR, "Could not start node collection: out of memory while building JSON constraints");
  errno = ENOMEM;
  return -1;
}

static void mark_nodes_complete() {
  if ( pthread_mutex_lock(&g_node_candidate_queue_mutex) != 0 ) {
    CLUSTERD_LOG(CLUSTERD_ERROR, "Could not mark nodes complete: mutex locking failed: %s", strerror(errno));
    exit(EXIT_FAILURE);
  }

  g_nodes_complete = 1;

  if ( pthread_cond_broadcast(&g_node_candidate_queue_cond) != 0 ) {
    CLUSTERD_LOG(CLUSTERD_ERROR, "Could not wake up threads: %s", strerror(errno));
  }

  if ( pthread_mutex_unlock(&g_node_candidate_queue_mutex) != 0 ) {
    CLUSTERD_LOG(CLUSTERD_ERROR, "Could not mark nodes complete: mutex unlocking failed");
    exit(EXIT_FAILURE);
  }
}

static int node_processing_complete() {
  int ret;

  if ( pthread_mutex_lock(&g_node_candidate_queue_mutex) != 0 ) {
    CLUSTERD_LOG(CLUSTERD_ERROR, "Could not check if node processing complete: mutex locking failed: %s", strerror(errno));
    return 0;
  }

  ret = g_node_candidate_queue == NULL && g_node_threads_started == 0;

  if ( pthread_mutex_unlock(&g_node_candidate_queue_mutex) != 0 ) {
    CLUSTERD_LOG(CLUSTERD_ERROR, "Could not check if processing complete: mutex unlocking failed");
    return 0;
  }

  return ret;
}

static int process_node_line(struct node_entry *node, const char *line, size_t linelen) {
  int err;
  char resource_name[256];
  double qty;

  struct limit_request *limit;
  struct avail_resource *node_rc;

  if ( linelen == 0 ) return 0;

  err = sscanf(line, "%256s %lf", resource_name, &qty);
  if ( err != 2 ) {
    CLUSTERD_LOG(CLUSTERD_ERROR, "Status line '%.*s' is malformed", (int)linelen, line);
    return -1;
  }

  // Attempt to lookup the resource
  HASH_FIND_STR(g_global_limits, resource_name, limit);
  if ( !limit ) {
    CLUSTERD_LOG(CLUSTERD_DEBUG, "Skipping resource %s", resource_name);
    return 0;
  }

  HASH_FIND_PTR(node->resources, &limit, node_rc);
  if ( !node_rc || !node_rc->available ) {
    CLUSTERD_LOG(CLUSTERD_DEBUG, "No max limit for resource '%s' specified for node %s. Limit should be %p",
                 resource_name, node->hostname, limit);
    return 0;
  }

  node_rc->in_use = qty;

  return 0;
}

static int collect_node_stats(struct node_entry *node) {
  char *ssh_args[] = {
    "ssh", "-l", "clusterd", node->ip, g_clusterd_stats_cmd, NULL
  };
  int stspipe[2], err;
  pid_t child;

  // Create pipe, fork and run ssh
  err = pipe(stspipe);
  if ( err < 0 ) {
    CLUSTERD_LOG(CLUSTERD_ERROR, "Could not create pipe for SSH connection");
    return -1;
  }

  child = fork();
  if ( child < 0 ) {
    int serrno = errno;
    close(stspipe[0]);
    close(stspipe[1]);
    CLUSTERD_LOG(CLUSTERD_ERROR, "Could not fork: %s", strerror(errno));
    return -1;
  } else if ( child == 0 ) {
    // Run ssh
    close(stspipe[0]);

    err = dup2(stspipe[1], STDOUT_FILENO);
    if ( err < 0 ) {
      CLUSTERD_LOG(CLUSTERD_ERROR, "Could not redirect stdout: %s", strerror(errno));
      exit(EXIT_FAILURE);
    }
    close(stspipe[1]);

    // Now run ssh
    execvp("ssh", ssh_args);
    exit(EXIT_FAILURE);
  } else {
    FILE *output = NULL;
    char *line = NULL;
    size_t linelen = 0;
    int ret = 0, wsts;

    close(stspipe[1]); // No need to write

    output = fdopen(stspipe[0], "rt");
    if ( !output ) {
      CLUSTERD_LOG(CLUSTERD_ERROR, "Could not open pipe end %d: %s", stspipe[0], strerror(errno));
      ret = -1;
      close(stspipe[0]);
      goto done;
    }

    while ( getline(&line, &linelen, output) >= 0 ) {
      CLUSTERD_LOG(CLUSTERD_DEBUG, "Got line %.*s", (int)linelen, line);

      err = process_node_line(node, line, linelen);
      if ( err < 0 ) {
        CLUSTERD_LOG(CLUSTERD_ERROR, "Could not process line: %.*s", (int)linelen, line);
        ret = -1;
        goto done;
      }
    }

    if ( errno ) {
      CLUSTERD_LOG(CLUSTERD_ERROR, "Could not finish reading status: %s", strerror(errno));

      ret = -1;
      goto done;
    }

  done:
    if ( line ) free(line);
    if ( output ) fclose(output);

    // Wait for child to end
    if ( waitpid(child, &wsts, 0) < 0 ) {
      CLUSTERD_LOG(CLUSTERD_ERROR, "Could not wait for ssh status: %s", strerror(errno));
    }

    if ( !WIFEXITED(wsts) || WEXITSTATUS(wsts) != 0 ) {
      if ( WIFEXITED(wsts) )
        CLUSTERD_LOG(CLUSTERD_ERROR, "SSH command exited with error %d for node %s",
                     WEXITSTATUS(wsts), node->hostname);
      else
        CLUSTERD_LOG(CLUSTERD_ERROR, "SSH command exited abnormally for node %s",
                     node->hostname);
      ret = -1;
    }

    return ret;
  }
}

static int score_node(struct node_entry *node) {
  double score = 0;
  struct limit_request *limit, *limittmp;

  // Check if every required limit is available
  HASH_ITER(hh, g_global_limits, limit, limittmp) {
    struct avail_resource *rc;
    double pct_used;

    HASH_FIND_PTR(node->resources, &limit, rc);

    if ( !rc || !rc->available ) {
      if ( limit->strictly_required ) {
        CLUSTERD_LOG(CLUSTERD_DEBUG, "Skipping node %s because required resource %s is missing",
                     node->hostname, limit->resource);
        return -1;
      } else
        continue;
    }

    if ( rc->in_use > rc->total_quantity )
      pct_used = 1;
    else
      pct_used = rc->in_use / rc->total_quantity;

    CLUSTERD_LOG(CLUSTERD_DEBUG, "[%s] resource '%s', %lf in use / %lf total (%lf in use)",
                 node->hostname, limit->resource, rc->in_use, rc->total_quantity, pct_used);

    score += (1 - pct_used) * limit->weight;
    CLUSTERD_LOG(CLUSTERD_DEBUG, "[%s] score is now %lf (after %lf, 1 - pct = %lf, weight = %lf)",
                 node->hostname, score, (1 - pct_used) * limit->weight,
                 1 - pct_used, limit->weight);
  }

  node->score = score;

  CLUSTERD_LOG(CLUSTERD_DEBUG, "Score for %s is %lf", node->hostname, node->score);

  return 0;
}

static int select_node(struct node_entry *n) {
  struct node_entry **new_place;
  int lo = 0, hi = g_chosen_node_count - 1;

  if ( pthread_mutex_lock(&g_chosen_node_mutex) != 0 ) {
    CLUSTERD_LOG(CLUSTERD_ERROR, "Could not lock chosen nodes mutex");
    return -1;
  }

  if ( hi < 0 ) hi = 0;

  CLUSTERD_LOG(CLUSTERD_DEBUG, "Finding spot for score %lf in range %d %d", n->score, lo, hi);
  // Find insertion point
  while ( hi >= lo && g_chosen_node_count > 0 ) {
    int mid = (hi + lo) / 2;

    CLUSTERD_LOG(CLUSTERD_DEBUG, "Examine node %s(index=%d, score=%lf) versus %lf",
                 g_chosen_nodes[mid]->hostname, mid, g_chosen_nodes[mid]->score, n->score);

    if ( g_chosen_nodes[mid]->score >= n->score ) {
      lo = mid + 1;
    } else {
      hi = mid - 1;
    }

    CLUSTERD_LOG(CLUSTERD_DEBUG, "Will examine %d %d", lo, hi);
  }

  CLUSTERD_LOG(CLUSTERD_DEBUG, "[%s] Got insertion point %d %d", n->hostname, lo, hi);

  if ( lo == hi + 1 || g_chosen_node_count == 0 ) {
    int new_index = lo;

    if ( new_index >= g_chosen_node_capacity ) {
      CLUSTERD_LOG(CLUSTERD_DEBUG, "Node score %lf is lower than any other score, deleting", n->score);
      free_node_entry(n);
    } else if ( new_index < g_chosen_node_count ) {
      if ( g_chosen_node_count >= g_chosen_node_capacity ) {
        free_node_entry(g_chosen_nodes[g_chosen_node_count - 1]);
      } else
        g_chosen_node_count ++;

      // Shift over
      memcpy(g_chosen_nodes + new_index + 1, g_chosen_nodes + new_index,
             (g_chosen_node_count - new_index - 1) * sizeof(*g_chosen_nodes));

      g_chosen_nodes[new_index] = n;
    } else {
      g_chosen_node_count ++;
      g_chosen_nodes[new_index] = n;
    }
  }

  if ( pthread_mutex_unlock(&g_chosen_node_mutex) != 0 ) {
    CLUSTERD_LOG(CLUSTERD_DEBUG, "Could not unlock chosen nodes mutex");
    return -1;
  }

  return 0;
}

static void *node_processor_worker(void *arg) {
  for (;;) {
    struct node_entry *node = NULL;
    int err;

    if ( pthread_mutex_lock(&g_node_candidate_queue_mutex) != 0 ) {
      CLUSTERD_LOG(CLUSTERD_ERROR, "Could not process nodes: mutex locking failed: %s", strerror(errno));
      return NULL;
    }

    while ( !g_node_candidate_queue ) {
      if ( g_nodes_complete ) {
	g_node_threads_started--;
	CLUSTERD_LOG(CLUSTERD_DEBUG, "Thread exiting");

	if ( pthread_mutex_unlock(&g_node_candidate_queue_mutex) != 0 ) {
	  CLUSTERD_LOG(CLUSTERD_ERROR, "Could not unlock node candidate mutex while quitting: %s", strerror(errno));
	}

	// Send SIGCONT to main thread to interrupt the pause()
	if ( pthread_kill(g_main_thread, SIGCONT) < 0 ) {
	  CLUSTERD_LOG(CLUSTERD_WARNING, "Could not wake-up main thread: %s", strerror(errno));
	}

	return NULL;
      }

      CLUSTERD_LOG(CLUSTERD_DEBUG, "Waiting for node to process");
      if ( pthread_cond_wait(&g_node_candidate_queue_cond, &g_node_candidate_queue_mutex) != 0 ) {
	CLUSTERD_LOG(CLUSTERD_ERROR, "Could not wait for node candidate: %s", strerror(errno));
	if ( pthread_mutex_unlock(&g_node_candidate_queue_mutex) != 0 ) {
	  CLUSTERD_LOG(CLUSTERD_CRIT, "Could not unlock mutex during failure: %s", strerror(errno));
	}
	return NULL;
      }
    }

    node = g_node_candidate_queue;
    g_node_candidate_queue = g_node_candidate_queue->next_to_fetch;
    if ( !g_node_candidate_queue )
      g_node_candidate_queue_tail = NULL;
    CLUSTERD_LOG(CLUSTERD_DEBUG, "Queue is now %p", g_node_candidate_queue);

    if ( pthread_mutex_unlock(&g_node_candidate_queue_mutex) != 0 ) {
      CLUSTERD_LOG(CLUSTERD_WARNING, "Could not unlock node candidate mutex");
      // Proceed anyway because we want to get the info for this node entry
    }

    CLUSTERD_LOG(CLUSTERD_DEBUG, "Processing node %s", node->hostname);

    err = collect_node_stats(node);
    if ( err < 0 ) {
      CLUSTERD_LOG(CLUSTERD_WARNING, "Could not collect stats for %s: ignoring", node->hostname);
      free_node_entry(node);
      goto node_processing_complete;
    }

    // Make sure all required resources were actually present
    err = score_node(node);
    if ( err < 0 ) {
      CLUSTERD_LOG(CLUSTERD_WARNING, "Discarding %s because some resources were not really present", node->hostname);
      free_node_entry(node);
      goto node_processing_complete;
    }

    // Now the score is valid, so let's see if it would go into our set
    err = select_node(node);
    if ( err < 0 ) {
      CLUSTERD_LOG(CLUSTERD_WARNING, "Error while selecting node %s", node->hostname);
      goto node_processing_complete;
    }

  node_processing_complete:
    continue;
  }
}

static void start_threads() {
  int nprocs, i, err;
  sigset_t sigalrm, oldmask;

  if ( pthread_mutex_init(&g_node_candidate_queue_mutex, NULL) != 0 ) {
    CLUSTERD_LOG(CLUSTERD_CRIT, "Could not initialize node candidate mutex");
    exit(EXIT_FAILURE);
  }

  if ( pthread_cond_init(&g_node_candidate_queue_cond, NULL) != 0 ) {
    CLUSTERD_LOG(CLUSTERD_CRIT, "Could not initialize node candidate condition variable");
    exit(EXIT_FAILURE);
  }

  if ( pthread_mutex_init(&g_chosen_node_mutex, NULL) != 0 ) {
    CLUSTERD_LOG(CLUSTERD_CRIT, "Could not initialize chosen nodes mutex");
    exit(EXIT_FAILURE);
  }

  sigemptyset(&sigalrm);
  sigaddset(&sigalrm, SIGALRM);

  err = pthread_sigmask(SIG_BLOCK, &sigalrm, &oldmask);
  if ( err != 0 ) {
    CLUSTERD_LOG(CLUSTERD_ERROR, "Could not block SIGALRM in child threads: %s", strerror(errno));
    exit(EXIT_FAILURE);
  }

  nprocs = sysconf(_SC_NPROCESSORS_ONLN);
  if ( nprocs < 0 ) {
    CLUSTERD_LOG(CLUSTERD_WARNING, "Could not get processor count: %s", strerror(errno));
    nprocs = 2;
  }

  nprocs += 1;
  if ( nprocs > 8 )
    nprocs = 8; // Cap out at eight max simultaneous requests

  if ( pthread_mutex_lock(&g_node_candidate_queue_mutex) != 0 )
    CLUSTERD_LOG(CLUSTERD_WARNING, "Could not lock candidate queue while making threads");

  for ( i = 0; i < nprocs; ++i ) {
    pthread_t hdl;

    err = pthread_create(&hdl, NULL, node_processor_worker, NULL);
    if ( err < 0 ) {
      CLUSTERD_LOG(CLUSTERD_WARNING, "Could not launch node processor worker: %s", strerror(errno));
    } else
      g_node_threads_started++;
  }

  if ( !g_node_threads_started ) {
    CLUSTERD_LOG(CLUSTERD_CRIT, "Could not start any node worker threads");
    exit(EXIT_FAILURE);
  }

  if ( pthread_mutex_unlock(&g_node_candidate_queue_mutex) != 0 )
    CLUSTERD_LOG(CLUSTERD_WARNING, "Could not unlock candidate queue");

  err = pthread_sigmask(SIG_SETMASK, &oldmask, NULL);
  if ( err != 0 ) {
    CLUSTERD_LOG(CLUSTERD_ERROR, "Could not restore signal set. Alarm may not sound");
  }
}

static int enqueue_node(struct node_entry *entry) {
  int ret = 0;

  CLUSTERD_LOG(CLUSTERD_DEBUG, "Enqueueing node %s (hostname=%s)", entry->nodeid, entry->hostname);

  if ( !g_node_threads_started )
    start_threads();

  if ( pthread_mutex_lock(&g_node_candidate_queue_mutex) != 0 ) {
    CLUSTERD_LOG(CLUSTERD_WARNING, "Could not enqueue node entry: could not lock mutex");
    free_node_entry(entry);
    return -1;
  }

  entry->next_to_fetch = NULL;

  if ( !g_node_candidate_queue_tail ) {
    g_node_candidate_queue_tail = g_node_candidate_queue = entry;
  } else {
    g_node_candidate_queue_tail->next_to_fetch = entry;
    g_node_candidate_queue_tail = entry;
  }

  if ( pthread_cond_signal(&g_node_candidate_queue_cond) != 0 ) {
    CLUSTERD_LOG(CLUSTERD_WARNING, "Could not signal candidate queue threads: %s", strerror(errno));
    ret = -1;
  }

 done:
  if ( pthread_mutex_unlock(&g_node_candidate_queue_mutex) != 0 ) {
    CLUSTERD_LOG(CLUSTERD_WARNING, "Could not unlock node queue mutex");
    return -1;
  }

  return ret;
}

static int add_node(const char *nodebuf, size_t nodelen) {
  json_t *nodeinfo;
  json_error_t jsonerr;
  int err;
  const char *jsonname, *jsonid, *jsonip;
  struct limit_request *limit, *limittmp;

  struct node_entry *entry;

  CLUSTERD_LOG(CLUSTERD_DEBUG, "Got node %.*s", (int)nodelen, nodebuf);

  nodeinfo = json_loadb(nodebuf, nodelen, 0, &jsonerr);
  if ( !nodeinfo ) {
    CLUSTERD_LOG(CLUSTERD_WARNING, "Could not interpret node: %.*s", (int)nodelen, nodebuf);
    CLUSTERD_LOG(CLUSTERD_WARNING, "  %d:%d (byte %d): %s", jsonerr.line, jsonerr.column, jsonerr.position,
                 jsonerr.text);
    return -1;
  }

  err = json_unpack(nodeinfo, "{ssssss}", "hostname", &jsonname, "id", &jsonid, "ip", &jsonip);
  if ( err < 0 ) {
    CLUSTERD_LOG(CLUSTERD_ERROR, "Could not access node name or id: %.*s", (int)nodelen, nodebuf);

    goto error;
  }

  if ( strlen(jsonid) >= sizeof(entry->nodeid) ||
       strlen(jsonname) >= sizeof(entry->hostname) ||
       strlen(jsonip) >= sizeof(entry->ip) ) {
    CLUSTERD_LOG(CLUSTERD_ERROR, "Node ID, IP, or hostname is too long");
    goto error;
  }

  entry = malloc(sizeof(*entry));
  if ( !entry ) {
    CLUSTERD_LOG(CLUSTERD_CRIT, "Could not allocate node entry for node %s", jsonname);
    goto error;
  }

  strncpy(entry->nodeid, jsonid, sizeof(entry->nodeid));
  strncpy(entry->hostname, jsonname, sizeof(entry->hostname));
  strncpy(entry->ip, jsonip, sizeof(entry->ip));
  entry->resource_fetch_attempts = 0;
  entry->resources = NULL;
  entry->next_to_fetch = NULL;
  entry->score = NAN;

  // Now parse all resources
  HASH_ITER(hh, g_global_limits, limit, limittmp) {
    char rc_column_name[1024];
    double quantity;
    struct avail_resource *new_rc;

    CLUSTERD_LOG(CLUSTERD_DEBUG, "Examine limit %s", limit->resource);

    err = snprintf(rc_column_name, sizeof(rc_column_name), "avail_%s", limit->resource);
    if ( err >= sizeof(rc_column_name) ) {
      CLUSTERD_LOG(CLUSTERD_ERROR, "Resource name %s is too long", limit->resource);
      goto error;
    }

    new_rc = malloc(sizeof(*new_rc));
    if ( !new_rc ) {
      CLUSTERD_LOG(CLUSTERD_CRIT, "Could not allocate resource entry for %s in node %s",
                   limit->resource, jsonname);
      goto error;
    }

    new_rc->resource = limit;
    new_rc->in_use = 0;

    err = json_unpack(nodeinfo, "{sF}", rc_column_name, &quantity);
    if ( err < 0 ) {
      // Not available
      new_rc->available = 0;
      new_rc->total_quantity = 0;
    } else {
      new_rc->available = 1;
      new_rc->total_quantity = quantity;
    }

    CLUSTERD_LOG(CLUSTERD_DEBUG, "Get column %s(limit %p) for %s: %d %lf",
                 rc_column_name, limit, jsonname, err, quantity);

    HASH_ADD_PTR(entry->resources, resource, new_rc);
  }

  json_decref(nodeinfo);

  err = enqueue_node(entry);
  if ( err < 0 ) {
    CLUSTERD_LOG(CLUSTERD_ERROR, "Could not add node %s", entry->nodeid);
    free_node_entry(entry);
    return -1;
  }

  return 0;

 error:
  json_decref(nodeinfo);
  return -1;
}

static void handle_timeout(int timeout) {
  g_alarm_sounded = 1;
}

static void handle_cont(int cont) {
}

static void start_timeout(int timeout) {
  struct sigaction alarm_handler, cont_handler;
  int err;

  alarm_handler.sa_handler = handle_timeout;
  sigemptyset(&alarm_handler.sa_mask);
  sigaddset(&alarm_handler.sa_mask, SIGCONT);
  alarm_handler.sa_flags = 0;

  err = sigaction(SIGALRM, &alarm_handler, NULL);
  if ( err < 0 ) {
    CLUSTERD_LOG(CLUSTERD_CRIT, "Could not set alarm handler: %s", strerror(errno));
    return;
  }

  cont_handler.sa_handler = handle_cont;
  sigemptyset(&cont_handler.sa_mask);
  cont_handler.sa_flags = 0;

  err = sigaction(SIGCONT, &cont_handler, NULL);
  if ( err < 0 ) {
    CLUSTERD_LOG(CLUSTERD_CRIT, "Could not remove SA_RESTART from SIGCONT: %s", strerror(errno));
    return;
  }

  alarm(timeout);
}

static void usage() {
  fprintf(stderr, "clusterd-schedule -- find nodes to run a clusterd payload\n");
  fprintf(stderr, "Usage: clusterd-schedule -vhf [-l LIMIT...] [-g RESOURCE...]\n");
  fprintf(stderr, "         [-N COUNT] [-t TIMEOUT]\n\n");
  fprintf(stderr, "   -N COUNT[:MAX]   Max number of nodes to return, optionally with the max to examine\n");
  fprintf(stderr, "   -l LIMIT         Specify additional limit requests. Limit requests take\n");
  fprintf(stderr, "                    the form RESOURCECLASS[=REQUEST][!][@WEIGHT].\n");
  fprintf(stderr, "   -g RESOURCE:REL  Ask for affinity on nodes with the given relation to the\n");
  fprintf(stderr, "                    global resource\n");
  fprintf(stderr, "   -t TIMEOUT       Max timeout in seconds for the scheduling process\n");
  fprintf(stderr, "   -f               Override any limits specified for the service\n");
  fprintf(stderr, "   -v               Show verbose debug output\n");
  fprintf(stderr, "   -h               Show this help message\n\n");
  fprintf(stderr, "'clusterd-schedule' is typically invoked by 'clusterd-exec' or other clusterd(7) tools.\n\n");
  fprintf(stderr, "Please report bugs to support@f-omega.com\n");
}

static int parse_node_count_and_max(const char *arg, int *count, int *max) {
  int err;

  *max = -1;

  err = sscanf(arg, "%d:%d", count, max);
  if ( err == 2 ) {
    if ( count < 0 || max < 0 ) {
      CLUSTERD_LOG(CLUSTERD_ERROR, "Node count and max must both be non-negative");
      return -1;
    } else
      return 0;
  } else if ( err == 1 ) {
    *max = -1;
    if ( count < 0 ) {
      CLUSTERD_LOG(CLUSTERD_ERROR, "Node count must be non-negative");
      return -1;
    } else
      return 0;
  } else {
    CLUSTERD_LOG(CLUSTERD_ERROR, "Node count must be given as COUNT or COUNT:MAX");
    return -1;
  }
}

int main(int argc, char *const *argv) {
  int c, err;
  clusterctl ctl;

  char nodebuf[64 * 1024];
  size_t nodebuflen;

  int collect_service_constraints = 1;
  int max_nodes = 10, max_to_examine = -1, timeout = 30, examined_count = 0;

  unsigned int i;

  setlocale(LC_ALL, "C");
  srandom(time(NULL));

  g_main_thread = pthread_self();

  g_clusterd_stats_cmd = getenv("CLUSTERD_STATS_COMMAND");
  if ( !g_clusterd_stats_cmd )
    g_clusterd_stats_cmd = "clusterd-stats";

  while ( (c = getopt(argc, argv, "l:g:N:t:vhf")) != -1 ) {
    switch ( c ) {
    case 'l':
      // Add this limit in to the limit set
      err = add_global_limit(optarg);
      if ( err < 0 ) return 1;
      break;

    case 'g':
      // Make sure the global resource parses
      err = add_global_resource_constraint(optarg);
      if ( err < 0 ) return 1;
      break;

    case 'N':
      err = parse_node_count_and_max(optarg, &max_nodes, &max_to_examine);
      if ( err < 0 ) {
	CLUSTERD_LOG(CLUSTERD_ERROR, "Invalid format for -N option");
	return 1;
      }
      break;

    case 't':
      err = sscanf(optarg, "%d", &timeout);
      if ( err != 1 ) {
	CLUSTERD_LOG(CLUSTERD_ERROR, "Invalid timeout: %s", optarg);
	return 1;
      }
      if ( timeout <= 0 )
	timeout = -1;
      break;

    case 'f':
      collect_service_constraints = 0;
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

  g_chosen_node_capacity = max_nodes;
  g_chosen_nodes = calloc(g_chosen_node_capacity, sizeof(*g_chosen_nodes));
  if ( !g_chosen_nodes ) {
    CLUSTERD_LOG(CLUSTERD_CRIT, "Could not allocate space for chosen nodes");
    return 1;
  }

  err = clusterctl_open(&ctl);
  if ( err < 0 ) {
    CLUSTERD_LOG(CLUSTERD_CRIT, "Could not contact cluster controller: %s", strerror(errno));
    return 1;
  }

  if ( timeout > 0 )
    start_timeout(timeout);

  // Now, we collect a list of all nodes matching our criteria from
  // the server. Then, we start checking each node.
  err = start_node_collection(&ctl);
  if ( err < 0 ) {
    CLUSTERD_LOG(CLUSTERD_ERROR, "Could not request nodes from cluster controller");
    return 1;
  }

  // Now, begin to process nodes as they come in, by reading from the controller
  for ( nodebuflen = sizeof(nodebuf);
        (err = clusterctl_read_output(&ctl, nodebuf, &nodebuflen)) == CLUSTERCTL_READ_LINE;
        nodebuflen = sizeof(nodebuf) ) {
    err = add_node(nodebuf, nodebuflen);
    if ( err < 0 ) {
      CLUSTERD_LOG(CLUSTERD_WARNING, "Could not add node information... skipping");
      CLUSTERD_LOG(CLUSTERD_DEBUG, "Node info was %.*s", (int)nodebuflen, nodebuf);
    }

    examined_count ++;
    if ( max_to_examine >= 0 && examined_count >= max_to_examine ) {
      CLUSTERD_LOG(CLUSTERD_DEBUG, "Examined maximum number of nodes... waiting");
      break;
    }
  }

  // Set the examination as complete
  mark_nodes_complete();

  switch ( err ) {
  case CLUSTERCTL_READ_ERROR:
    do {
      CLUSTERD_LOG(CLUSTERD_ERROR, "%.*s", (int)nodebuflen, nodebuf);
    } while ( nodebuflen = sizeof(nodebuf),
              (err = clusterctl_read_output(&ctl, nodebuf, &nodebuflen)) == CLUSTERCTL_READ_ERROR );
    break;

  case CLUSTERCTL_READ_DONE:
    // All nodes complete
    break;

  default:
    CLUSTERD_LOG(CLUSTERD_CRIT, "Invalid ending state for clusterctl_read_output: %d", err);
    return 1;
  }

  clusterctl_close(&ctl);

  // Now, wait until the alarm sounds, or we've finished examining all nodes
  while ( !g_alarm_sounded ) {
    if ( node_processing_complete() )
      break;

    err = pause();
    if ( errno != EINTR )
      CLUSTERD_LOG(CLUSTERD_CRIT, "pause() returned a non-standard error: %s", strerror(errno));

    CLUSTERD_LOG(CLUSTERD_DEBUG, "Woke up from pause()");
  }

  if ( pthread_mutex_lock(&g_chosen_node_mutex) != 0 ) {
    CLUSTERD_LOG(CLUSTERD_ERROR, "Could not lock chosen nodes mutex");
    return 1;
  }

  for ( i = 0; i < g_chosen_node_count; ++i ) {
    printf("%s\t%s\t%s\t%lf\n", g_chosen_nodes[i]->nodeid, g_chosen_nodes[i]->hostname,
           g_chosen_nodes[i]->ip, g_chosen_nodes[i]->score);
  }

  return 0;
}
