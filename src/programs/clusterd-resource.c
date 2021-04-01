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

#define CLUSTERD_COMPONENT "resource"
#include "clusterd/log.h"
#include "clusterd/common.h"
#include "libclusterctl.h"

#include <getopt.h>
#include <stdio.h>
#include <locale.h>
#include <time.h>
#include <stdlib.h>
#include <unistd.h>

int CLUSTERD_LOG_LEVEL = CLUSTERD_INFO;

enum output_format
  { HUMAN_READABLE = 0,
    JSON
  };

enum action
  { NO_ACTION,
    CREATE_RESOURCE,
    UPDATE_RESOURCE,
    DELETE_RESOURCE,
    ASSIGN_RESOURCE,
    UNASSIGN_RESOURCE,
    CLAIM_RESOURCE,
    RELEASE_RESOURCE,
  };

static void hex_format(char *out, const char *in, size_t inlen) {
  static const char digits[] = "0123456789ABCDEF";
  size_t i;

  for ( i = 0; i < inlen; ++i ) {
    int c = in[i];

    out[i * 2] = digits[(c >> 4) & 0xF];
    out[i * 2 + 1] = digits[c & 0xF];
  }
}

static ssize_t read_file_to_hex(int file, char *out, ssize_t outsz) {
  char buf[1024];
  ssize_t origsz = outsz;

  while ( outsz > 0 ) {
    ssize_t capacity = outsz / 2, bytesread;

    if ( capacity > sizeof(buf) )
      capacity = sizeof(buf);

    bytesread = read(file, buf, capacity);
    if ( bytesread < 0 )
      return -1;
    else if ( bytesread == 0 )
      return 0;

    hex_format(out, buf, bytesread);

    outsz -= bytesread * 2;
    out += bytesread * 2;
  }

  return origsz - outsz;
}

static int update_resource_claims(clusterctl *ctl, const char *nsid, const char *resnm,
                                  const char *process, enum action action) {
  static const char update_claims_lua[] =
    "clusterd.claim_resource(params.ns, params.name, tonumber(params.pid))\n";
  static const char release_claim_lua[] =
    "pid = nil\n"
    "if params.pid ~= nil then\n"
    "  pid = tonumber(params.process)\n"
    "  if pid == nil then\n"
    "    error('process id must be a number')\n"
    "  end\n"
    "end\n"
    "clusterd.release_claim(params.ns, params.name, pid)\n";

  int err;

  if ( action == RELEASE_RESOURCE ) {
    if ( process ) {
      err = clusterctl_call_simple(ctl, CLUSTERCTL_MAY_WRITE, release_claim_lua,
                                   NULL, 0, "ns", nsid, "name", resnm,
                                   "pid", process, NULL);
    } else {
      err = clusterctl_call_simple(ctl, CLUSTERCTL_MAY_WRITE, release_claim_lua,
                                   NULL, 0, "ns", nsid, "name", resnm, NULL);
    }
  } else {
    if ( !process ) {
      CLUSTERD_LOG(CLUSTERD_ERROR, "Process must be provided when claiming resource");
      return 1;
    }

    err = clusterctl_call_simple(ctl, CLUSTERCTL_MAY_WRITE, update_claims_lua,
                                 NULL, 0, "ns", nsid, "name", resnm, "pid", process,
                                 NULL);
  }

  if ( err < 0 ) {
    CLUSTERD_LOG(CLUSTERD_ERROR, "Could not change resource claims: %s", strerror(errno));
    return 1;
  }

  return 0;
}

static int update_resource_assignment(clusterctl *ctl, const char *nsid, const char *resnm,
                                      const char *node, enum action action, const char *rel) {
  static const char assign_resource_lua[] =
    "status, options = pcall(function() return json.decode(clusterd.dehex(params.hex_options)) end)\n"
    "if status then\n"
    "  clusterd.assign_global_resource(params.ns, params.name, params.node, options)\n"
    "else\n"
    "  error('bad input json: ' .. options)\n"
    "end\n";
  char hexinput[65536];
  ssize_t n;
  int err;

  if ( action == ASSIGN_RESOURCE ) {
    n = read_file_to_hex(STDIN_FILENO, hexinput, sizeof(hexinput));
    if ( n < 0 ) {
      CLUSTERD_LOG(CLUSTERD_ERROR, "Could not read input data: %s", strerror(errno));
      return 1;
    }

    if ( n >= sizeof(hexinput) ) {
      CLUSTERD_LOG(CLUSTERD_ERROR, "Not enough room in buffer");
      return 1;
    }
  } else {
    static const char unassign[] = "{\"unassign\": true, \"rel\": ";
    ssize_t offs = 0;

    if ( rel && strlen(rel) >= 100 ) {
      CLUSTERD_LOG(CLUSTERD_ERROR, "Relation must be less than 100 characters");
      return 1;
    }

    hex_format(hexinput, unassign, strlen(unassign));
    offs += strlen(unassign) * 2;

    if ( !rel ) {
      hex_format(hexinput + offs, "null}", 5);
    } else {
      hex_format(hexinput + offs, "\"", 1);
      offs += 2;

      // TODO escape lua string
      hex_format(hexinput + offs, rel, strlen(rel));
      offs += strlen(rel) * 2;

      hex_format(hexinput + offs, "\"}", 2);
    }
  }

  err = clusterctl_call_simple(ctl, CLUSTERCTL_MAY_WRITE, assign_resource_lua,
                               NULL, 0,
                               "hex_options", hexinput, "ns", nsid,
                               "name", resnm, "node", node,
                               NULL);
  if ( err < 0 ) {
    CLUSTERD_LOG(CLUSTERD_ERROR, "Could not create resource: %s", strerror(errno));
    return 1;
  }

  return 0;
}

static int create_or_update_resource(clusterctl *ctl, const char *nsid, const char *resnm,
                                     enum action action, enum output_format format) {
  static const char change_resource_lua[] =
    "status, options = pcall(function() return json.decode(clusterd.dehex(params.hex_options)) end)\n"
    "if status then\n"
    "  clusterd[params.action](params.ns, params.name, options)\n"
    "else\n"
    "  error('bad input json: ' .. options)\n"
    "end\n";


  char hexinput[65536];
  int err;
  ssize_t n;

  memset(hexinput, 0, sizeof(hexinput));

  n = read_file_to_hex(STDIN_FILENO, hexinput, sizeof(hexinput));
  if ( n < 0 ) {
    CLUSTERD_LOG(CLUSTERD_ERROR, "Could not read input data: %s", strerror(errno));
    return 1;
  }

  if ( n >= sizeof(hexinput) ) {
    CLUSTERD_LOG(CLUSTERD_ERROR, "Not enough room in buffer");
    return 1;
  }

  err = clusterctl_call_simple(ctl, CLUSTERCTL_MAY_WRITE, change_resource_lua,
                               NULL, 0,
                               "hex_options", hexinput,
                               "ns", nsid,
                               "name", resnm,
                               "action", action == CREATE_RESOURCE ? "new_global_resource" : "update_global_resource",
                               NULL);
  if ( err < 0 ) {
    CLUSTERD_LOG(CLUSTERD_ERROR, "Could not create resource: %s", strerror(errno));
    return 1;
  }

  return 0;
}

static int delete_resource(clusterctl *ctl, const char *nsid, const char *resnm) {
  static const char delete_resource_lua[] =
    "clusterd.delete_global_resource(params.ns, params.name)\n";
  int err;

  err = clusterctl_call_simple(ctl, CLUSTERCTL_MAY_WRITE,
                               delete_resource_lua,
                               NULL, 0,
                               "ns", nsid,
                               "name", resnm, NULL);
  if ( err < 0 ) {
    CLUSTERD_LOG(CLUSTERD_ERROR, "Could not delete resource: %s", strerror(errno));
    return 1;
  }

  return 0;
}

static int list_resources(clusterctl *ctl, const char *nsid,
                          enum output_format format,
                          clusterctl_tx_level txlvl) {
  static const char list_resources_lua[] =
    "function format_resource(r)\n"
    "  return r.name .. '\t' .. r.management_service .. '\t' ..\n"
    "    r.metadata .. '\t' .. r.type .. '\t' .. (r.description or '') ..'\t' ..\n"
    "    r.persistent .. '\t' .. r.available\n"
    "end\n"
    "\n"
    "resources = clusterd.list_global_resources({ namespace = params.ns })\n"
    "if params.format == 'json' then\n"
    "  for _, resource in ipairs(resources) do\n"
    "    clusterd.output(json.encode(resource))\n"
    "  end\n"
    "else\n"
    "  for _, resource in ipairs(resources) do\n"
    "    clusterd.output(format_resource(resource))\n"
    "  end\n"
    "end\n";

  int err, success = 0;

  err = clusterctl_simple(ctl, txlvl, list_resources_lua,
                          "ns", nsid,
                          "format", format == JSON ? "json" : "human",
                          NULL);
  if ( err < 0 ) {
    CLUSTERD_LOG(CLUSTERD_ERROR, "list_resources(): could not call script: %s", strerror(errno));
    return 1;
  }

  err = clusterctl_flush_output(ctl, STDOUT_FILENO, STDERR_FILENO, &success);
  if ( err < 0 ) {
    CLUSTERD_LOG(CLUSTERD_ERROR, "could not flush output: %s", strerror(errno));
    return 1;
  }

  if ( success ) return 0;
  else return 1;
}

#define PRINT_FIELD(what)                                       \
  do {                                                          \
    char line[2048];                                            \
    size_t linesz = sizeof(line);                               \
    err = clusterctl_read_output(ctl, line, &linesz);           \
    switch ( err ) {                                            \
    case CLUSTERCTL_READ_DONE:                                  \
      CLUSTERD_LOG(CLUSTERD_ERROR, "Premature end of output");  \
      return 1;                                                 \
                                                                \
    case CLUSTERCTL_READ_LINE:                                  \
      if ( what ) {                                             \
        printf("%s\t%.*s", (char *) what, (int)linesz, line);   \
      } else {                                                  \
        printf("%.*s", (int)linesz, line);                      \
      }                                                         \
      break;                                                    \
                                                                \
    case CLUSTERCTL_READ_ERROR:                                         \
      fprintf(stderr, "%.*s", (int)linesz, line);                       \
      clusterctl_flush_output(ctl, STDOUT_FILENO, STDERR_FILENO, &err); \
      return 1;                                                         \
                                                                        \
    default:                                                            \
      if ( what ) {                                                     \
        CLUSTERD_LOG(CLUSTERD_ERROR, "Error while reading field %s: %s", \
                     (char *) what, strerror(errno));                   \
      } else {                                                          \
        CLUSTERD_LOG(CLUSTERD_ERROR, "Error while reading row: %s", strerror(errno)); \
      }                                                                 \
      return 1;                                                         \
    }                                                                   \
  } while (0)

#define PRINT_ROW() PRINT_FIELD(NULL)

static int has_more(clusterctl *ctl) {
  char line[2048];
  size_t linesz = sizeof(line);
  int err;

  err = clusterctl_read_output(ctl, line, &linesz);
  switch (err) {
  case CLUSTERCTL_READ_DONE:
    return 0;

  case CLUSTERCTL_READ_LINE:
    if ( linesz == 5 && memcmp("MORE\n", line, linesz) == 0 )
      return 1;
    else if ( linesz == 4 && memcmp("END\n", line, linesz) == 0 )
      return 0;
    else {
      CLUSTERD_LOG(CLUSTERD_ERROR, "Protocol violation");
      exit(1);
    }

  case CLUSTERCTL_READ_ERROR:
    fprintf(stderr, "%.*s", (int)linesz, line);
    clusterctl_flush_output(ctl, STDOUT_FILENO, STDERR_FILENO, &err);
    exit(1);

  default:
    CLUSTERD_LOG(CLUSTERD_ERROR, "Error while reading listing: %s", strerror(errno));
    exit(1);
  }
}

static int get_resource(clusterctl *ctl,
                        const char *nsid, const char *name,
                        enum output_format format,
                        clusterctl_tx_level txlvl) {
  static const char get_resource_lua[] =
    "resource = clusterd.get_global_resource(params.ns, params.name)\n"
    "if resource == nil then\n"
    "  error('resource ' .. params.name .. ' in namespace ' .. params.ns .. ' not found')\n"
    "end\n"
    "resource.assignments = clusterd.get_global_resource_assignments(params.ns, params.name)\n"
    "if params.format == \"json\" then\n"
    "  clusterd.output(json.encode(resource))\n"
    "else\n"
    "  clusterd.output(resource.name)\n"
    "  clusterd.output(resource.management_service)\n"
    "  clusterd.output(resource.type)\n"
    "  clusterd.output(resource.persistent)\n"
    "  clusterd.output(resource.available)\n"
    "  clusterd.output(resource.description or '')\n"

    "  for _, assignment in ipairs(resource.assignments) do\n"
    "    clusterd.output(\"MORE\")\n"
    "    clusterd.output(table.concat({assignment.node, assignment.rel, assignment.enforce_affinity, assignment.description}, '\\t'))\n"
    "  end\n"
    "  clusterd.output(\"END\")\n"

    "end\n";
  int err;

  err = clusterctl_simple(ctl, txlvl,
                          get_resource_lua,
                          "ns", nsid,
                          "name", name,
                          "format", format == JSON ? "json" : "human",
                          NULL);
  if ( err < 0 ) {
    CLUSTERD_LOG(CLUSTERD_ERROR, "get_resource(): could not call script: %s", strerror(errno));
    return 1;
  }

  if ( format == JSON ) {
    int success = 0;

    // JSON is returned immediately from the script, just return that
    err = clusterctl_flush_output(ctl, STDOUT_FILENO, STDERR_FILENO,
                                  &success);
    if ( err < 0 ) {
      CLUSTERD_LOG(CLUSTERD_ERROR, "could not flush output: %s", strerror(errno));
      return 1;
    }

    if ( !success ) return 1;
    else return 0;
  } else {
    // Human readable format
    PRINT_FIELD("NAME");
    PRINT_FIELD("SERVICE");
    PRINT_FIELD("TYPE");
    PRINT_FIELD("PERSISTENT");
    PRINT_FIELD("AVAILABLE");
    PRINT_FIELD("DESCRIPTION");

    printf("\nASSIGNMENTS\n");
    while ( has_more(ctl) ) {
      PRINT_ROW();
    }

    // while ( HAS_MORE ) {
    //   PRINT_ROW();
    // }
    return 0;
  }
}

static void usage() {
  fprintf(stderr, "clusterd-resource -- manage global resources in a clusterd cluster\n");
  fprintf(stderr, "Usage: clusterd-resource -vhcJ\n");
  fprintf(stderr, "          [-A | -D | -U | -a <node> | -u <node> |\n");
  fprintf(stderr, "           -C <pid> | -R [<pid>] ] [-n <namespace>] [-r <rel>]\n");
  fprintf(stderr, "          <resource-name>\n");
  fprintf(stderr, "   -A              Create a resource with the given name\n");
  fprintf(stderr, "   -U              Update a resource with the given name\n");
  fprintf(stderr, "   -D              Delete a resource with the given name\n");
  fprintf(stderr, "   -a <node>       Assign a resource to the given node\n");
  fprintf(stderr, "   -u <node>       Unassign a resource from the given node\n");
  fprintf(stderr, "   -r <rel>        When unassigning, only unassign nodes with the\n");
  fprintf(stderr, "                   given relation\n");
  fprintf(stderr, "   -C <pid>        Claim a resource by the given process id\n");
  fprintf(stderr, "   -R [<pid>]      Release a resource. If a pid is given, the resource\n");
  fprintf(stderr, "                   is released only if owned by the given pid\n");
  fprintf(stderr, "   -n <namespace>  Only affect resources in the given namespace\n");
  fprintf(stderr, "   -c              Do not return stale output\n");
  fprintf(stderr, "   -J              Display results in JSON\n");
  fprintf(stderr, "   -h              Show this help menu\n");
  fprintf(stderr, "   -v              Show verbose debugging output\n\n");
  fprintf(stderr, "Without any flags, clusterd-resource will list all resources in the\n");
  fprintf(stderr, "given namespace (or the default namespace if none given). If a resource\n");
  fprintf(stderr, "name is provided, then detailed information on that resource will be\n");
  fprintf(stderr, "fetched and displayed.\n\n");
  fprintf(stderr, "Please report bugs to support@f-omega.com\n");
}

int main(int argc, char *const *argv) {
  clusterctl ctl;
  int c, err;
  enum output_format format = HUMAN_READABLE;
  enum action action = NO_ACTION;
  const char *nsid = "default", *node = NULL, *process = NULL, *resource = NULL, *rel = NULL;
  clusterctl_tx_level txlvl = CLUSTERCTL_STALE_READS;

  setlocale(LC_ALL, "C");
  srandom(time(NULL));

  while ((c = getopt(argc, argv, "-vhcJn:AUDa:u:C:r:R::")) != -1) {
    switch (c) {
    case 1:
      if ( resource ) {
        fprintf(stderr, "Only one resource may be specified\n");
        usage();
        return 1;
      }
      resource = optarg;
      break;

    case 'r':
      rel = optarg;
      break;

    case 'A':
    case 'U':
    case 'D':
    case 'a':
    case 'u':
    case 'C':
    case 'R':
      if ( action != NO_ACTION ) {
        fprintf(stderr, "Only one of -A, -U, -D, -a, -u, -C, or -R can be specified\n");
        usage();
        return 1;
      }

      switch (c) {
      case 'A':
        action = CREATE_RESOURCE;
        break;
      case 'U':
        action = UPDATE_RESOURCE;
        break;
      case 'D':
        action = DELETE_RESOURCE;
        break;

      case 'a':
        action = ASSIGN_RESOURCE;
        node = optarg;
        break;
      case 'u':
        action = UNASSIGN_RESOURCE;
        node = optarg;
        break;

      case 'C':
        action = CLAIM_RESOURCE;
        process = optarg;
        break;
      case 'R':
        action = RELEASE_RESOURCE;
        process = optarg;
        break;
      }

      break;

    case 'J':
      format = JSON;
      break;

    case 'n':
      nsid = optarg;
      break;

    case 'c':
      txlvl = CLUSTERCTL_CONSISTENT_READS;
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

  if ( rel && action != UNASSIGN_RESOURCE ) {
    CLUSTERD_LOG(CLUSTERD_ERROR, "rel should only be specified when unassigning");
  }

  err = clusterctl_open(&ctl);
  if ( err < 0 ) {
    CLUSTERD_LOG(CLUSTERD_ERROR, "Could not open cluster controller: %s", strerror(errno));
    return 1;
  }

  switch (action) {
  case UPDATE_RESOURCE:
  case CREATE_RESOURCE:
    return create_or_update_resource(&ctl, nsid, resource, action, format);

  case DELETE_RESOURCE:
    return delete_resource(&ctl, nsid, resource);

  case ASSIGN_RESOURCE:
  case UNASSIGN_RESOURCE:
    return update_resource_assignment(&ctl, nsid, resource, node, action, rel);

  case CLAIM_RESOURCE:
  case RELEASE_RESOURCE:
    return update_resource_claims(&ctl, nsid, resource, process, action);

  case NO_ACTION:
    if ( resource )
      return get_resource(&ctl, nsid, resource, format, txlvl);
    else
      return list_resources(&ctl, nsid, format, txlvl);

  default:
    CLUSTERD_LOG(CLUSTERD_CRIT, "Unimplemented action");
    return 1;
  }
}
