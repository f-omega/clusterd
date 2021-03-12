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

#define CLUSTERD_COMPONENT "clusterd-service"
#include "clusterd/log.h"
#include "libclusterctl.h"

#include <errno.h>
#include <string.h>
#include <unistd.h>
#include <getopt.h>
#include <stdlib.h>
#include <locale.h>
#include <time.h>
#include <ctype.h>

#define FORMAT_SERVICE_DEF                                             \
  "function format_service(svc)\n"                                     \
  "  return (svc.s_id .. '\t' .. svc.s_label .. '\t' .. svc.s_path)\n" \
  "end\n"                                                              \

static const char *list_one_lua =
  FORMAT_SERVICE_DEF
  "svc = clusterd.get_service(params.namespace, params.nameorid)\n"
  "clusterd.output(format_service(svc))\n";
static const char *add_one_lua =
  "options = {}\n"
  "if params.label ~= nil and #params.label > 0 then\n"
  "  options.label = params.label\n"
  "  options.id = params.nameorid\n"
  "else\n"
  "  options.id = params.nameorid\n"
  "end\n"
  "if #params.path > 0 then\n"
  "  options.path = params.path\n"
  "end\n"
  "if params.action == \"update\" then\n"
  "  options.update_only = true\n"
  "elseif params.action == \"create\" then\n"
  "  options.create_only = true\n"
  "end\n"
  "s_id = clusterd.update_service(params.namespace, options)\n"
  "clusterd.output(s_id)";
static const char *delete_one_lua =
  "clusterd.delete_service(params.namespace, params.path)\n";
static const char *list_all_lua =
  FORMAT_SERVICE_DEF
  "svcs = clusterd.list_services(params.namespace)\n"
  "for _, row in ipairs(svcs) do\n"
  "    clusterd.output(format_service(row))\n"
  "end\n";

int CLUSTERD_LOG_LEVEL = CLUSTERD_INFO;

enum CLUSTERSVC_ACTION
  { CLUSTERSVC_LIST = 1,
    CLUSTERSVC_ADD,
    CLUSTERSVC_DELETE };

#define OPEN_CLUSTER                                                    \
  if ( performed == 0 ) {                                               \
    err = clusterctl_open(&ctl);                                        \
    if ( err < 0 ) {                                                    \
      CLUSTERD_LOG(CLUSTERD_CRIT, "Could not connect to controller: %s", strerror(errno)); \
      return 1;                                                         \
    }                                                                   \
  }

static void usage() {
  fprintf(stderr, "clusterd-service -- clusterd service manager\n");
  fprintf(stderr, "Usage: clusterd-service -vch -n NAMESPACE (-D | -A | -U | -C) -p PATH\n");
  fprintf(stderr, "         [-U] -L [LABELORID...]\n\n");
  fprintf(stderr, "   -D    Delete the service with the given label or ID\n");
  fprintf(stderr, "   -A    Create or modify a service with the given\n");
  fprintf(stderr, "         label or ID. The -p option must be specified\n");
  fprintf(stderr, "   -U    Like -A, but mandates that the action only update\n");
  fprintf(stderr, "         an existing service\n");
  fprintf(stderr, "   -C    Like -A, but mandates that the action only create\n");
  fprintf(stderr, "         a new service\n");
  fprintf(stderr, "   -L    Set the label for the next namespace to be modified\n");
  fprintf(stderr, "   -n    Use this namespace to resolve the service. Defaults\n");
  fprintf(stderr, "         to namespace 0 (default)\n");
  fprintf(stderr, "   -c    Mandate that results read are not out-of-date. Does\n");
  fprintf(stderr, "         nothing for requests that modify controller state.\n");
  fprintf(stderr, "   -p    Set the service directory path\n");
  fprintf(stderr, "   -h    Show this help menu\n");
  fprintf(stderr, "   -v    Show verbose debugging output\n\n");
  fprintf(stderr, "Without any flags, clusterd-service will list services with the\n");
  fprintf(stderr, "given label or ID in the given namespace. If no names are given,\n");
  fprintf(stderr, "all services will be listed.\n\n");
  fprintf(stderr, "Please report bugs to support@f-omega.com\n");
}

int main(int argc, char *const *argv) {
  int c, err;
  int action = CLUSTERSVC_LIST, performed = 0, success = 0, error = 0, was_success = 0,
    create_only = 0, update_only = 0;
  clusterctl ctl;
  const char *action_name, *nsid = "default", *path = NULL, *label = NULL;
  clusterctl_tx_level txlvl = CLUSTERCTL_STALE_READS;

  setlocale(LC_ALL, "C");
  srandom(time(NULL));

  while ( (c = getopt(argc, argv, "-ADUCn:cp:L:hv")) != -1 ) {
    switch ( c ) {
    case 1:
      switch ( action ) {
      case CLUSTERSVC_LIST:
        OPEN_CLUSTER;
        action_name = "get";
        err = clusterctl_simple(&ctl, txlvl, list_one_lua,
                                "nameorid", optarg,
                                "namespace", nsid, NULL);
        break;

      case CLUSTERSVC_ADD:
        OPEN_CLUSTER;
        action_name = "add";

        err = clusterctl_simple(&ctl, CLUSTERCTL_MAY_WRITE, add_one_lua,
                                "nameorid", optarg,
                                "path", path ? path : "",
                                "label", label ? label : (isdigit(optarg[0]) ? "" : optarg),
                                "action", (update_only ? "update"  :
                                           (create_only ? "create" :
                                            "any")),
                                "namespace", nsid, NULL);
        label = NULL;
        break;

      case CLUSTERSVC_DELETE:
        OPEN_CLUSTER;
        action_name = "delete";
        err = clusterctl_simple(&ctl, CLUSTERCTL_MAY_WRITE, delete_one_lua,
                                "nameorid", optarg,
                                "namespace", nsid, NULL);
      break;

      default:
        CLUSTERD_LOG(CLUSTERD_CRIT, "Unknown action");
        return 1;
      }

      performed++;
      was_success = 0;
      if ( err < 0 ) {
        CLUSTERD_LOG(CLUSTERD_ERROR, "Could not %s service %s in namespace %s: %s",
                     action_name, optarg, nsid, strerror(errno));
      } else {
        err = clusterctl_flush_output(&ctl, STDOUT_FILENO, STDERR_FILENO, &was_success);
        if ( err < 0 ) goto output_error;
      }

      if ( was_success )
        success ++;
      else
        error ++;
      break;

    case 'p':
      path = optarg;
      break;

    case 'L':
      if ( label ) {
        CLUSTERD_LOG(CLUSTERD_ERROR, "Only one label should be provided for each ID being modified\n");
        usage();
        return 1;
      }
      label = optarg;
      break;

    case 'A':
    case 'D':
    case 'U':
    case 'C':
      if ( action == CLUSTERSVC_ADD ||
           action == CLUSTERSVC_DELETE ) {
        CLUSTERD_LOG(CLUSTERD_ERROR, "Only one of -A, -U, -C or -D should be given");
        usage();
        return 1;
      }

    case 'n':
    case 'c':
    case 'v':
      if ( performed ) {
        CLUSTERD_LOG(CLUSTERD_ERROR, "-A, -U, -C -D, -c, -v and -n must be given before any actions\n");
        usage();
        return 1;
      }

      if ( c == 'A' || c == 'U' || c == 'C' ) {
        action = CLUSTERSVC_ADD;
        action_name = "add";
        create_only = c == 'C';
        update_only = c == 'U';
        txlvl = CLUSTERCTL_MAY_WRITE;
      } else if ( c == 'D' ) {
        action = CLUSTERSVC_DELETE;
        action_name = "delete";
        txlvl = CLUSTERCTL_MAY_WRITE;
      } else if ( c == 'n' ) {
        nsid = optarg;
      } else if ( c == 'c' ) {
        txlvl = CLUSTERCTL_MAY_WRITE;
      } else if ( c == 'v' ) {
        CLUSTERD_LOG_LEVEL = CLUSTERD_DEBUG;
      }
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

  if ( error > 0 ) return 2;
  else if ( performed == 0 ) {
    switch ( action ) {
    case CLUSTERSVC_LIST:
      OPEN_CLUSTER;
      action_name = "list";
      err = clusterctl_simple(&ctl, txlvl, list_all_lua, "namespace", nsid, NULL);
      break;

    case CLUSTERSVC_ADD:
      if ( !path ) {
        CLUSTERD_LOG(CLUSTERD_ERROR, "Could not add service in namespace %s: no path given",
                     nsid);
        usage();
        return 1;
      }
      OPEN_CLUSTER;
      action_name = "add";
      err = clusterctl_simple(&ctl, CLUSTERCTL_MAY_WRITE, add_one_lua, "namespace", nsid, "path", path, NULL);
      break;

    case CLUSTERSVC_DELETE:
      CLUSTERD_LOG(CLUSTERD_ERROR, "-D must be provided with at least one service to delete");
      usage();
      return 1;

    default:
      CLUSTERD_LOG(CLUSTERD_CRIT, "Unknown action");
      return 1;
    }

    was_success = 0;
    if ( err < 0 ) {
      CLUSTERD_LOG(CLUSTERD_ERROR, "Could not %s service(s) in namespace %s: %s",
                   action_name, nsid, strerror(errno));
    } else {
      err = clusterctl_flush_output(&ctl, STDOUT_FILENO, STDERR_FILENO, &was_success);
      if ( err < 0 ) goto output_error;

      if ( was_success )
        return 0;
      else
        return 2;
    }
  } else
    return 0;

 output_error:
  CLUSTERD_LOG(CLUSTERD_CRIT, "Could not read command output: %s", strerror(errno));
  return 3;
}
