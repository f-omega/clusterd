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

#define CLUSTERD_COMPONENT "clusterd-ns"
#include "clusterd/log.h"
#include "libclusterctl.h"

#include <errno.h>
#include <string.h>
#include <unistd.h>
#include <getopt.h>
#include <locale.h>
#include <time.h>
#include <stdlib.h>

#define FORMAT_NAMESPACE_DEF                                    \
  "function format_namespace(row)\n"                            \
  "  return (row.ns_id .. \"\\t\" .. (row.ns_label or ''))\n"  \
  "end\n"

static const char *list_one_lua =
  FORMAT_NAMESPACE_DEF
  "ns = clusterd.get_namespace(params.nameorid)\n"
  "clusterd.output(format_namespace(ns))\n";
static const char *add_one_lua =
  "ns_id = clusterd.add_namespace(params.name)\n"
  "clusterd.output(ns_id)\n";
static const char *delete_one_lua =
  "clusterd.delete_namespace(params.nameorid)\n";
static const char *list_all_lua =
  FORMAT_NAMESPACE_DEF
  "ns = clusterd.list_namespaces()\n"
  "for _, row in ipairs(ns) do\n"
  "    clusterd.output(format_namespace(row))\n"
  "end\n";

int CLUSTERD_LOG_LEVEL = CLUSTERD_INFO;

enum CLUSTERNS_ACTION
  { CLUSTERNS_LIST = 1,
    CLUSTERNS_ADD,
    CLUSTERNS_DELETE };

#define OPEN_CLUSTER                                                    \
  if ( performed == 0 ) {                                               \
    err = clusterctl_open(&ctl);                                        \
    if ( err < 0 ) {                                                    \
      CLUSTERD_LOG(CLUSTERD_CRIT, "Could not connect to controller: %s", strerror(errno)); \
      return 1;                                                         \
    }                                                                   \
  }

static void usage() {
  fprintf(stderr, "clusterd-ns -- clusterd namespace manager\n");
  fprintf(stderr, "Usage: clusterd-ns -vch (-D | -A) [NAMEORID]...\n\n");
  fprintf(stderr, "   -D    Delete the given namespaces\n");
  fprintf(stderr, "   -A    Create namespaces with the given names,\n");
  fprintf(stderr, "         or, if no names given, an unnamed one\n");
  fprintf(stderr, "   -c    Mandate that results read are not out-of-date\n");
  fprintf(stderr, "   -h    Show this help menu\n");
  fprintf(stderr, "   -v    Show verbose debugging output\n\n");
  fprintf(stderr, "Without any flags, clusterd-ns will list the given namespaces,\n");
  fprintf(stderr, "or, if no names given, all namespaces in the cluster.\n\n");
  fprintf(stderr, "Please report bugs to support@f-omega.com\n");
}

int main(int argc, char *const *argv) {
  int c, err;
  int action = CLUSTERNS_LIST, performed = 0, success = 0, error = 0, was_success = 0;
  clusterctl_tx_level txlvl = CLUSTERCTL_STALE_READS;
  clusterctl ctl;

  const char *action_name;

  setlocale(LC_ALL, "C");
  srandom(time(NULL));

  while ( (c = getopt(argc, argv, "-ADvhc")) != -1 ) {
    switch ( c ) {
    case 1:
      // Perform the action on this namespace
      switch ( action ) {
      case CLUSTERNS_LIST:
        OPEN_CLUSTER;
        action_name = "get";
        err = clusterctl_simple(&ctl, txlvl, list_one_lua,
                                "nameorid", optarg, NULL);
        break;

      case CLUSTERNS_ADD:
        OPEN_CLUSTER;
        action_name = "add";
        err = clusterctl_simple(&ctl, CLUSTERCTL_MAY_WRITE, add_one_lua,
                                "name", optarg, NULL);
        break;

      case CLUSTERNS_DELETE:
        OPEN_CLUSTER;
        action_name = "delete";
        err = clusterctl_simple(&ctl, CLUSTERCTL_MAY_WRITE, delete_one_lua,
                                "nameorid", optarg, NULL);
        break;

      default:
        CLUSTERD_LOG(CLUSTERD_CRIT, "Unknown action");
        return 1;
      }

      performed++;
      was_success = 0;

      if ( err < 0 ) {
        CLUSTERD_LOG(CLUSTERD_ERROR, "Could not %s namespace %s: %s", action_name, optarg, strerror(errno));
      } else {
        err = clusterctl_flush_output(&ctl, STDOUT_FILENO, STDERR_FILENO, &was_success);
        if ( err < 0 ) goto output_error;
      }

      if ( was_success )
        success ++;
      else
        error ++;
      break;

    case 'A':
    case 'D':
      if ( performed > 0 ) {
        CLUSTERD_LOG(CLUSTERD_ERROR, "-A or -D must come before any namespaces");
        usage();
        return 1;
      }

      if ( action == CLUSTERNS_ADD ||
           action == CLUSTERNS_DELETE ) {
        CLUSTERD_LOG(CLUSTERD_ERROR, "Only one of -A or -D should be given");
        usage();
        return 1;
      }

      txlvl = CLUSTERCTL_MAY_WRITE;
      if ( c == 'A' )
        action = CLUSTERNS_ADD;
      else
        action = CLUSTERNS_DELETE;
      break;

    case 'c':
      if ( performed > 0 ) {
        CLUSTERD_LOG(CLUSTERD_ERROR, "-c must come before any actions");
        return 1;
      }

      txlvl = CLUSTERCTL_CONSISTENT_READS;
      break;

    case 'v':
      if ( performed > 0 ) {
        CLUSTERD_LOG(CLUSTERD_ERROR, "-v must come before any namespaces");
        usage();
        return 1;
      }

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

  if ( error > 0 ) return 2;
  else if ( performed == 0 ) {
    switch ( action ) {
    case CLUSTERNS_LIST:
      OPEN_CLUSTER;
      action_name = "list";
      err = clusterctl_simple(&ctl, txlvl, list_all_lua, NULL);
      break;

    case CLUSTERNS_ADD:
      OPEN_CLUSTER;
      action_name = "add";
      err = clusterctl_simple(&ctl, CLUSTERCTL_MAY_WRITE, add_one_lua, NULL);
      break;

    case CLUSTERNS_DELETE:
      CLUSTERD_LOG(CLUSTERD_ERROR, "-D must be provided with at least one namespace to delete");
      usage();
      return 1;

    default:
      CLUSTERD_LOG(CLUSTERD_CRIT, "Unknown action");
      return 1;
    }

    was_success = 0;
    if ( err < 0 ) {
      CLUSTERD_LOG(CLUSTERD_ERROR, "Could not %s namespace(s): %s", action_name, strerror(errno));
    } else {
      err = clusterctl_flush_output(&ctl, STDOUT_FILENO, STDERR_FILENO, &was_success);
      if ( err < 0 ) goto output_error;
    }

    if ( was_success )
      return 0;
    else
      return 2;
  } else
    return 0;

 output_error:
  CLUSTERD_LOG(CLUSTERD_CRIT, "Could not read command output: %s", strerror(errno));
  return 3;
}
