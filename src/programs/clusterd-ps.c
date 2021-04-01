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

#define CLUSTERD_COMPONENT "ps"
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

const char *list_processes_lua =
  "function list_processes(nsid)\n"
  "  procs = clusterd.list_processes(nsid, { resolve_names = true })\n"
  "  for _, proc in ipairs(procs) do\n"
  "    clusterd.output(proc.ps_id .. '\t' .. (proc.ps_svc_name or proc.ps_svc) .. '\t' .."
  "       (proc.ps_ns_name or proc.ps_ns) .. '\t' .. proc.ps_state .. '\t' .. (proc.ps_placement or ''))\n"
  "  end\n"
  "end\n"
  "\n"
  "if #params.namespace == 0 then\n"
  "  for _, ns in ipairs(clusterd.list_namespaces()) do\n"
  "    list_processes(ns.ns_id)\n"
  "  end\n"
  "else\n"
  "  ns_id = clusterd.resolve_namespace(params.namespace)\n"
  "  if ns_id == nil then\n"
  "    error('namespace ' .. params.namespace .. ' not found')\n"
  "  end\n"
  "  list_processes(ns_id)\n"
  "end\n";

static void usage() {
  fprintf(stderr, "clusterd-ps -- list clusterd processes\n");
  fprintf(stderr, "Usage: clusterd-ps -vhcA [-n NAMESPACE]\n\n");
  fprintf(stderr, "   -c           Do not return stale results\n");
  fprintf(stderr, "   -A           List processes from all namespaces\n");
  fprintf(stderr, "   -n NAMESPACE Only list processes from this namespace\n");
  fprintf(stderr, "   -v           Display verbose debugging output\n");
  fprintf(stderr, "   -h           Show this help message\n\n");
  fprintf(stderr, "Please report bugs to support@f-omega.com\n");
}

int main(int argc, char *const *argv) {
  clusterctl ctl;
  const char *namespace = NULL;
  int err, sts, c;
  clusterctl_tx_level txlvl = CLUSTERCTL_STALE_READS;

  setlocale(LC_ALL, "C");
  srandom(time(NULL));

  while ((c = getopt(argc, argv, "vhcAn:")) != -1) {
    switch (c) {
    case 'v':
      CLUSTERD_LOG_LEVEL = CLUSTERD_DEBUG;
      break;

    case 'h':
      usage();
      return 0;

    case 'c':
      txlvl = CLUSTERCTL_CONSISTENT_READS;
      break;

    case 'A':
      if ( namespace ) {
        CLUSTERD_LOG(CLUSTERD_ERROR, "-A cannot be given with -n");
        usage();
        return 1;
      }
      namespace = "";
      break;

    case 'n':
      if ( namespace ) {
        CLUSTERD_LOG(CLUSTERD_ERROR, "-n cannot be given with -A");
        usage();
        return 1;
      }
      namespace = optarg;
      break;

    default:
      usage();
      return 1;
    }
  }

  if ( namespace == NULL )
    namespace = "default";

  err = clusterctl_open(&ctl);
  if ( err < 0 ) {
    CLUSTERD_LOG(CLUSTERD_ERROR, "Could not open cluster: %s", strerror(errno));
    return 1;
  }

  err = clusterctl_simple(&ctl, txlvl, list_processes_lua,
                          "namespace", namespace,
                          NULL);
  if ( err < 0 ) {
    CLUSTERD_LOG(CLUSTERD_ERROR, "Could not issue list processes: %s", strerror(errno));
    return 1;
  }

  err = clusterctl_flush_output(&ctl, STDOUT_FILENO, STDERR_FILENO, &sts);
  if ( err < 0 ) {
    CLUSTERD_LOG(CLUSTERD_ERROR, "Could not get output: %s", strerror(errno));
    return 1;
  }

  if ( !sts ) {
    CLUSTERD_LOG(CLUSTERD_ERROR, "Process listing failed");
    return 1;
  }

  return 0;
}
