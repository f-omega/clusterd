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

#define CLUSTERD_COMPONENT "kill"
#include "clusterd/log.h"
#include "clusterd/common.h"
#include "libclusterctl.h"

#include <string.h>
#include <stdlib.h>
#include <errno.h>
#include <getopt.h>
#include <stdio.h>
#include <fcntl.h>
#include <unistd.h>
#include <locale.h>
#include <time.h>
#include <sys/wait.h>

#define MAX_PENDING_PROCS 32

int CLUSTERD_LOG_LEVEL = CLUSTERD_INFO;

struct proc_info {
  clusterd_namespace_t nsid;
  clusterd_pid_t pid;
  char *current_host;
};

static void do_kill(struct proc_info *procs, int pids_waiting, int force, char signal) {
  
}

static void usage() {
  fprintf(stderr, "clusterd-kill - send signal to a clusterd process\n");
  fprintf(stderr, "Usage: clusterd-kill -vhf [-n NAMESPACE] [-s SIGNAL] PID...\n\n");
  fprintf(stderr, "   -n NAMESPACE  Interpret subsequent PIDs as belonging to this namespace\n");
  fprintf(stderr, "   -s SIGNAL     UNIX signal name or number to send or clusterd\n");
  fprintf(stderr, "                 signal character\n");
  fprintf(stderr, "   -f            If signal is SIGTERM, SIGKILL, or SIGQUIT, then\n");
  fprintf(stderr, "                 force kill the process by removing it from the\n");
  fprintf(stderr, "                 process table. Otherwise, has no effect\n");
  fprintf(stderr, "   -v            Display verbose debug output\n");
  fprintf(stderr, "   -h            Show this help menu\n");
  fprintf(stderr, "   PID           The PIDs to kill\n\n");
  fprintf(stderr, "Please report bugs to support@f-omega.com\n");
}

int main(int argc, char *const *argv) {
  int c, err, pids_signalled = 0, pids_waiting = 0, force = 0, has_ns = 0;
  char signal = 't'; // SIGTERM by default
  clusterd_namespace_t nsid;
  struct proc_info processes[MAX_PENDING_PROCS];


  // We collect all process details then ssh to the servers in parallel
  while ( (c = getopt(argc, argv, "-n:s:fvh")) != -1 ) {
    switch ( c ) {
    case 1:
      if ( !has_ns ) {
        err = get_namespace(&ctl, "default", &nsid);
        if ( err < 0 ) {
          CLUSTERD_LOG(CLUSTERD_ERROR, "Could not lookup default namespace: %s", strerror(errno));
          return 1;
        }

        has_ns = 1;
      }

      err = add_pid(&pids_signalled, &pids_waiting, processes, MAX_PENDING_PROCS,
                    nsid, new_pid);
      if ( err < 0 ) {
        CLUSTERD_LOG(CLUSTERD_WARNING, "Could not add pid %s: %s", optarg, strerror(errno));
      }

      if ( pids_waiting == MAX_PENDING_PROCS ) {
        do_kill(processes, pids_waiting, force, signal);
        pids_signalled += pids_waiting;
        pids_waiting = 0;
      }

      break;

    case 'n':
      err = get_namespace(&ctl, optarg, &nsid);
      if ( err < 0 ) {
        CLUSTERD_LOG(CLUSTERD_ERROR, "Could not lookup namespace %s: %s", optarg, strerror(errno));
        return 1;
      }

      has_ns = 1;
      break;

    case 's':
      if ( pids_signalled > 0 || pids_waiting > 0 ) {
        CLUSTERD_LOG(CLUSTERD_CRIT, "Must specify signal before process. Not killing anymore");
        return 1;
      } else {
        int new_signal = parse_signal(optarg);
        if ( new_signal < 0 ) {
          CLUSTERD_LOG(CLUSTERD_ERROR, "Invalid signal spec: %s: %s", optarg, strerror(errno));
          return 1;
        }

        signal = new_signal;
      }
      break;

    case 'f':
      force = 1; // This sends the signal and then forces the process down
      break;

    case 'h':
      usage();
      return 0;

    case 'v':
      if ( pids_signalled > 0 || pids_waiting > 0 ) {
        CLUSTERD_LOG(CLUSTERD_WARNING, "-v should be provided before any PIDs... Ignoring");
      }

      CLUSTERD_LOG_LEVEL = CLUSTERD_DEBUG;
      break;

    default:
      usage();
      return 1;
    }
  }

  if ( pids_waiting > 0 )
    do_kill(processes, pids_waiting, force, signal);

  return 0;
}
