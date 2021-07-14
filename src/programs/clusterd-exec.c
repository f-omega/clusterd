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

#define CLUSTERD_COMPONENT "clusterd-exec"
#include "clusterd/log.h"
#include "clusterd/common.h"
#include "libclusterctl.h"

#include <string.h>
#include <stdlib.h>
#include <errno.h>
#include <getopt.h>
#include <math.h>
#include <stdio.h>
#include <fcntl.h>
#include <unistd.h>
#include <locale.h>
#include <time.h>
#include <sys/wait.h>

#define MAX_ARGS 128
#define MIN_MONITORS 3 // Minimum number of monitors for high availability

int CLUSTERD_LOG_LEVEL = CLUSTERD_INFO;

typedef struct clusterd_monitor {
  struct clusterd_monitor *next;
  const char *monitor_spec;
} clusterd_monitor;

typedef enum
  { CLUSTERD_LOCAL_AVAILABILITY = 0,
    CLUSTERD_HIGH_AVAILABILITY
  } clusterd_exec_availability;

typedef enum
  { CLUSTERD_EXEC_WAIT_INVALID = -1,
    CLUSTERD_EXEC_NO_WAIT = 0,
    CLUSTERD_EXEC_WAIT_UNTIL_STARTED,
    CLUSTERD_EXEC_WAIT_UNTIL_READY
  } clusterd_exec_wait;

static const char *create_process_lua =
  "n_id = clusterd.resolve_node(params.pinned)\n"
  "if n_id == nil then\n"
  "  error('node ' .. params.pinned .. ' does not exist')\n"
  "end\n"
  "node = clusterd.get_node(n_id)\n"
  "if node == nil then\n"
  "  error('could not find node')\n"
  "end\n"
  "clusterd.output(node.hostname)\n"

  "svc = clusterd.get_service(params.namespace, params.service)\n"
  "if svc == nil then\n"
  "  error('service ' .. params.service .. ' in namespace ' .. params.namespace .. ' not found')\n"
  "end\n"
  "pid = clusterd.new_process(params.namespace, svc.s_id,\n"
  "                           { placement = node.id })\n"
  "clusterd.output(pid)\n";

static const char *kill_process_lua =
  "clusterd.delete_process(params.namespace, tonumber(params.pid))\n";

clusterd_monitor *g_monitors = NULL;
unsigned long g_monitors_desired = MIN_MONITORS;

static int add_monitor(const char *monitor);

static void usage() {
  fprintf(stderr, "clusterd-exec -- schedule and execute a clusterd process\n");
  fprintf(stderr, "Usage: clusterd-exec -vhHiI [-n NAMESPACE] [-N RETRIES] [-l RESOURCE=REQUEST...]\n");
  fprintf(stderr, "         [-w CONDITION] SERVICE [args...]\n\n");
  fprintf(stderr, "   -L            Run the process without high-availability\n");
  fprintf(stderr, "   -H NUMBER     Run in high availability, with optional number of desired monitors\n");
  fprintf(stderr, "   -n NAMESPACE  Use the given namespace for service lookup and deployment\n");
  fprintf(stderr, "   -N RETRIES    If the process dies, retry it this many times in a row\n");
  fprintf(stderr, "                 before failing permanently\n");
  fprintf(stderr, "   -l RESOURCE=REQUEST\n");
  fprintf(stderr, "                 Request additional resources, overriding requirements from\n");
  fprintf(stderr, "                 the controller\n");
  fprintf(stderr, "   -w CONDITION  Wait for the service to reach some state before exiting\n");
  fprintf(stderr, "   -m MONITOR    Use the given node (specified by hostname, IP, or node ID) as a monitor\n");
  fprintf(stderr, "   -d            Run the host process in debug mode (logs output to a logging directory)\n");
  fprintf(stderr, "   -i            Redirect the stdout of the process onto the current terminal\n");
  fprintf(stderr, "   -I            Redirect both stdin and stdout of the process to the current terminal\n");
  fprintf(stderr, "   -v            Show verbose debugging output\n");
  fprintf(stderr, "   -h            Show this help message\n\n");
  fprintf(stderr, "Please report bugs to support@f-omega.com\n");
}

static int kill_process(clusterctl *ctl, const char *namespace, clusterd_pid_t pid) {
  char pidstr[32];
  int err;

  err = snprintf(pidstr, sizeof(pidstr), PID_F, pid);
  if ( err >= sizeof(pidstr) ) {
    CLUSTERD_LOG(CLUSTERD_CRIT, "Could not write pid: overflow");
    errno = ENAMETOOLONG;
    return -1;
  }

  err = clusterctl_call_simple(ctl, CLUSTERCTL_MAY_WRITE, kill_process_lua,
                               NULL, 0,
                               "namespace", namespace,
                               "pid", pidstr,
                               NULL);
  if ( err < 0 ) {
    CLUSTERD_LOG(CLUSTERD_CRIT, "Could not destroy process: %s", strerror(errno));
    return -1;
  }

  return 0;
}

static int randomint(int lo, int hi) {
  double r;
  long int n, i;

  r = random();
  n = hi - lo;

  i = lround(n * r);

  return lo + i;
}

static const char *sample_nodes(FILE *schedule) {
  static char chosennode[256], chosenip[128];
  char nodeline[1024], nodeid[37], hostname[256], hostip[128];
  double last_score = NAN;
  int nodes_examined = 0, err;

  while ( fgets(nodeline, sizeof(nodeline), schedule) != NULL ) {
    int n, r;
    double next_score;

    n = sscanf(nodeline, "%37s %256s %128s %lf", nodeid, hostname, hostip, &next_score);
    if ( n != 4 ) {
      CLUSTERD_LOG(CLUSTERD_DEBUG, "Could not read nodeline, ignoring: %s", nodeline);
      continue;
    }

    if ( last_score == last_score &&
         next_score < last_score )
      break;

    if ( last_score != last_score ) { // No nodes seen
      last_score = next_score;
    }

    nodes_examined++;

    r = randomint(1, nodes_examined);

    if ( r <= 1 ) {
      // Add this monitor if we need to
      if ( g_monitors_desired > 0 ) {
        const char *nodestr = strdup(chosennode);
        if ( !nodestr ) {
          CLUSTERD_LOG(CLUSTERD_WARNING, "Out of memory copying %s", chosennode);
        } else {
          err = add_monitor(chosennode);
          if ( err < 0 ) {
            CLUSTERD_LOG(CLUSTERD_WARNING, "Could not add monitor %s", chosennode);
          }
        }
      }

      // Replace node
      strncpy(chosennode, hostname, sizeof(chosennode));
      strncpy(chosenip, hostip, sizeof(chosenip));
    }
  }

  if ( last_score == last_score ) {
    while ( g_monitors_desired > 0 &&
            fgets(nodeline, sizeof(nodeline), schedule) != NULL ) {
      const char *monitor;
      double next_score;

      // Add any remaining node as monitors
      err = sscanf(nodeline, "%37s %256s %128s %lf", nodeid, hostname, hostip, &next_score);
      if ( err != 4 ) {
        CLUSTERD_LOG(CLUSTERD_DEBUG, "Could not read nodeline while processin monitors, ignoring: %s", nodeline);
        continue;
      }

      monitor = strdup(hostname);
      if ( !monitor ) {
        CLUSTERD_LOG(CLUSTERD_WARNING, "Out of memory copying %s", nodeid);
        continue;
      }

      err = add_monitor(monitor);
      if ( err < 0 ) {
        CLUSTERD_LOG(CLUSTERD_WARNING, "Could not add monitor %s", monitor);
        continue;
      }
    }

    return chosennode;
  }

  CLUSTERD_LOG(CLUSTERD_ERROR, "No nodes available for scheduling");
  return NULL;
}

static void push_arg(const char ***args, const char *arg) {
  int i = 0;

  if ( ! (*args) )
    *args = malloc(sizeof(*args) * 2);
  else {
    for ( i = 0; (*args)[i]; ++i );

    *args = realloc(*args, sizeof(*args) * (i + 2));
  }

  if ( ! (*args) ) {
    CLUSTERD_LOG(CLUSTERD_ERROR, "Could not push argument: no memory");
    exit(EXIT_FAILURE);
  }

  (*args)[i] = arg;
  (*args)[i + 1] = NULL;
}

static const char *schedule_process(clusterctl *ctl, const char *namespace, const char *service) {
  int outpipe[2], err;
  pid_t child;

  err = pipe(outpipe);
  if ( err < 0 ) {
    CLUSTERD_LOG(CLUSTERD_ERROR, "Could not create pipe for 'clusterd-schedule'");
    return NULL;
  }

  child = fork();
  if ( child < 0 ) {
    CLUSTERD_LOG(CLUSTERD_ERROR, "Could not fork process for 'clusterd-schedule'");
    close(outpipe[0]);
    close(outpipe[1]);
    return NULL;
  } else if ( child == 0 ) {
    close(outpipe[0]);
    const char **args = NULL, *schedprog;

    push_arg(&args, "clusterd-schedule");
    if ( CLUSTERD_LOG_LEVEL <= CLUSTERD_DEBUG )
      push_arg(&args, "-v");
    push_arg(&args, "-n");
    push_arg(&args, namespace);
    push_arg(&args, "-s");
    push_arg(&args, service);
    push_arg(&args, "-t");
    push_arg(&args, "30");

    err = dup2(outpipe[1], STDOUT_FILENO);
    if ( err < 0 ) {
      CLUSTERD_LOG(CLUSTERD_ERROR, "Could not redirect stdout");
      exit(EXIT_FAILURE);
    }
    close(outpipe[1]);

    // Run clusterd-schedule
    schedprog = getenv("CLUSTERD_SCHEDULE");
    if ( !schedprog )
      schedprog = "clusterd-schedule";

    execvp(schedprog, (char *const *) args);

    CLUSTERD_LOG(CLUSTERD_ERROR, "Could not execute clusterd-schedule: %s", strerror(errno));
    exit(EXIT_FAILURE);
  } else {
    FILE *schedule = NULL;
    int wsts;
    pid_t perr;
    const char *ret;

    close(outpipe[1]);

    schedule = fdopen(outpipe[0], "rt");
    if ( !schedule ) {
      close(outpipe[0]);

      CLUSTERD_LOG(CLUSTERD_ERROR, "Could not open clusterd-schedule output");

      ret = NULL;
      goto done;
    }

    // Read the schedule line by line
    ret = sample_nodes(schedule);

  done:
    if ( schedule )
      fclose(schedule);

    perr = waitpid(child, &wsts, 0);
    if ( perr < 0 ) {
      CLUSTERD_LOG(CLUSTERD_ERROR, "Could not wait on clusterd-schedule process");
      ret = NULL;
    }

    if ( WIFEXITED(wsts) && WEXITSTATUS(wsts) != 0 ) {
      CLUSTERD_LOG(CLUSTERD_ERROR, "clusterd-schedule returned failure error code: %d", WEXITSTATUS(wsts));
      ret = NULL;
    }

    if ( WIFSIGNALED(wsts) && WTERMSIG(wsts) != SIGPIPE ) {
      CLUSTERD_LOG(CLUSTERD_ERROR, "clusterd-schedule killed by non-SIGPIPE signal: %d", WTERMSIG(wsts));
      ret = NULL;
    }

    return ret;
  }
}

static int create_process(clusterctl *ctl,
                          const char *namespace, const char *service,
                          const char *pinnednode, clusterd_pid_t *pid,
                          char *pinnedaddr, size_t pinnedaddrlen) {
  int err;
  char *pid_start, *pid_end;
  char newprocstr[128 + HOST_NAME_MAX];

  err = clusterctl_call_simple(ctl, CLUSTERCTL_MAY_WRITE, create_process_lua,
                               newprocstr, sizeof(newprocstr),
                               "namespace", namespace,
                               "service", service,
                               "pinned", pinnednode,
                               NULL);
  if ( err < 0 ) {
    CLUSTERD_LOG(CLUSTERD_CRIT, "Could not create process; %s", strerror(errno));
    return -1;
  }

  pid_start = strchr(newprocstr, '\n');
  if ( !pid_start ) {
    CLUSTERD_LOG(CLUSTERD_CRIT, "Could not get pid from create process call");
    errno = EPROTO;
    return -1;
  }

  *pid_start = '\0';

  // Read the node address
  if ( (pid_start - newprocstr) >= pinnedaddrlen ) {
    CLUSTERD_LOG(CLUSTERD_ERROR, "Cannot fit node address %s into buffer", newprocstr);
    errno = ENAMETOOLONG;
    return -1;
  }

  strncpy(pinnedaddr, newprocstr, pinnedaddrlen);
  pid_start ++;

  errno = 0;
  *pid = strtol(pid_start, &pid_end, 10);
  if ( errno != 0 ) {
    CLUSTERD_LOG(CLUSTERD_ERROR, "Error reading pid: %s", strerror(errno));
    errno = EINVAL;
    return -1;
  }

  if ( *pid_end != '\n' &&
       *pid_end != '\0' ) {
    CLUSTERD_LOG(CLUSTERD_ERROR, "Junk at end of process id: %s", pid_end);
    errno = EINVAL;
    return -1;
  }

  return 0;
}

clusterd_exec_wait parse_wait_condition(const char *c) {
  if ( strcasecmp(c, "none") == 0 )
    return CLUSTERD_EXEC_NO_WAIT;
  else if ( strcasecmp(c, "started") == 0 )
    return CLUSTERD_EXEC_WAIT_UNTIL_STARTED;
  else if ( strcasecmp(c, "ready") == 0 )
    return CLUSTERD_EXEC_WAIT_UNTIL_READY;
  else
    return CLUSTERD_EXEC_WAIT_INVALID;
}

static int launch_service(clusterctl *ctl, const char *nodeaddr,
                          const char *namespace, const char *service, clusterd_pid_t pid,
                          int keep_logs, int interactive,
                          int argc, char *const *argv) {
  const char **new_argv, *cmd;
  char ourname[HOST_NAME_MAX + 1], pidstr[32];
  int err, argind = 0, sts[2];
  pid_t child;
  clusterd_monitor *m;

  err = snprintf(pidstr, sizeof(pidstr), PID_F, pid);
  if ( err >= sizeof(pidstr) ) {
    errno = ENOMEM;
    return -1;
  }

  CLUSTERD_LOG(CLUSTERD_INFO, "Launching service %s with %d argument(s) on %s",
               service, argc, nodeaddr);

  // If the node hostname matches our hostname, then we don't need to
  // use ssh
  err = gethostname(ourname, sizeof(ourname));
  if ( err < 0 ) {
    fprintf(stderr, "Could not get hostname: %s", strerror(errno));
    return err;
  }

  new_argv = calloc(argc + MAX_ARGS + 1, sizeof(char *));
  if ( !new_argv ) goto nomem;

  if ( strncmp(ourname, nodeaddr, sizeof(ourname)) == 0 ) {
    cmd = "sudo";
  } else {
    cmd = "ssh";
    new_argv[argind++] = "ssh";
    new_argv[argind++] = "-l";
    new_argv[argind++] = "clusterd";
    new_argv[argind++] = nodeaddr;
  }

  new_argv[argind++] = "sudo";
  new_argv[argind++] = "clusterd-host";

  if ( CLUSTERD_LOG_LEVEL == CLUSTERD_DEBUG )
    new_argv[argind++] = "-v";

  if ( keep_logs )
    new_argv[argind++] = "-d";

  if ( interactive )
    new_argv[argind++] = "-i";

  new_argv[argind++] = "-p";
  new_argv[argind++] = pidstr;
  new_argv[argind++] = "-n";
  new_argv[argind++] = namespace;

  // For each monitor, add the monitor arguments
  for ( m = g_monitors; m; m = m->next ) {
    new_argv[argind++] = "-m";
    new_argv[argind++] = m->monitor_spec;

    CLUSTERD_LOG(CLUSTERD_DEBUG, "Sending monitor %s", m->monitor_spec);
  }

  new_argv[argind++] = service;
  new_argv[argind++] = "--";
  memcpy(new_argv, argv, sizeof(*new_argv) * argc);
  argind += argc;

  err = pipe2(sts, O_CLOEXEC);
  if ( err < 0 ) {
    int serrno = errno;
    free(new_argv);
    CLUSTERD_LOG(CLUSTERD_ERROR, "Could not create status pipe: %s", strerror(errno));
    errno = serrno;
    return 1;
  }

  child = fork();
  if ( child < 0 ) {
    CLUSTERD_LOG(CLUSTERD_CRIT, "Could not start process: %s", strerror(errno));
    return -1;
  } else if ( child == 0 ) {
    int serrno;

    close(sts[0]);

    execvp(cmd, (char * const*)new_argv);

    serrno = errno;
    err = write(sts[1], &serrno, sizeof(serrno));
    if ( err < 0 ) {
      CLUSTERD_LOG(CLUSTERD_CRIT, "Could not write to status pipe. This is bad");
      exit(100);
    }

    exit(100);
  } else {
    int werr, wsts;
    close(sts[1]);
    free(new_argv);

    err = read(sts[0], &werr, sizeof(werr));
    if ( err < 0 ) {
      CLUSTERD_LOG(CLUSTERD_CRIT, "Could not read from status pipe: %s", strerror(errno));
      errno = EPIPE;
      return -1;
    } else if ( err == sizeof(werr) ) {
      // Exec failed
      CLUSTERD_LOG(CLUSTERD_CRIT, "Exec of %s failed: %s", cmd, strerror(werr));

      if ( waitpid(child, &wsts, WNOHANG) < 0 ) {
        CLUSTERD_LOG(CLUSTERD_CRIT, "Could not wait for child: %s", strerror(errno));
      } else {
        CLUSTERD_LOG(CLUSTERD_ERROR, "Child exited with status: %d", WEXITSTATUS(wsts));
      }

      errno = werr;
      return -1;
    } else if ( err > 0 ) {
      CLUSTERD_LOG(CLUSTERD_CRIT, "Invalid data read from status pipe: %s", strerror(errno));
      errno = EPROTO;
      return -1;
    }
  }

  return 0;

 nomem:
  errno = ENOMEM;
  return -1;
}

static int add_monitor(const char *monitor) {
  clusterd_monitor *m;

  m = malloc(sizeof(*m));
  if ( !m ) {
    CLUSTERD_LOG(CLUSTERD_CRIT, "Could not allocate space for monitor");
    errno = ENOMEM;
    return -1;
  }

  m->monitor_spec = monitor;
  m->next = g_monitors;

  g_monitors = m;

  if ( g_monitors_desired > 0 )
    g_monitors_desired--;

  return 0;
}

int main(int argc, char *const *argv) {
  int c;
  const char *namespace = "default", *service = NULL, *pinnednode = NULL,
    *nodeaddr = NULL;
  char *end;
  int retries, firstarg = -1, redirect_stdout = 0, redirect_stdin = 0, keep_logs = 0, err;
  clusterd_exec_wait wait_condition = CLUSTERD_EXEC_NO_WAIT;
  clusterd_exec_availability availability = CLUSTERD_HIGH_AVAILABILITY;

  clusterd_pid_t pid;
  clusterctl ctl;

  char pinnednodeaddr[HOST_NAME_MAX];

  setlocale(LC_ALL, "C");
  srandom(time(NULL));

  while ( (c = getopt(argc, argv, "-:n:N:l:w:m:iIvhLH:d")) != -1 ) {
    switch ( c ) {
    case 1:
      if ( optarg[0] == '@' ) {
        if ( !pinnednode ) {
          pinnednode = optarg + 1;
          if ( strcmp(pinnednode, "local") == 0 ) {
            // Get the local clusterd hostname
            pinnednode = clusterd_hostname();
            if ( !pinnednode ) {
              CLUSTERD_LOG(CLUSTERD_ERROR, "Could not get local hostname");
            }
          }
        } else {
          CLUSTERD_LOG(CLUSTERD_ERROR, "Only one pinned node can be specified if pinning a process\n");
          usage();
          return 1;
        }
      } else {
        firstarg = optind;
        service = optarg;
        goto argsdone;
      }
      break;

    case 'm':
      err = add_monitor(optarg);
      if ( err < 0 ) {
        CLUSTERD_LOG(CLUSTERD_ERROR, "Could not add monitor %s", optarg);
        return 1;
      }
      break;

    case 'n':
      namespace = optarg;
      break;

    case 'N':
      errno = 0;
      retries = strtol(optarg, &end, 10);
      if ( errno != 0 || *end != '\0' ) {
        CLUSTERD_LOG(CLUSTERD_ERROR, "Invalid retries number: %s", optarg);
        usage();
        return 1;
      }
      break;

    case 'l':
      CLUSTERD_LOG(CLUSTERD_ERROR, "TODO Resouces not yet supported");
      break;

    case 'H':
      availability = CLUSTERD_HIGH_AVAILABILITY;
      err = sscanf(optarg, "%lu", &g_monitors_desired);
      if ( err != 1 ) {
        CLUSTERD_LOG(CLUSTERD_ERROR, "-H expects a number as an argument, got %s", optarg);
        return 1;
      }
      break;

    case 'L':
      availability = CLUSTERD_LOCAL_AVAILABILITY;
      break;

    case 'd':
      keep_logs = 1;
      break;

    case 'w':
      wait_condition = parse_wait_condition(optarg);
      if ( wait_condition == CLUSTERD_EXEC_WAIT_INVALID ) {
        CLUSTERD_LOG(CLUSTERD_ERROR, "Invalid wait condition: %s", optarg);
        usage();
        return 1;
      }
      break;

    case 'I':
      redirect_stdin = 1;

    case 'i':
      redirect_stdout = 1;
      break;

    case 'v':
      CLUSTERD_LOG_LEVEL = CLUSTERD_DEBUG;
      break;

    case 'h':
      usage();
      return 0;

    case ':':
      switch (optopt) {
      case 'H':
        availability = CLUSTERD_HIGH_AVAILABILITY;
        break;

      default:
        CLUSTERD_LOG(CLUSTERD_ERROR, "Option -%c requires an argument", optopt);
        usage();
        return 1;
      }
      break;

    default:
      usage();
      return 1;
    }
  }

 argsdone:
  if ( (redirect_stdin || redirect_stdout) &&
       availability == CLUSTERD_HIGH_AVAILABILITY ) {
    CLUSTERD_LOG(CLUSTERD_ERROR, "High availability is not compatible with stdin/stdout redirection");
    usage();
    return 1;
  }

  if ( !service ) {
    CLUSTERD_LOG(CLUSTERD_ERROR, "A service must be specified\n");
    return 1;
  }

  err = clusterctl_open(&ctl);
  if ( err < 0 ) {
    CLUSTERD_LOG(CLUSTERD_CRIT, "Could not connect to controller: %s", strerror(errno));
    return 1;
  }

  CLUSTERD_LOG(CLUSTERD_DEBUG, "Starting a process in service %s (namespace %s)\n",
               service, namespace);

  if ( !pinnednode )
    pinnednode = schedule_process(&ctl, namespace, service);

  if ( !pinnednode )
    CLUSTERD_LOG(CLUSTERD_ERROR, "No node could be chosen for scheduling");

  /* First we find the service and make sure it exists, and create a
   * process in the scheduling state */
  err = create_process(&ctl, namespace, service, pinnednode, &pid,
                       pinnednodeaddr, sizeof(pinnednodeaddr));
  /* Read the pid and maybe the node id */
  if ( err < 0 ) {
    CLUSTERD_LOG(CLUSTERD_CRIT, "Could not create process: %s", strerror(errno));
    return 1;
  }

  /* Before actually executing anything, check if we're in
   * high-availability mode. If we are, we need to contact some other
   * nodes in the system and recruit them into helping us with this
   * execution.
   *
   * We will also fork a thread that will send heartbeats to the
   * monitors periodically. If the monitors don't hear from us,
   * they'll launch a cluster-exec of their own to finish the
   * deployment.
   *
   * Basically, these are monitor nodes that we choose to monitor our
   * own deployment. Once at least one monitor node has been
   * contacted, we print a status message to stdout. Once all monitor
   * nodes have been contacted, we print a status message indicating
   * high-availability. */
  if ( availability != CLUSTERD_HIGH_AVAILABILITY ) {
    g_monitors_desired = 0;
  }

  nodeaddr = pinnednodeaddr;

  if ( !nodeaddr ) {
    CLUSTERD_LOG(CLUSTERD_ERROR, "No node could be found for scheduling");
    goto cleanup;
  }

  if ( firstarg < 0 ) firstarg = argc;

  err = launch_service(&ctl, nodeaddr, namespace, service, pid,
                       keep_logs, redirect_stdin || redirect_stdout,
                       argc - firstarg, argv + firstarg);
  if ( err < 0 ) {
    CLUSTERD_LOG(CLUSTERD_ERROR, "Could not launch service: %s", strerror(errno));
    goto cleanup;
  }

  // Now ideally, we'll

  return 0;

 cleanup:
  err = kill_process(&ctl, namespace, pid);
  if ( err < 0 ) {
    CLUSTERD_LOG(CLUSTERD_WARNING, "Could not kill process %u (namespace %s): %s",
                 pid, namespace, strerror(errno));
  }
  return 1;
}
