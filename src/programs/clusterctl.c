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

#define CLUSTERD_COMPONENT "clusterctl"
#include "clusterd/log.h"
#include "libclusterctl.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <limits.h>
#include <netdb.h>
#include <fcntl.h>
#include <locale.h>

#define MAX_PARAMETER_SZ 64 * 1024
static char saved_parameters[MAX_PARAMETER_SZ + 1];
static size_t saved_parameter_sz = 0;

int CLUSTERD_LOG_LEVEL = CLUSTERD_INFO;

void usage() {
  fprintf(stderr, "clusterctl -- issue commands to the clusterd controller\n");
  fprintf(stderr, "Usage: clusterctl -vpwec [-E param=value]... /path/to/script\n\n");
  fprintf(stderr, "   -p              Save the script for future invocation. Requires\n");
  fprintf(stderr, "                   a script file. Provides small perf improvement\n");
  fprintf(stderr, "   -D PARAM=VALUE  Define a parameter to the script\n");
  fprintf(stderr, "   -d              Read additional parameters from stdin\n");
  fprintf(stderr, "   -w              Allow the script to make permanent updates to\n");
  fprintf(stderr, "                   the controller state\n");
  fprintf(stderr, "   -c              Mandate consistency by always using the leader\n");
  fprintf(stderr, "   -v              Show verbose debugging output\n\n");
  fprintf(stderr, "If script path is not given, clusterctl will read the script from\n");
  fprintf(stderr, "standard input\n\n");
  fprintf(stderr, "Please report bugs to support@f-omega.com\n");
}

static int send_script(int sk, const char *script_path) {
  if ( script_path ) {
    errno = ENOENT;
    return -1;
  } else {
  }
}

int main(int argc, char *const *argv) {
  int c;

  const char *script_path = NULL;
  int readonly = 1, save = 0, readenv = 0, force_consistent = 0, read_error = 0;
  size_t remaining, written;
  ssize_t err;

  char line_buf[128 * 1024];
  size_t line_sz = 0;

  clusterctl ctl;

  setlocale(LC_ALL, "C");
  while ((c = getopt(argc, argv, "-vhwpcdD:")) != -1) {
    switch (c) {
    case 1:
      if ( !script_path )
        script_path = optarg;
      else {
        fprintf(stderr, "Only one script may be given on the command line at a time\n");
      }
      break;

    case 'c':
      force_consistent = 1;
      break;

    case 'D':
      /* Save the argument */
      remaining = MAX_PARAMETER_SZ - saved_parameter_sz;
      written = snprintf(saved_parameters + saved_parameter_sz, remaining,
                         "%s\n", optarg);
      if ( written >= remaining ) {
        CLUSTERD_LOG(CLUSTERD_ERROR, "Too many parameters provided");
        return 1;
      } else {
        saved_parameter_sz += written;
      }
      break;

    case 'p':
      save = 1;
      break;

    case 'd':
      readenv = 1;
      break;

    case 'w':
      readonly = 0;
      break;

    case 'v':
      CLUSTERD_LOG_LEVEL = CLUSTERD_DEBUG;
      break;

    case 'h':
      usage();
      return 0;

    default:
      usage();
      return 1;
    }
  }

  if ( readenv && !script_path ) {
    CLUSTERD_LOG(CLUSTERD_ERROR, "You must provide a script if using -e");
  }

  err = clusterctl_open(&ctl, readonly == 0 || force_consistent);
  if ( err < 0 ) {
    CLUSTERD_LOG(CLUSTERD_CRIT, "Could not connect to controller: %s", strerror(errno));
    return 1;
  }

  // If save is set, then transmit sha256 sum of file
  if ( save ) {
    if ( !script_path ) {
      CLUSTERD_LOG(CLUSTERD_CRIT, "If you want to save the command, you must provide a file");
      return 3;
    }

    //    err = clusterctl_invoke_sha256(&ctl, _);
    return 3;
  } else {
    err = clusterctl_begin_custom(&ctl);
  }
  if ( err < 0 ) {
    CLUSTERD_LOG(CLUSTERD_CRIT, "Could not send command: %s", strerror(errno));
    return 1;
  }

  if ( err == CLUSTERCTL_NEEDS_SCRIPT ) {
    if ( script_path ) {
      int fd = open(script_path, O_RDONLY);
      if ( fd < 0 ) {
        CLUSTERD_LOG(CLUSTERD_CRIT, "Could not open script file %s: %s", script_path, strerror(errno));
        return 1;
      }

      err = clusterctl_pipe_script(&ctl, fd);
      if ( err < 0 ) {
        CLUSTERD_LOG(CLUSTERD_CRIT, "Could not send script from file %s: %s", script_path, strerror(errno));
        close(fd);
        return 1;
      }

      close(fd);
    } else {
      err = clusterctl_pipe_script(&ctl, STDIN_FILENO);
    }
    if ( err < 0 ) {
      CLUSTERD_LOG(CLUSTERD_CRIT, "Could not send script: %s", strerror(errno));
      return 1;
    }
  }

  // Send script end
  err = clusterctl_upload_script(&ctl, NULL, 0);
  if ( err < 0 ) {
    CLUSTERD_LOG(CLUSTERD_CRIT, "Could not finish script: %s", strerror(errno));
    return 1;
  }

  // Send parameters
  err = clusterctl_upload_params(&ctl, saved_parameters, saved_parameter_sz);
  if ( err < 0 ) {
    CLUSTERD_LOG(CLUSTERD_CRIT, "Could not upload parameters: %s", strerror(errno));
    return 1;
  }

  if ( readenv ) {
    // TODO splice stdin to env
    return 2;
  }

  err = clusterctl_invoke(&ctl);
  if ( err < 0 ) {
    CLUSTERD_LOG(CLUSTERD_CRIT, "Could not invoke command: %s", strerror(errno));
  }

  // Now the command runs, we read in each line and output it until
  // --DONE. If we get --ERROR then read the error to stderr
  read_error = 0;
  for (;;) {
    line_sz = sizeof(line_buf);
    err = clusterctl_read_output(&ctl, line_buf, &line_sz);
    if ( err < 0 ) {
      CLUSTERD_LOG(CLUSTERD_CRIT, "Could not read output: %s", strerror(errno));
      read_error = 1;
      break;
    } else if ( err == CLUSTERCTL_READ_DONE ) {
      break;
    } else if ( err == CLUSTERCTL_READ_ERROR ) {
      if ( !read_error ) {
        CLUSTERD_LOG(CLUSTERD_CRIT, "clusterctl error:");
        read_error = 1;
      }
      CLUSTERD_LOG(CLUSTERD_CRIT, "error: %.*s", (int)line_sz, line_buf);
    } else if ( err == CLUSTERCTL_READ_LINE ) {
      printf("%.*s", (int)line_sz, line_buf);
    }
  }

  if ( err < 0 && errno != ESHUTDOWN ) {
    read_error = 1;
    CLUSTERD_LOG(CLUSTERD_CRIT, "Error while reading output: %s", strerror(errno));
  }

  clusterctl_close(&ctl);

  if ( read_error ) {
    return 1;
  } else {
    return 0;
  }
}
