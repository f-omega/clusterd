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
#include "libclusterctl.h"

int main(int argc, char *const *argv) {
  int c;
  const char *namespace = "default", *end, *tmppath = NULL;
  int retries, firstarg = -1, redirect_stdout = 0, redirect_stdin = 0;
  int wait_condition = CLUSTERD_EXEC_NO_WAIT, availability = CLUSTERD_LOCAL_AVAILABILITY;

  while ( (c = getopt("-n:N:l:f:w:iIvh", argc, argv)) != -1 ) {
    switch ( c ) {
    case 1:
      firstarg = optind - 1;
      goto argsdone;

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
      CLUSTERD_LOG(CLUSTERD_ERROR, "Resouces not yet supported");
      
      break;

    case 'H':
      availability = CLUSTERD_HIGH_AVAILABILITY;
      break;

    case 'f':
      tmppath = optarg;
      break;

    case 'w':
      wait_condition = parse_wait_condition(optarg);
      if ( wait_condition < 0 ) {
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

    default:
      usage();
      return 1;
    }
  }

  if ( (redirect_stdin || redirect_stdout) &&
       availibility == CLUSTERD_HIGH_AVAILABILITY ) {
    CLUSTERD_LOG(CLUSTERD_ERROR, "High availability is not compatible with stdin/stdout redirection");
    usage();
    return 1;
  }

  /* First we find the service, and its resource requirements */

  /* Then we create a new process, in the scheduling state */

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

  /* Next, we choose a random assortment of nodes from the controller,
   * and get them along with their resource capabilities and
   * availabilities.
   */

  /* Next we attempt to contact all nodes and check the current state
   * of their resources */
  

  /* All nodes are now ranked. We choose the top-ranked one, or if
   * there are multiple, we randomize the choice. */

  /* Launch service */
  { "ssh", "-l", username, ip, 
}
