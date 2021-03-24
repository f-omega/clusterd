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

#define CLUSTERD_COMPONENT "clusterd-setuidgidmap"
#include "clusterd/log.h"
#include "clusterd/common.h"

#include <locale.h>
#include <getopt.h>
#include <time.h>
#include <errno.h>
#include <stdlib.h>
#include <sys/types.h>

#define MAP_CAPACITY (16 * 1024)

#define SYS_UID_MIN 400
#define SYS_USER_COUNT 600
#define NORMAL_UID_MIN 1000

static int CLUSTERD_LOG_LEVEL = CLUSTERD_INFO;

static char uid_map[MAP_CAPACITY], gid_map[MAP_CAPACITY];
static size_t uid_offs = 0, gid_offs = 0;

static uid_t g_ns_uid_lower = 0;
static unsigned int g_ns_uid_count = 0;

static int write_map(char *buf, size_t *offs, uid_t ns_start, uid_t parent_start, unsigned int count) {
  int err;
  size_t remaining = MAP_CAPACITY - *offs;

  err = snprintf(buf + *offs, remaining, "%d %d %u\n", ns_start, parent_start, count);
  if ( err >= remaining ) {
    errno = ENAMETOOLONG;
    return -1;
  }

  *offs += err;

  return 0;
}

static int set_mappings(const char *pidstr) {
  pid_t childpid, parentpid;
  int err, ret = -1;
  struct stat procstat;
  char procstatpath[PATH_MAX];
  char comm[33];
  uid_t normal_user_start = 0;
  unsigned int system_user_count = SYS_USER_COUNT, normal_user_count = 0;

  err = sscanf(pidstr, "%d", &childpid);
  if ( err != 1 ) {
    errno = ESRCH;
    return -1;
  }

  err = snprintf(procstatpath, sizeof(procstatpath), "/proc/%d/stat", childpid);
  if ( err >= sizeof(procstatpath) ) {
    errno = ENAMETOOLONG;
    return -1;
  }

  err = stat(procstatpath, &procstat);
  if ( err < 0 ) {
    return -1;
  }

  err = write_map(gid_map, &gid_offs, 0, procstat.st_gid, 1);
  if ( err < 0 )
    return -1;

  err = write_map(uid_map, &uid_offs, 0, procstat.st_uid, 1);
  if ( err < 0 )
    return -1;

  if ( system_user_count > g_ns_uid_count )
    system_user_count = g_ns_uid_count;

  if ( g_ns_uid_count > system_user_count ) {
    normal_user_count = g_ns_uid_count - system_user_count;
    normal_user_start = g_ns_uid_lower + system_user_count;
  }

  if ( system_user_count > 0 ) {
    err = write_map(uid_map, &uid_offs, SYS_UID_MIN, g_ns_uid_lower, system_user_count);
    if ( err < 0 )
      return -1;
  }

  // Now write as many normal users as possible
  if ( normal_user_count > 0 ) {
    err = write_map(uid_map, &uid_offs, NORMAL_UID_MIN, g_ns_uid_lower + system_user_count, normal_user_count);
    if ( err < 0 )
      return -1;
  }

  ret = 0;
 done:
  return ret;
}

static void usage() {
  fprintf(stderr, "clusterd-setuidgidmap - set up sub-uid/gid mappings for a child process\n");
  fprintf(stderr, "Usage: clusterd-setuidgidmap -vh PID...\n\n");
  fprintf(stderr, "   PID        The PID of the process whose namespace to set up\n");
  fprintf(stderr, "   -v         Display verbose debug output\n");
  fprintf(stderr, "   -h         Show this help menu\n\n");
  fprintf(stderr, "Please reports bugs to support@f-omega.com\n");
}

int main(int argc, char *const *argv) {
  int c, err;

  setlocale(LC_ALL, "C");
  srandom(time(NULL));

  while ( (c = getopt(argc, argv, "-vh")) != -1 ) {
    switch ( c ) {
    case 1:
      // Interpret this argument as a PID to set up uid/gid mappings for
      err = set_mappings(optarg);
      if ( err < 0 ) {
        CLUSTERD_LOG(CLUSTERD_INFO, "Could not set mappings for %s: %s", optarg, strerror(errno));
        return 1;
      }
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
}
