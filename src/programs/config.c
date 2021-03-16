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

#define CLUSTERD_COMPONENT "config"
#include "clusterd/log.h"
#include "clusterd/common.h"
#include "config.h"

#include <string.h>
#include <errno.h>
#include <grp.h>

extern int CLUSTERD_LOG_LEVEL;

static int clusterd_read_config(FILE *config, config_func_t cmdfunc) {
  char line[4096];
  int err;

  while ( fgets(line, sizeof(line), config) ) {
    char *cmdend, *valstart;
    if ( strchr(line, '\n') == NULL ) {
      if ( !feof(config) ) {
        CLUSTERD_LOG(CLUSTERD_ERROR, "Line is too long");
        fclose(config);

        errno = ENOMEM;
        return -1;
      }
    }

    cmdend = strpbrk(line, "\n \t");
    if ( !cmdend ) {
      CLUSTERD_LOG(CLUSTERD_ERROR, "No command: %s", line);

      fclose(config);
      errno = EINVAL;
      return -1;
    }

    *cmdend = '\0';
    valstart = cmdend + 1;

    if ( cmdfunc(line, valstart) < 0 )
      return -1;
  }

  return 0;
}

int clusterd_read_system_config(config_func_t cmdfunc) {
  struct stat configstat;
  char configpath[PATH_MAX];
  FILE *sysconfig;
  int err;

  err = snprintf(configpath, sizeof(configpath), "%s/system", clusterd_get_config_dir());
  if ( err >= sizeof(configpath) ) {
    CLUSTERD_LOG(CLUSTERD_CRIT, "Could not create system config path");
    errno = ENAMETOOLONG;
    return -1;
  }

  // The system config file must be writable only by root and owned by root
  err = lstat(configpath, &configstat);
  if ( err < 0 ) {
    CLUSTERD_LOG(CLUSTERD_CRIT, "Could not stat() %s: %s", configpath, strerror(errno));
    return -1;
  }

  if ( configstat.st_uid != 0 ||
       (configstat.st_mode & (S_IWGRP | S_IWOTH | S_IXUSR | S_IXGRP | S_IXOTH)) != 0 ) {
    CLUSTERD_LOG(CLUSTERD_CRIT, "%s must be writable only by root", configpath);

    errno = EPERM;
    return -1;
  }

  // Open the file and process it line by line
  sysconfig = fopen(configpath, "rt");
  if ( !sysconfig ) {
    CLUSTERD_LOG(CLUSTERD_ERROR, "Could not open config file %s: %s", configpath, strerror(errno));
    return -1;
  }

  err = clusterd_read_config(sysconfig, cmdfunc);
  if ( err < 0 ) {
    int serrno = errno;
    fclose(sysconfig);
    errno = serrno;
    return -1;
  }

  fclose(sysconfig);
  return 0;
}

int clusterd_parse_uid_range(const char *range, uid_t *lower, unsigned int *count) {
  int err;

  err = sscanf(range, "%u %u", lower, count);
  if ( err != 2 ) {
    errno = EINVAL;
    return -1;
  }

  return 0;
}

int clusterd_parse_group(const char *group, gid_t *gid) {
  int err;

  err = sscanf(group, "%u", gid);
  if ( err != 1 ) {
    // Group is a group name
    struct group *info = getgrnam(group);
    if ( !info ) {
      CLUSTERD_LOG(CLUSTERD_ERROR, "Group %s does not exist", group);
      errno = ENOENT;
      return -1;
    }

    return info->gr_gid;
  } else
    return 0;
}
