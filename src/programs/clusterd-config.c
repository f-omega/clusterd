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

#define CLUSTERD_COMPONENT "clusterd-config"
#include "clusterd/log.h"
#include "clusterd/common.h"
#include "config.h"

#include <locale.h>
#include <stdlib.h>
#include <time.h>
#include <stdio.h>

int CLUSTERD_LOG_LEVEL = CLUSTERD_INFO;

char *g_key = NULL;
int g_key_found;

static int read_g_key(const char *key, const char *value) {
  if ( g_key_found ) return 0;

  if ( strcmp(key, g_key) == 0 ) {
    g_key_found = 1;
    printf("%s\n", value);
  }
}

static void usage() {
  fprintf(stderr, "clusterd-config - retrieve a configuration value from the system configuration\n");
  fprintf(stderr, "Usage: clusterd-config -vh [KEY...] -q [KEY...]\n\n");
  fprintf(stderr, "   KEY     The name of the key to look up\n");
  fprintf(stderr, "   -q      Treat all subsequent keys as optional. If not found\n");
  fprintf(stderr, "           a blank line will be printed instead of an error\n");
  fprintf(stderr, "   -v      Display verbose debug output\n");
  fprintf(stderr, "   -h      Show this help menu\n\n");
  fprintf(stderr, "Please report bugs to support@f-omega.com\n");
}

int main(int argc, char *const *argv) {
  int i, quiet = 0, c, err;

  setlocale(LC_ALL, "C");
  srandom(time(NULL));

  while ( (c = getopt(argc, argv, "-vhq")) != -1 ) {
    switch ( c ) {
    case 1:
      g_key = optarg;
      g_key_found = 0;

      err = clusterd_read_system_config(read_g_key);
      if ( err < 0 || ! g_key_found ) {
        if ( !quiet ) {
          CLUSTERD_LOG(CLUSTERD_ERROR, "Key %s not found", g_key);
          return 1;
        } else {
          printf("\n");
        }
      }
      break;

    case 'q':
      quiet = 1;
      break;

    case 'v':
      CLUSTERD_LOG_LEVEL = CLUSTERD_DEBUG;
      break;

    case 'h':
      if ( g_key ) {
        CLUSTERD_LOG(CLUSTERD_CRIT, "Can't combine -h with lookups\n");
      }

      usage();

      if ( g_key ) return 1;
      return 0;

    default:
      usage();
      return 1;
    }
  }

  return 0;
}
