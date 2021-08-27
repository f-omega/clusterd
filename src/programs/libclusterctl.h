#ifndef __clusterd_libclusterctl_H__
#define __clusterd_libclusterctl_H__

#include <stdarg.h>
#include <sys/socket.h>

#define CLUSTERCTL_CLOSED       0
#define CLUSTERCTL_WAIT_COMMAND 1
#define CLUSTERCTL_WAIT_SCRIPT  2
#define CLUSTERCTL_WAIT_PARAMS  3
#define CLUSTERCTL_PROCESSING   4
#define CLUSTERCTL_ERROR        5

#define CLUSTERCTL_NEEDS_SCRIPT 1

#define CLUSTERCTL_INVALID_COMMAND 1
#define CLUSTERCTL_HASH_MISMATCH   2
#define CLUSTERCTL_BAD_SYNTAX      3
#define CLUSTERCTL_BAD_PARAM       4
#define CLUSTERCTL_OOM             5
#define CLUSTERCTL_UNKNOWN_ERR     6
#define CLUSTERCTL_PROTO           7
#define CLUSTERCTL_INVALID_PRAGMA  8
#define CLUSTERCTL_UNAVAILABLE     9

#define CLUSTERCTL_READ_DONE       0
#define CLUSTERCTL_READ_LINE       1
#define CLUSTERCTL_READ_ERROR      2
#define CLUSTERCTL_READ_TXFAILED   3

/**
 * Valid values for use_leader argument of clusterctl_begin_*
 */
typedef enum
  {
   CLUSTERCTL_STALE_READS = 0,
   CLUSTERCTL_CONSISTENT_READS,
   CLUSTERCTL_MAY_WRITE
  } clusterctl_tx_level;

#define CLUSTERCTL_MAX_ATTEMPTS    16

typedef struct {
  char line[16 * 1024];
  size_t linelen;

  struct sockaddr_storage attempt_addrs[CLUSTERCTL_MAX_ATTEMPTS];
  int attempts;

  int sk;
  int state;
  int error;
} clusterctl;

/**
 * Open a connection to the clusterd controller based on the contents
 * of the CLUSTERD_CONTROLLER environment variable. If use_leader is
 * given, then a connection is made to the clusterd leader. Otherwise,
 * if stale reads are acceptable, then a connection to any controller
 * is made.
 *
 * Returns -1 on error with errno set. Otherwise returns 0 and the
 * clusterctl parameter is initialized. Close it with
 * clusterctl_close().
 */
int clusterctl_open(clusterctl *c);

/**
 * Begin an invocation of an unnammed, non-persistent command. The
 * return value will be CLUSTERCTL_NEEDS_SCRIPT on success or a
 * negative number on error, and errno will be set.
 *
 * Use clusterctl_pipe_script() or clusterctl_upload_script() to set
 * the script
 */
int clusterctl_begin_custom(clusterctl *c, clusterctl_tx_level level);

/**
 * Begin an invocation of a named, persistent command. The return
 * value will be zero if the script already exists on the remote
 * server or CLUSTERCTL_NEEDS_SCRIPT if a script must be uploaded. On
 * error, a negative number will be returned and errno set
 */
int clusterctl_begin_named(clusterctl *c, const char *hash, clusterctl_tx_level level);

/**
 * Upload a script from a file descriptor
 */
int clusterctl_pipe_script(clusterctl *c, int fd);

/**
 * Upload part of a clusterctl script. To end the script, set the
 * script argument to NULL. If scriptlen is non-negative, it is used
 * as the length of the script string. Otherwise, strlen(script) is
 * used.
 *
 * Returns zero on success and negative on error (and errno will be
 * set)
 */
int clusterctl_upload_script(clusterctl *c, const char *script, ssize_t scriptlen);

/**
 * Invoke a named script or re-use it. Calculates the SHA256 hash automatically
 */
int clusterctl_start_script(clusterctl *c, clusterctl_tx_level lvl, const char *script);

/**
 * Read all output into buffer (or to nowhere, if null). Send error to stderr
 */
int clusterctl_read_all_output(clusterctl *c, char *output, size_t outputsz);

/**
 * Upload parameters specified as arguments
 */
int clusterctl_send_params(clusterctl *c, ...);

/**
 * Upload a newline delimited set of parameters. No checks are
 * performed to ensure the parameters are correctly formed.
 *
 * If paramslen is non-negative, then it's used as the length of
 * params. If negative, then strlen(params) is used.
 *
 * Returns 0 on success and a negative on error (and errno will be
 * set)
 */
int clusterctl_upload_params(clusterctl *c, const char *params, ssize_t paramslen);

/**
 * Upload simple name=value parameters one at a time. This is a
 * convenience wrapper around clusterctl_upload_params()
 *
 * Returns 0 on success and a negative on error (and errno will be
 * set)
 */
int clusterctl_set_param(clusterctl *c, const char *name, const char *value);

/**
 * Invoke the procedure after all parameters have been uploaded
 */
int clusterctl_invoke(clusterctl *c);

/**
 * Read a line of output from the cluster. Returns a negative number
 * if no line can be read, and errno will be set.
 *
 * If the command has finished, CLUSTERCTL_READ_DONE is returned. If
 * an output line has been read, CLUSTERDCTL_READ_LINE is returned. If
 * an error has occurred and the line is describing an error,
 * CLUSTERDCTL_READ_ERROR is returned (multiple error lines may be
 * returned).
 */
int clusterctl_read_output(clusterctl *c, char *linebuf, size_t *linesz);

/**
 * Perform (and save) the given lua script with some number of
 * parameters.
 *
 * Calls the interned script, uploads the script if need be, sets the
 * parameters, and invokes the script all in one go.
 *
 * Returns 0 if the script was invoked, or a negative number
 * otherwise, with errno set.
 */
int clusterctl_simple(clusterctl *c, clusterctl_tx_level lvl, const char *lua, ...);

/**
 * Same as clusterctl_simple() but accepting a va_list
 */
int clusterctl_simplev(clusterctl *c, clusterctl_tx_level lvl, const char *lua, va_list args);

/**
 * Flushes the output of the given clusterctl to the given file
 * descriptor.
 *
 * If no error lines were emitted, was_success is set to 1 (if it's
 * not NULL).
 *
 * Returns zero if lines were read successfully, even if they were all
 * error lines. Returns a negative number if there was a transport
 * error reading the lines, and errno is set appropriately.
 */
int clusterctl_flush_output(clusterctl *c, int out_fd, int err_fd,
                            int *was_success);

/**
 * Perform and save the given lua script and then return its entire
 * output (lines and all) in the given buffer. If the output does not
 * fit it is truncated, but output is always null terminated.
 */
int clusterctl_call_simple(clusterctl *c, clusterctl_tx_level lvl,
                           const char *lua, char *output, size_t outputsz,
                           ...);

/**
 * Close the given clusterctl structure
 */
void clusterctl_close(clusterctl *c);

#endif
