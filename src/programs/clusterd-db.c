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

#define CLUSTERD_COMPONENT "controller"
#include "clusterd/log.h"
#include "clusterd/controller.h"
#include "clusterd/common.h"

#include <lauxlib.h>
#include <raft.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>

extern int CLUSTERD_LOG_LEVEL;
extern const char rc_controller_schema_sql[];
extern const char rc_controller_api_lua[];

extern sqlite3 *g_database;

struct raft *lua_tx_raft = NULL;

static int g_in_transaction = 0;
int g_writes_allowed = 0;

// Management of all commands in the transaction
static struct raft_buffer g_transaction;

static int g_allow_write;
static struct raft_apply g_raft_apply;

// Raft transaction handling

#define CLUSTERD_TX_VERSION       1

void clusterd_next_write(int status, void *result);

static int clusterd_tx_ensure_transaction() {
  int err;

  if ( g_transaction.base ) return 0;

  err = snprintf(NULL, 0, "--%u\n", CLUSTERD_TX_VERSION);
  if ( err < 0 ) {
    CLUSTERD_LOG(CLUSTERD_CRIT, "Could not format Tx version: %s", strerror(errno));
    return -1;
  }

  g_transaction.len = err;
  g_transaction.base = raft_malloc(g_transaction.len + 1); // Snprintf truncates its output
  if ( !g_transaction.base ) {
    g_transaction.len = 0;
    errno = ENOMEM;
    return -1;
  }

  snprintf(g_transaction.base, g_transaction.len + 1, "--%u\n", CLUSTERD_TX_VERSION);
  return 0;
}

static void clusterd_tx_add(lua_State *lua, sqlite3_stmt *stmt) {
  ssize_t cmdsz;
  int err;
  char *sstmt, *newbase;

  err = clusterd_tx_ensure_transaction();
  if ( err < 0 ) {
    err = sqlite3_finalize(stmt);
    if ( err != SQLITE_OK ) {
      luaL_error(lua, "Could not finalize statement in error (out of memory)");
    }

    luaL_error(lua, "Out of memory");
  }

  sstmt = sqlite3_expanded_sql(stmt);
  if ( !sstmt ) {
    err = sqlite3_finalize(stmt);
    if ( err != SQLITE_OK )
      luaL_error(lua, "Could not finalize statement in error");
    luaL_error(lua, "Could not expand SQL");
  }

  err = sqlite3_finalize(stmt);
  if ( err != SQLITE_OK ) {
    sqlite3_free(sstmt);
    luaL_error(lua, "Could not finalize statement");
  }

  cmdsz = g_transaction.len + strlen(sstmt) + 2; // Two extra bytes, one semicolon, one newline
  newbase = raft_realloc(g_transaction.base, cmdsz);
  if ( !newbase ) {
    sqlite3_free(sstmt);
    luaL_error(lua, "Could not add tx entry: out of memory");
  }

  g_transaction.base = newbase;
  memcpy(g_transaction.base + g_transaction.len, sstmt, strlen(sstmt));
  memcpy(g_transaction.base + cmdsz - 2, ";\n", 2);

  g_transaction.len = cmdsz;

  sqlite3_free(sstmt);
}

void clusterd_tx_free() {
  int i;

  if ( g_transaction.base ) {
    free(g_transaction.base);
  }

  g_transaction.len = 0;
  g_transaction.base = NULL;
}

static void clusterd_tx_apply_complete(struct raft_apply *req, int status, void *result) {
  CLUSTERD_LOG(CLUSTERD_DEBUG, "Raft application complete: %d", status);

  if ( status != 0 ) {
    CLUSTERD_LOG(CLUSTERD_ERROR, "Raft apply error: %s", raft_errmsg(lua_tx_raft));

    if ( status == RAFT_LEADERSHIPLOST ) {
      CLUSTERD_LOG(CLUSTERD_INFO, "Leadership lost. Will send notice to client");
    } else {
      CLUSTERD_LOG(CLUSTERD_DEBUG, "This will cause the abort of the transaction");
    }
  }

  clusterd_next_write(status, result);
}

int clusterd_pad_transaction() {
  size_t padded, i;
  char *new_base;

  padded = ((g_transaction.len + 7) / 8) * 8;
  new_base = raft_realloc(g_transaction.base, padded);
  if ( !new_base ) {
    errno = ENOMEM;
    return -1;
  }

  for ( i = g_transaction.len; i < padded; ++i )
    new_base[i] = ' ';

  g_transaction.base = new_base;
  g_transaction.len = padded;

  return 0;
}

int clusterd_tx_commit() {
  int entries_to_dismiss, err, i;

  if ( !g_transaction.base ) {
    // No data manipulation statements, the TX was read-only. return
    return 0;
  } else {

    CLUSTERD_LOG(CLUSTERD_DEBUG, "applying transaction to raft: %.*s (length=%zd)", (int)g_transaction.len, (char *)g_transaction.base, g_transaction.len);

    // Round up to 8 bytes, and pad with spaces
    err = clusterd_pad_transaction();
    if ( err < 0 ) {
      errno = ENOMEM;
      return -1;
    }

    // We only have one application going on at a time. If we're here, the request can be used
    err = raft_apply(lua_tx_raft, &g_raft_apply, &g_transaction, 1, clusterd_tx_apply_complete);
    if ( err != 0 ) {
      CLUSTERD_LOG(CLUSTERD_CRIT, "Could not submit transaction entries to raft: %s",
                   raft_errmsg(lua_tx_raft));
      clusterd_tx_free();

      errno = EREMOTE;
      return -1;
    }

    g_transaction.base = NULL;
    g_transaction.len = 0;

    // Raft quorum is asynchronous
    errno = EAGAIN;
    return -1;
  }
}

static int db_schema_cb(void *ud, int ncols, char **vals, char **cols) {
  *(int*)ud = 1;
  return SQLITE_OK;
}

static sqlite3 *clusterd_do_open(const char *path) {
  sqlite3 *db;
  char *errmsg;
  int err, had_rows = 0;

  err = sqlite3_open(path, &db);
  if ( err != SQLITE_OK ) {
    CLUSTERD_LOG(CLUSTERD_CRIT, "Could not open database: %s: %s", path, sqlite3_errstr(err));
    return NULL;
  }

  err = sqlite3_exec(db, "PRAGMA foreign_keys=ON; BEGIN;", db_schema_cb, (void *)&had_rows, &errmsg);
  if ( err != SQLITE_OK ) {
    CLUSTERD_LOG(CLUSTERD_CRIT, "Could not begin DDL transaction: %s", errmsg);
    sqlite3_free(errmsg);
    sqlite3_close(db);
    return NULL;
  }

  err = sqlite3_exec(db, rc_controller_schema_sql,
                     db_schema_cb, (void *)&had_rows,
                     &errmsg);
  if ( err != SQLITE_OK ) {
    CLUSTERD_LOG(CLUSTERD_CRIT, "Could not apply latest schema: %s", errmsg);
    sqlite3_free(errmsg);
    sqlite3_close(db);
    return NULL;
  }

  if ( had_rows ) {
    CLUSTERD_LOG(CLUSTERD_CRIT, "Schema change returned rows");
    sqlite3_close(db);
    return NULL;
  }

  err = sqlite3_exec(db, "COMMIT;", db_schema_cb, (void *)&had_rows,
                     &errmsg);
  if ( err != SQLITE_OK ) {
    CLUSTERD_LOG(CLUSTERD_CRIT, "Could not apply schema change: %s", errmsg);
    sqlite3_free(errmsg);
    sqlite3_close(db);
    return NULL;
  }

  return db;
}


sqlite3 *clusterd_open_database(const char *path) {
  int err;

  // If the path already exists, delete it. We trust raft to restore from snapshot
  err = unlink(path);
  if ( err < 0 ) {
    if ( errno != ENOENT ) {
      CLUSTERD_LOG(CLUSTERD_WARNING, "Could not unlink %s: %s", path, strerror(errno));
    }
  }

  return clusterd_do_open(path);
}

static int do_vacuum(const char *backup_path) {
  char query_buf[PATH_MAX * 2];
  int err, had_rows = 0;
  char *errmsg = NULL;

  err = snprintf(query_buf, sizeof(query_buf),
                 "VACUUM INTO '%s';", backup_path);
  if ( err >= sizeof(query_buf) ) {
    errno = ENAMETOOLONG;
    return -1;
  }

  CLUSTERD_LOG(CLUSTERD_DEBUG, "Vacuuming: %s", query_buf);

  err = sqlite3_exec(g_database, query_buf, db_schema_cb, (void *)&had_rows, &errmsg);
  if ( err != SQLITE_OK ) {
    CLUSTERD_LOG(CLUSTERD_CRIT, "could not vacuum database: %s", errmsg);
    sqlite3_free(errmsg);
    return -1;
  } else if ( had_rows ) {
    CLUSTERD_LOG(CLUSTERD_WARNING, "VACUUM INTO statement contained %d rows", had_rows);
  }

  CLUSTERD_LOG(CLUSTERD_DEBUG, "Vacuumed");

  return 0;
}

int clusterd_snapshot_database(const char *main_path) {
  int err;
  char backup_path[PATH_MAX];

  err = snprintf(backup_path, sizeof(backup_path), "%s.snap", main_path);
  if ( err >= sizeof(backup_path) ) {
    errno = ENAMETOOLONG;
    return -1;
  }

  CLUSTERD_LOG(CLUSTERD_DEBUG, "Snapshotting to %s", backup_path);

  err = access(backup_path, R_OK | W_OK);
  if ( err < 0 && errno != ENOENT ) {
    CLUSTERD_LOG(CLUSTERD_ERROR, "Cannot snapshot database. Can't access backup file: %s", strerror(errno));
    return -1;
  } else if ( err == 0 ) {
    CLUSTERD_LOG(CLUSTERD_DEBUG, "Found existing snapshot at %s. Removing", backup_path);

    err = unlink(backup_path);
    if ( err < 0 ) {
      CLUSTERD_LOG(CLUSTERD_ERROR, "Could not remove database at %s: %s", backup_path, strerror(errno));
      return -1;
    }
  }

  CLUSTERD_LOG(CLUSTERD_DEBUG, "Goign to vacuum");

  // Issue vacuum into command
  err = do_vacuum(backup_path);
  if ( err < 0 )
    return -1;

  CLUSTERD_LOG(CLUSTERD_DEBUG, "Re-opening database");

  // Now close the original database, rename the backup_path to
  // main_path, and then re-open the database at main_path.
  err = sqlite3_close(g_database);
  if ( err != SQLITE_OK ) {
    CLUSTERD_LOG(CLUSTERD_CRIT, "Could not close sqlite database: %s", sqlite3_errstr(err));

    errno = EBUSY;
    return -1;
  }

  g_database = NULL;

  err = rename(backup_path, main_path);
  if ( err < 0 ) {
    CLUSTERD_LOG(CLUSTERD_CRIT, "Could not rename backup to new path");
    return -1;
  }


  CLUSTERD_LOG(CLUSTERD_DEBUG, "Going to reset g_database");

  // Now re-open the database
  g_database = clusterd_do_open(main_path);
  if ( !g_database )
    return -1;

  return 0;
}

int clusterd_restore_snapshot(const char *file, void *base, size_t len) {
  char restore_file[PATH_MAX];
  int err, fd;

  err = snprintf(restore_file, sizeof(restore_file), "%s.restore", file);
  if ( err >= sizeof(restore_file) ) {
    errno = ENAMETOOLONG;
    return -1;
  }

  fd = open(restore_file, O_RDWR | O_CREAT | O_TRUNC, S_IWUSR | S_IRUSR);
  if ( fd < 0 ) {
    CLUSTERD_LOG(CLUSTERD_ERROR, "Could not open restore file %s: %s", restore_file, strerror(errno));
    return -1;
  }

  err = ftruncate(fd, len);
  if ( err < 0 ) {
    int serrno = errno;
    CLUSTERD_LOG(CLUSTERD_ERROR, "Could not truncate file %s to %zd: %s",
                 restore_file, len, strerror(errno));
    close(fd);
    errno = serrno;
    return -1;
  }

  err = pwrite(fd, base, len, 0);
  if ( err < 0 ) {
    int serrno = errno;
    CLUSTERD_LOG(CLUSTERD_ERROR, "Could not write snapshot file %s: %s", restore_file, strerror(errno));
    errno = serrno;
    return -1;
  }

  close(fd);

  err = sqlite3_close(g_database);
  if ( err != SQLITE_OK ) {
    CLUSTERD_LOG(CLUSTERD_CRIT, "Could not close database during restore: %s", sqlite3_errstr(err));

    errno = EBUSY;
    return -1;
  }

  g_database = NULL;

  err = rename(restore_file, file);
  if ( err < 0 ) {
    CLUSTERD_LOG(CLUSTERD_CRIT, "Could not restore file to main database during move: %s", strerror(errno));
    return -1;
  }

  g_database = clusterd_do_open(file);
  if ( !g_database )
    return -1;

  return 0;
}

static void sqlite3_lua_error(lua_State *lua, char *error, const char *what) {
  char error_string[1024];
  int err;

  err = snprintf(error_string, sizeof(error_string), "%s", error);
  error_string[1023] = '\0';

  sqlite3_free(error);

  luaL_error(lua, "Could not execute sql: %s: %s", what, error);
}

int clusterd_apply_tx(char *sql, size_t sqlsz, char **errmsg) {
  int err;
  char *rbmsg;

  *errmsg = NULL;

  err = sqlite3_exec(g_database, "BEGIN;", NULL, NULL, errmsg);
  if ( err != SQLITE_OK )
    return -1;

  if ( sql[sqlsz - 1] != '\0' )
    sql[sqlsz - 1] = '\0';

  err = sqlite3_exec(g_database, sql, NULL, NULL, errmsg);
  if ( err != SQLITE_OK )
    goto abort;

  err = sqlite3_exec(g_database, "COMMIT;", NULL, NULL, errmsg);
  if ( err != SQLITE_OK )
    goto abort;

  *errmsg = NULL;
  return 0;

 abort:
  err = sqlite3_exec(g_database, "ROLLBACK", NULL, NULL, &rbmsg);
  if ( err != SQLITE_OK ) {
    CLUSTERD_LOG(CLUSTERD_ERROR, "Could not rollback transaction: %s", rbmsg);
    return -1;
  }

  return -1;
}

int clusterd_begin(char **errmsg, int writable) {
  int err, had_rows = 0;

  g_allow_write = 0;
  err = sqlite3_exec(g_database, "BEGIN;", db_schema_cb, (void *)&had_rows, errmsg);
  if ( err != SQLITE_OK ) {
    return -1;
  } else {
    g_allow_write = writable;
    if ( g_allow_write ) {
      g_transaction.len = 0;
      g_transaction.base = NULL;
    }
    return 0;
  }
}

int clusterd_rollback() {
  int err, had_rows = 0;
  char *errmsg;

  err = sqlite3_exec(g_database, "ROLLBACK;", db_schema_cb, (void *)&had_rows, &errmsg);
  if ( err < 0 ) {
    CLUSTERD_LOG(CLUSTERD_ERROR, "Could not rollback: %s", errmsg);
  }

  clusterd_tx_free();

  if ( err != SQLITE_OK ) return -1;
  else return 0;
}

int clusterd_commit(char **errmsg) {
  int err, had_rows = 0;

  err = sqlite3_exec(g_database, "COMMIT;", db_schema_cb, (void *)&had_rows, errmsg);
  if ( err != SQLITE_OK ) {
    return -1;
  } else
    return 0;
}

static int clusterd_lua_run(lua_State *lua) {
  const char *sql;
  int nargs, nparams, err, i, nrow = 0, raft_replicated = 0;
  sqlite3_stmt *stmt = NULL;

  nargs = lua_gettop(lua);
  if ( nargs == 0 )
    return luaL_error(lua, "internal.run must be called with at least a SQL statement");
  else if ( nargs > 2 )
    return luaL_error(lua, "internal.run expects two arguments, the SQL statement and the parameters");

  sql = lua_tostring(lua, 1);
  if ( !sql )
    return luaL_error(lua, "internal.run must be called with a statement and optional parameters");

  err = sqlite3_prepare_v2(g_database, sql, -1, &stmt, NULL);
  if ( err != SQLITE_OK )
    goto sqlite_error;

  if ( !sqlite3_stmt_readonly(stmt) ) {
    CLUSTERD_LOG(CLUSTERD_DEBUG, "attempt to run update statement on state %d", raft_state(lua_tx_raft));
    if ( !g_allow_write ) {
      sqlite3_finalize(stmt);
      return luaL_error(lua, "Cannot run update statements on follower node");
    }

    raft_replicated = 1;
  }

  nparams = sqlite3_bind_parameter_count(stmt);
  if ( nparams > 0 && nargs == 1 ) {
    lua_pushstring(lua, "Statement contains parameters, but no parameters given");
    goto call_error;
  }

  for ( i = 0; i < nparams; ++i ) {
    int param = i + 1;

    const char *nm = sqlite3_bind_parameter_name(stmt, param);
    if ( !nm ) {
      lua_pushstring(lua, "Statement contains nameless parameters");
      goto call_error;
    }

    nm++; // Ignore name sigil

    lua_pushstring(lua, nm);
    lua_gettable(lua, 2);

    switch ( lua_type(lua, -1) ) {
    case LUA_TNONE:
      lua_pushstring(lua, "Internal error");
      goto call_error;

    case LUA_TNIL:
      err = sqlite3_bind_null(stmt, param);
      break;

    case LUA_TNUMBER:
      if ( lua_isinteger(lua, -1) ) {
        err = sqlite3_bind_int64(stmt, param, lua_tointeger(lua, -1));
      } else {
        err = sqlite3_bind_double(stmt, param, lua_tonumber(lua, -1));
      }
      break;

    case LUA_TBOOLEAN:
      err = sqlite3_bind_int64(stmt, param, lua_toboolean(lua, -1));
      break;

    case LUA_TSTRING:
      // Could be blob or string. If the name is prefixed with bin_
      // then it's a blob
      if ( strlen(nm) >= 4 &&
           memcmp(nm, "bin_", 4) == 0 ) {
        size_t nbytes;
        const char *luabytes;
        char *bytes;

        luabytes = lua_tolstring(lua, -1, &nbytes);

        bytes = malloc(nbytes);
        if ( !bytes ) {
          sqlite3_finalize(stmt);
          return luaL_error(lua, "Out of memory");
        }

        memcpy(bytes, luabytes, nbytes);

        err = sqlite3_bind_blob(stmt, param, bytes, nbytes, free);
      } else {
        const char *luastr = lua_tostring(lua, -1);
        char *str = malloc(strlen(luastr) + 1);
        if ( !str ) {
          sqlite3_finalize(stmt);
          return luaL_error(lua, "Out of memory");
        }

        strcpy(str, luastr);

        err = sqlite3_bind_text(stmt, param, str, -1, free);
      }

      break;

    default:
      lua_pushstring(lua, "Invalid type ");
      lua_pushstring(lua, lua_typename(lua, lua_type(lua, -1)));
      lua_pushstring(lua, " for sql parameter ");
      lua_pushstring(lua, nm);
      lua_concat(lua, 4);
      goto call_error;
    }

    if ( err != SQLITE_OK )
      goto sqlite_error;
  }

  lua_newtable(lua); // Results

  nrow = 0;
  while ( (err = sqlite3_step(stmt)) == SQLITE_ROW ) {
    int ncol = sqlite3_column_count(stmt);

    lua_newtable(lua); // Row
    for ( i = 0; i < ncol; ++i ) {
      lua_pushstring(lua, sqlite3_column_name(stmt, i));
      switch ( sqlite3_column_type(stmt, i) ) {
      case SQLITE_INTEGER:
        lua_pushinteger(lua, sqlite3_column_int64(stmt, i));
        break;

      case SQLITE_FLOAT:
        lua_pushnumber(lua, sqlite3_column_double(stmt, i));
        break;

      case SQLITE_BLOB:
        lua_pushlstring(lua, sqlite3_column_blob(stmt, i), sqlite3_column_bytes(stmt, i));
        break;

      case SQLITE_NULL:
        lua_pushnil(lua);
        break;

      case SQLITE_TEXT:
        lua_pushlstring(lua, sqlite3_column_text(stmt, i), sqlite3_column_bytes(stmt, i));
        break;

      default:
        lua_pushstring(lua, "Bad SQLite data type returned");
        goto call_error;
      }

      // Stack is now name, value
      lua_settable(lua, -3);
    }

    // Stack is now results, row
    lua_seti(lua, -2, nrow + 1);
    nrow++;
  }

  CLUSTERD_LOG(CLUSTERD_DEBUG, "Query complete");

  if ( err != SQLITE_DONE )
    goto sqlite_error;

  if ( raft_replicated )
    clusterd_tx_add(lua, stmt);
  else {
    err = sqlite3_finalize(stmt);
    if ( err != SQLITE_OK ) {
      return luaL_error(lua, "Could not finalize statement");
    }
  }

  lua_pushnil(lua); // No error

  return 2;

 sqlite_error:
  lua_pushstring(lua, sqlite3_errmsg(g_database));

 call_error:
  if ( stmt )
    sqlite3_finalize(stmt);

  // Error message is at top of stack
  lua_pushnil(lua);
  lua_rotate(lua, -2, 1);

  // Now it's result(nil) and error
  return 2;
}

static int clusterd_lua_log(lua_State *lua) {
  int level;
  const char *message;

  if ( lua_gettop(lua) != 2 || !lua_isnumber(lua, -2) || !lua_isstring(lua, -1) )
    return luaL_error(lua, "internal.log must be called with a level and a string");

  level = lua_tonumber(lua, -2);
  message = lua_tostring(lua, -1);

  CLUSTERD_LOG(level, "%s", message);

  return 0;
}

static int clusterd_lua_is_valid_ip(lua_State *lua) {
  int nargs;
  const char *ip;
  struct sockaddr_storage ss;

  nargs = lua_gettop(lua);
  if ( nargs != 1 )
    return luaL_error(lua, "internal.is_valid_ip expects one argument");

  ip = lua_tostring(lua, 1);
  if ( !ip )
    return luaL_error(lua, "internal.is_valid_ip expects a string");

  if ( clusterd_addr_parse(ip, &ss, 0) < 0 )
    lua_pushnumber(lua, 0);
  else
    lua_pushnumber(lua, 1);

  return 1;
}

#define REGISTER_FUNC(name, func)               \
  lua_pushcfunction(lua, (func));               \
  lua_setfield(lua, -2, (name));
#define REGISTER_INT(name, i)               \
  lua_pushnumber(lua, (i));                 \
  lua_setfield(lua, -2, (name));
int luaopen_clusterd(lua_State *lua) {
  int err;

  lua_newtable(lua);

  REGISTER_FUNC("log", clusterd_lua_log);
  REGISTER_INT("debug", CLUSTERD_DEBUG);
  REGISTER_INT("info", CLUSTERD_INFO);
  REGISTER_INT("warning", CLUSTERD_WARNING);
  REGISTER_INT("error", CLUSTERD_ERROR);
  REGISTER_INT("crit", CLUSTERD_CRIT);

  REGISTER_FUNC("run", clusterd_lua_run);
  REGISTER_FUNC("is_valid_ip", clusterd_lua_is_valid_ip);

  lua_setglobal(lua, "internal");

  err = luaL_dostring(lua, rc_controller_api_lua);
  if ( err != LUA_OK ) {
    lua_Debug tb;
    if ( lua_getstack(lua, 0, &tb) == 0 ) {
      CLUSTERD_LOG(CLUSTERD_CRIT, "Failure opening internal lua module: couldn't get stack");
    } else {
      err =  lua_getinfo(lua, "nSl", &tb);
      if ( err != LUA_OK ) {
        CLUSTERD_LOG(CLUSTERD_CRIT, "Failure opening internal lua module: could not get debug structure");
      } else {
        CLUSTERD_LOG(CLUSTERD_CRIT, "Lua error: %s (%s): %s: %d: %s",
                     tb.name, tb.namewhat, tb.short_src, tb.currentline, tb.what);
      }
    }

    if ( lua_isstring(lua, -1) ) {
      CLUSTERD_LOG(CLUSTERD_CRIT, "Reported error: %s", lua_tostring(lua, -1));
    }
    lua_pop(lua, 1);
    lua_pushnil(lua);
  }

  lua_pushnil(lua);
  lua_setglobal(lua, "internal");

  return 1;
}
