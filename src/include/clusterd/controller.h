#ifndef __clusterd_controller_H__
#define __clusterd_controller_H__

#include <sqlite3.h>
#include <lua.h>

sqlite3 *clusterd_open_database(const char *path);
int clusterd_snapshot_database(const char *path);
int clusterd_restore_snapshot(const char *path, void *base, size_t len);

int luaopen_clusterd(lua_State *lua);

#endif
