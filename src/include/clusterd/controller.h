#ifndef __clusterd_controller_H__
#define __clusterd_controller_H__

#include <sqlite3.h>
#include <lua.h>

sqlite3 *clusterd_open_database(const char *path);

int luaopen_clusterd(lua_State *lua);

#endif
