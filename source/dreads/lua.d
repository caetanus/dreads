module dreads.lua;

// Minimal extern(C) bindings against the system Lua 5.4 (liblua5.4.so).
// Only what dreads.scripting needs; declared nothrow because the Lua C API
// never throws D exceptions (errors are status codes or longjmp).

extern (C) nothrow @nogc:

struct lua_State;

alias lua_Alloc = void* function(void* ud, void* ptr, size_t osize, size_t nsize) nothrow;
alias lua_CFunction = int function(lua_State*) nothrow;

enum LUA_OK = 0;

enum LUA_TNIL = 0;
enum LUA_TBOOLEAN = 1;
enum LUA_TNUMBER = 3;
enum LUA_TSTRING = 4;
enum LUA_TTABLE = 5;

// LUAI_MAXSTACK (1_000_000) + 1000, as in lua 5.4's luaconf.h
enum LUA_REGISTRYINDEX = -1_001_000;

int lua_upvalueindex(int i)
{
    return LUA_REGISTRYINDEX - i;
}

lua_State* lua_newstate(lua_Alloc f, void* ud);
void lua_close(lua_State* L);
void luaL_openlibs(lua_State* L);

int luaL_loadbufferx(lua_State* L, const(char)* buff, size_t sz, const(char)* name, const(char)* mode);
int lua_pcallk(lua_State* L, int nargs, int nresults, int errfunc, ptrdiff_t ctx, void* k);

int lua_pcall(lua_State* L, int nargs, int nresults, int errfunc)
{
    return lua_pcallk(L, nargs, nresults, errfunc, 0, null);
}

int luaL_loadbuffer(lua_State* L, const(char)* buff, size_t sz, const(char)* name)
{
    return luaL_loadbufferx(L, buff, sz, name, null);
}

void lua_pushcclosure(lua_State* L, lua_CFunction fn, int n);
void lua_createtable(lua_State* L, int narr, int nrec);
void lua_pushlstring(lua_State* L, const(char)* s, size_t len);
void lua_pushinteger(lua_State* L, long n);
void lua_pushnumber(lua_State* L, double n);
void lua_pushboolean(lua_State* L, int b);
void lua_pushnil(lua_State* L);
void lua_pushlightuserdata(lua_State* L, void* p);

void lua_setglobal(lua_State* L, const(char)* name);
int lua_getglobal(lua_State* L, const(char)* name);
void lua_settop(lua_State* L, int idx);
int lua_gettop(lua_State* L);
int lua_type(lua_State* L, int idx);

const(char)* lua_tolstring(lua_State* L, int idx, size_t* len);
long lua_tointegerx(lua_State* L, int idx, int* isnum);
double lua_tonumberx(lua_State* L, int idx, int* isnum);
int lua_toboolean(lua_State* L, int idx);
void* lua_touserdata(lua_State* L, int idx);
ulong lua_rawlen(lua_State* L, int idx);

int lua_rawgeti(lua_State* L, int idx, long n);
void lua_rawseti(lua_State* L, int idx, long n);
int lua_getfield(lua_State* L, int idx, const(char)* k);
void lua_setfield(lua_State* L, int idx, const(char)* k);

int lua_error(lua_State* L);
int luaL_error(lua_State* L, const(char)* fmt, ...);

// selective library loading (sandbox)
void luaL_requiref(lua_State* L, const(char)* modname, lua_CFunction openf, int glb);
int luaopen_base(lua_State* L);
int luaopen_string(lua_State* L);
int luaopen_table(lua_State* L);
int luaopen_math(lua_State* L);

// raw global access (bypasses the _G protection metatable)
void lua_rawset(lua_State* L, int idx);
enum LUA_RIDX_GLOBALS = 2;

// per-run environment swap (upvalue 1 of a chunk is its _ENV)
const(char)* lua_setupvalue(lua_State* L, int funcindex, int n);
int lua_setmetatable(lua_State* L, int objindex);

// instruction-count hook (script time limit)
alias lua_Hook = void function(lua_State* L, void* ar) nothrow @nogc;
void lua_sethook(lua_State* L, lua_Hook f, int mask, int count);
enum LUA_MASKCOUNT = 1 << 3;
