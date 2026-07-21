#!/usr/bin/env pwsh
# Build a static liblua.a/.lib from UPSTREAM Lua + dreads' read-only patch —
# PowerShell port of build.sh so the vendored build works on Windows too.
# Cross-platform:
#   Windows -> MSVC (cl + lib)      -> liblua.lib
#   posix   -> cc + make liblua.a   -> liblua.a   (mirrors build.sh; testable here)
# The repo vendors NO Lua source — only our patch. This downloads the pristine
# upstream tarball (cached + sha256-verified), applies the patch (git apply, so
# no Unix `patch` is needed on Windows), and builds only the static library.
$ErrorActionPreference = 'Stop'

$Dir   = $PSScriptRoot
$Ver   = '5.4.8'
$Sha   = '4f18ddae154e793e46eeab727c59ef1c0c0c2b744e7b94219710d76f530629ae'
$Url   = "https://www.lua.org/ftp/lua-$Ver.tar.gz"
$Patch = Join-Path $Dir "dreads-readonly-$Ver.patch"

$Cache   = Join-Path $Dir 'cache'
$Build   = Join-Path $Dir 'build'
$Tarball = Join-Path $Cache "lua-$Ver.tar.gz"
$Src     = Join-Path $Build "lua-$Ver"
$LibName = if ($IsWindows) { 'liblua.lib' } else { 'liblua.a' }
$Lib     = Join-Path $Build $LibName

# up to date? (lib newer than both the patch and this script)
if ((Test-Path $Lib) `
        -and (Get-Item $Lib).LastWriteTime -gt (Get-Item $Patch).LastWriteTime `
        -and (Get-Item $Lib).LastWriteTime -gt (Get-Item $PSCommandPath).LastWriteTime) {
    exit 0
}

New-Item -ItemType Directory -Force -Path $Cache, $Build | Out-Null

# download once, verify every time
if (-not (Test-Path $Tarball)) {
    Invoke-WebRequest -Uri $Url -OutFile "$Tarball.tmp"
    Move-Item -Force "$Tarball.tmp" $Tarball
}
$got = (Get-FileHash -Algorithm SHA256 $Tarball).Hash.ToLower()
if ($got -ne $Sha) { throw "lua tarball sha256 mismatch: $got != $Sha" }

# fresh extract + patch (git apply is portable; -p1 matches build.sh's `patch -p1`)
if (Test-Path $Src) { Remove-Item -Recurse -Force $Src }
tar xzf $Tarball -C $Build
Push-Location (Join-Path $Src 'src')
try {
    # -c core.autocrlf=false + --ignore-whitespace: belt-and-suspenders against
    # a CRLF-converted patch/source EOL mismatch on Windows (the .patch is also
    # pinned to LF via .gitattributes).
    & git -c core.autocrlf=false apply --ignore-whitespace -p1 --unsafe-paths $Patch
    if ($LASTEXITCODE -ne 0) { throw 'patch (git apply) failed' }
} finally { Pop-Location }

$srcdir = Join-Path $Src 'src'
if ($IsWindows) {
    # MSVC: compile every core/lib .c EXCEPT the interpreter/compiler mains.
    Push-Location $srcdir
    try {
        $objs = Get-ChildItem *.c | Where-Object { $_.Name -notin @('lua.c', 'luac.c') }
        & cl /nologo /c /O2 /DNDEBUG @($objs.Name)
        if ($LASTEXITCODE -ne 0) { throw 'cl compile failed' }
        & lib /nologo "/OUT:$Lib" *.obj
        if ($LASTEXITCODE -ne 0) { throw 'lib archive failed' }
    } finally { Pop-Location }
} else {
    # posix (mirrors build.sh): the platform macro follows the OS.
    $cc  = if ($env:CC) { $env:CC } else { 'cc' }
    $plat = if ($IsMacOS) { '-DLUA_USE_MACOSX' } else { '-DLUA_USE_LINUX' }
    & make -C $srcdir liblua.a "CC=$cc" "MYCFLAGS=-O2 -fPIC $plat" 'MYLIBS=' | Out-Null
    if ($LASTEXITCODE -ne 0) { throw 'make failed' }
    Copy-Item -Force (Join-Path $srcdir 'liblua.a') $Lib
}
Write-Host "built $Lib (Lua $Ver + dreads read-only patch)"
