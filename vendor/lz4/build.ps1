#!/usr/bin/env pwsh
# Build a static lz4 library from UPSTREAM source — PowerShell port of build.sh,
# so the vendored build works on Windows too. Cross-platform by design:
#   Windows  -> MSVC (cl + lib) -> liblz4.lib   (LDC/link.exe wants a .lib)
#   posix    -> cc + make       -> liblz4.a      (mirrors build.sh; testable here)
# Downloads the pristine upstream tarball (cached + sha256-verified), vendors no
# source. Idempotent: a no-op once the lib is newer than this script.
$ErrorActionPreference = 'Stop'

$Dir = $PSScriptRoot
$Ver = '1.10.0'
$Sha = '537512904744b35e232912055ccf8ec66d768639ff3abe5788d90d792ec5f48b'
$Url = "https://github.com/lz4/lz4/releases/download/v$Ver/lz4-$Ver.tar.gz"

$Cache   = Join-Path $Dir 'cache'
$Build   = Join-Path $Dir 'build'
$Tarball = Join-Path $Cache "lz4-$Ver.tar.gz"
$Src     = Join-Path $Build "lz4-$Ver"
$LibName = if ($IsWindows) { 'liblz4.lib' } else { 'liblz4.a' }
$Lib     = Join-Path $Build $LibName

# up to date? (lib newer than this script)
if ((Test-Path $Lib) -and (Get-Item $Lib).LastWriteTime -gt (Get-Item $PSCommandPath).LastWriteTime) {
    exit 0
}

New-Item -ItemType Directory -Force -Path $Cache, $Build | Out-Null

# download once, verify every time
if (-not (Test-Path $Tarball)) {
    Invoke-WebRequest -Uri $Url -OutFile "$Tarball.tmp"
    Move-Item -Force "$Tarball.tmp" $Tarball
}
$got = (Get-FileHash -Algorithm SHA256 $Tarball).Hash.ToLower()
if ($got -ne $Sha) { throw "lz4 tarball sha256 mismatch: $got != $Sha" }

# fresh extract
if (Test-Path $Src) { Remove-Item -Recurse -Force $Src }
tar xzf $Tarball -C $Build

$libdir = Join-Path $Src 'lib'
if ($IsWindows) {
    # MSVC: compile the block/frame sources into one static archive.
    Push-Location $libdir
    try {
        & cl /nologo /c /O2 /DNDEBUG lz4.c lz4hc.c lz4frame.c xxhash.c
        if ($LASTEXITCODE -ne 0) { throw 'cl compile failed' }
        & lib /nologo "/OUT:$Lib" *.obj
        if ($LASTEXITCODE -ne 0) { throw 'lib archive failed' }
    } finally { Pop-Location }
} else {
    # posix (mirrors build.sh): -fPIC so it links into a PIE.
    $cc = if ($env:CC) { $env:CC } else { 'cc' }
    & make -C $libdir liblz4.a "CC=$cc" 'CFLAGS=-O3 -fPIC' | Out-Null
    if ($LASTEXITCODE -ne 0) { throw 'make failed' }
    Copy-Item -Force (Join-Path $libdir 'liblz4.a') $Lib
}
Write-Host "built $Lib (lz4 $Ver)"
