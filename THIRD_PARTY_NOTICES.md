# Third-Party Notices

This project (`dreads`) is an independent, from-scratch implementation. It is
not a fork of Redis or Valkey and shares no source code with them, with one
deliberate, narrowly-scoped exception documented below.

## Valkey — error message strings

For wire-protocol compatibility, `dreads` reproduces a number of client-facing
**error message strings** (e.g. `ERR value is not an integer or out of range`,
`WRONGTYPE Operation against a key holding the wrong kind of value`) verbatim
from Valkey so that existing Redis/Valkey clients and tests observe identical
error text. These strings are the only material derived from Valkey; no Valkey
source code, structure, or logic was copied.

Valkey is distributed under the BSD 3-Clause License, which permits this use
provided the following copyright notice, conditions, and disclaimer are
retained:

```
BSD 3-Clause License

Copyright (c) 2024-present, Valkey contributors
Copyright (c) 2006-2020, Redis Ltd.
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:

1. Redistributions of source code must retain the above copyright notice,
   this list of conditions and the following disclaimer.

2. Redistributions in binary form must reproduce the above copyright notice,
   this list of conditions and the following disclaimer in the documentation
   and/or other materials provided with the distribution.

3. Neither the name of the copyright holder nor the names of its
   contributors may be used to endorse or promote products derived from
   this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
```

Upstream: https://github.com/valkey-io/valkey (BSD 3-Clause).
