import { render } from 'preact'
import { useState, useEffect, useRef, useCallback } from 'preact/hooks'
import uPlot from 'uplot'
import 'uplot/dist/uPlot.min.css'
import { CodeJar } from 'codejar'
import Prism from 'prismjs'
import 'prismjs/components/prism-lua'
import './style.css'

const WINDOW = 240
const enc = new TextEncoder()

// parse "512mb" / "2gb" / "1048576" / "0" (unlimited) -> bytes, or null if bad
function parseBytes(s) {
  const m = String(s).trim().toLowerCase().match(/^(\d+(?:\.\d+)?)\s*(b|kb|mb|gb|k|m|g)?$/)
  if (!m) return null
  const mult = { '': 1, b: 1, k: 1024, kb: 1024, m: 1048576, mb: 1048576, g: 1073741824, gb: 1073741824 }
  return Math.round(parseFloat(m[1]) * mult[m[2] || ''])
}
const fmtBytes = (n) => n >= 1073741824 ? (n / 1073741824).toFixed(2) + ' GB'
  : n >= 1048576 ? (n / 1048576).toFixed(1) + ' MB' : n >= 1024 ? (n / 1024).toFixed(0) + ' KB' : n + ' B'

// ---- themes: a set of CSS variables; built-ins + custom ones stored in the dreads keyspace ----
const THEME_VARS = ['--bg', '--panel', '--panel2', '--edge', '--fg', '--fg2', '--dim', '--dim2',
  '--accent', '--accent2', '--ok', '--bad', '--warn', '--sel']
const THEMES = {
  default: { '--bg': '#0b0e14', '--panel': '#141b26', '--panel2': '#0d1219', '--edge': '#1f2a3a', '--fg': '#c9d1d9', '--fg2': '#adbac7', '--dim': '#7d8aa0', '--dim2': '#5c6b82', '--accent': '#2f81f7', '--accent2': '#79b8ff', '--ok': '#3fb950', '--bad': '#f85149', '--warn': '#db6d28', '--sel': '#101c2e' },
  desert: { '--bg': '#2b2416', '--panel': '#3a3120', '--panel2': '#241e12', '--edge': '#4a3f28', '--fg': '#ece0c8', '--fg2': '#d8c9a8', '--dim': '#a89878', '--dim2': '#7a6d52', '--accent': '#cf8b3b', '--accent2': '#e0a458', '--ok': '#8faa4a', '--bad': '#c65d3b', '--warn': '#d29922', '--sel': '#3f3620' },
  solarized: { '--bg': '#002b36', '--panel': '#073642', '--panel2': '#00252e', '--edge': '#0f4b58', '--fg': '#93a1a1', '--fg2': '#839496', '--dim': '#657b83', '--dim2': '#475a60', '--accent': '#268bd2', '--accent2': '#2aa198', '--ok': '#859900', '--bad': '#dc322f', '--warn': '#cb4b16', '--sel': '#0a4453' },
  mocha: { '--bg': '#1e1e2e', '--panel': '#28283c', '--panel2': '#181825', '--edge': '#313244', '--fg': '#cdd6f4', '--fg2': '#bac2de', '--dim': '#a6adc8', '--dim2': '#6c7086', '--accent': '#89b4fa', '--accent2': '#b4befe', '--ok': '#a6e3a1', '--bad': '#f38ba8', '--warn': '#fab387', '--sel': '#313244' },
  dracula: { '--bg': '#282a36', '--panel': '#343746', '--panel2': '#21222c', '--edge': '#44475a', '--fg': '#f8f8f2', '--fg2': '#e0e0e0', '--dim': '#8a92b8', '--dim2': '#6272a4', '--accent': '#bd93f9', '--accent2': '#8be9fd', '--ok': '#50fa7b', '--bad': '#ff5555', '--warn': '#ffb86c', '--sel': '#3c3f52' },
}
function applyTheme(vars) {
  const root = document.documentElement
  THEME_VARS.forEach((k) => { if (vars && vars[k]) root.style.setProperty(k, vars[k]) })
}

const fmt = (n) => {
  n = n || 0
  if (n >= 1e9) return (n / 1e9).toFixed(1) + 'G'
  if (n >= 1e6) return (n / 1e6).toFixed(1) + 'M'
  if (n >= 1e3) return (n / 1e3).toFixed(1) + 'k'
  return '' + Math.round(n)
}
const bytes = (n) => {
  n = n || 0
  const u = ['B', 'KB', 'MB', 'GB', 'TB']
  let i = 0
  while (n >= 1024 && i < 4) { n /= 1024; i++ }
  return n.toFixed(i ? 1 : 0) + u[i]
}

// --- redis-cli-style tokenizer: splits a line honouring single/double quotes ---
function tokenize(line) {
  const out = []
  let i = 0, cur = '', q = 0, has = false
  while (i < line.length) {
    const c = line[i]
    if (q) {
      if (c === q) { q = 0 }
      else if (c === '\\' && q === '"' && i + 1 < line.length) { cur += line[++i] }
      else cur += c
    } else if (c === '"' || c === "'") { q = c; has = true }
    else if (c === ' ' || c === '\t') { if (has || cur) { out.push(cur); cur = ''; has = false } }
    else cur += c
    i++
  }
  if (has || cur) out.push(cur)
  return out
}

// --- RESP encode (args -> bytes) ---
function toResp(args) {
  const parts = [enc.encode('*' + args.length + '\r\n')]
  for (const a of args) {
    const b = a instanceof Uint8Array ? a : enc.encode('' + a)
    parts.push(enc.encode('$' + b.length + '\r\n'), b, enc.encode('\r\n'))
  }
  let len = 0
  for (const p of parts) len += p.length
  const out = new Uint8Array(len)
  let o = 0
  for (const p of parts) { out.set(p, o); o += p.length }
  return out
}

// --- RESP decode (one value) ---
function parseResp(buf, pos) {
  const dec = new TextDecoder()
  function line(p) {
    let e = p
    while (e < buf.length && buf[e] !== 0x0d) e++
    return [dec.decode(buf.subarray(p, e)), e + 2]
  }
  const t = buf[pos]
  const [head, np] = line(pos + 1)
  if (t === 0x2b) return [{ t: 'status', v: head }, np]      // +
  if (t === 0x2d) return [{ t: 'error', v: head }, np]       // -
  if (t === 0x3a) return [{ t: 'int', v: Number(head) }, np] // :
  if (t === 0x24) {                                          // $ bulk
    const n = Number(head)
    if (n < 0) return [{ t: 'nil' }, np]
    const s = dec.decode(buf.subarray(np, np + n))
    return [{ t: 'bulk', v: s }, np + n + 2]
  }
  if (t === 0x2a) {                                          // * array
    const n = Number(head)
    if (n < 0) return [{ t: 'nil' }, np]
    const arr = []
    let p = np
    for (let i = 0; i < n; i++) { const [v, q] = parseResp(buf, p); arr.push(v); p = q }
    return [{ t: 'array', v: arr }, p]
  }
  if (t === 0x25) {                                          // % map (RESP3)
    const n = Number(head)
    const arr = []
    let p = np
    for (let i = 0; i < n * 2; i++) { const [v, q] = parseResp(buf, p); arr.push(v); p = q }
    return [{ t: 'map', v: arr }, p]
  }
  return [{ t: 'raw', v: head }, np]
}

// Flatten a k/v reply (RESP2 flat array *2n OR RESP3 map %n) to a {key: number}.
// QSTATS returns {enqueued, dequeued, depth} either way depending on the proto.
function kvNums(v) {
  const a = v && (v.t === 'map' || v.t === 'array') ? v.v : []
  const o = {}
  for (let i = 0; i + 1 < a.length; i += 2) o[a[i].v] = Number(a[i + 1].v)
  return o
}

// small reply accessors used by the Keys inspector
const rarr = (v) => (v && (v.t === 'array' || v.t === 'map') ? v.v : [])
const rval = (v) => (v && v.v != null ? v.v : v && v.t === 'nil' ? null : '')
const rpairs = (a) => { const o = []; for (let i = 0; i + 1 < a.length; i += 2) o.push({ f: rval(a[i]), v: rval(a[i + 1]) }); return o }
// a flat RESP2 map (*2n) or RESP3 map -> {key: rawValue} (strings preserved, unlike kvNums)
const kvObj = (v) => { const a = rarr(v), o = {}; for (let i = 0; i + 1 < a.length; i += 2) o[rval(a[i])] = rval(a[i + 1]); return o }

function replyText(v, depth = 0) {
  if (!v) return ''
  switch (v.t) {
    case 'status': return v.v
    case 'error': return v.v
    case 'int': return '(integer) ' + v.v
    case 'nil': return '(nil)'
    case 'bulk': return JSON.stringify(v.v)
    case 'array':
      if (!v.v.length) return '(empty)'
      return v.v.map((x, i) => (i + 1) + ') ' + replyText(x, depth + 1)).join('\n')
    default: return v.v
  }
}

// dashboard password (set via the login gate); sent on every request. Persisted in
// sessionStorage so a reload within the tab stays logged in.
let AUTH = sessionStorage.getItem('dash-auth') || ''
let onAuthFail = null // App registers this to flip to the login screen on a 401

async function exec(args) {
  const res = await fetch('/api/exec', {
    method: 'POST', body: toResp(args),
    headers: AUTH ? { 'X-Dashboard-Auth': AUTH } : {},
  })
  if (res.status === 401) { if (onAuthFail) onAuthFail(); return { t: 'error', v: 'ERR unauthorized' } }
  const buf = new Uint8Array(await res.arrayBuffer())
  const [v] = parseResp(buf, 0)
  return v
}

async function sha1hex(text) {
  const d = await crypto.subtle.digest('SHA-1', enc.encode(text))
  return [...new Uint8Array(d)].map((b) => b.toString(16).padStart(2, '0')).join('')
}

// ---------- charts / metrics (Overview) ----------
function Chart({ data, series, height = 190 }) {
  const el = useRef(null), plot = useRef(null)
  useEffect(() => {
    plot.current = new uPlot({
      width: el.current.clientWidth, height, series,
      legend: { show: true, live: true },
      axes: [
        { stroke: '#5c6b82', grid: { stroke: '#1a2230' }, ticks: { stroke: '#1a2230' } },
        { stroke: '#5c6b82', grid: { stroke: '#1a2230' }, ticks: { stroke: '#1a2230' }, size: 60, values: (u, vs) => vs.map(fmt) },
      ],
    }, data, el.current)
    const ro = new ResizeObserver(() => plot.current.setSize({ width: el.current.clientWidth, height }))
    ro.observe(el.current)
    return () => { ro.disconnect(); plot.current.destroy() }
  }, [])
  useEffect(() => { if (plot.current) plot.current.setData(data) }, [data])
  return <div ref={el} class="chart" />
}
const line = (label, stroke) => ({ label, stroke, width: 1.6, points: { show: false } })

function useMetrics() {
  const [snap, setSnap] = useState({ live: false, cur: null, ops: null, mem: null })
  const buf = useRef({ prev: null, t: [], cmds: [], str: [], list: [], stream: [], pub: [], mem: [] })
  useEffect(() => {
    let ws, stop = false
    const push = (a, v) => { a.push(v); if (a.length > WINDOW) a.shift() }
    const connect = () => {
      ws = new WebSocket((location.protocol === 'https:' ? 'wss' : 'ws') + '://' + location.host + '/ws'
        + (AUTH ? '?auth=' + AUTH : ''))
      ws.onclose = () => { setSnap((s) => ({ ...s, live: false })); if (!stop) setTimeout(connect, 1000) }
      ws.onmessage = (e) => {
        let m; try { m = JSON.parse(e.data) } catch { return }
        if (!m.t) return
        const d = buf.current, prev = d.prev, dt = prev ? (m.t - prev.t) / 1000 : 0
        const rate = (a, b) => (dt > 0 ? Math.max(0, (a - b) / dt) : 0)
        if (prev && dt > 0) {
          push(d.t, m.t / 1000); push(d.cmds, rate(m.cmds, prev.cmds)); push(d.str, rate(m.str, prev.str))
          push(d.list, rate(m.list, prev.list)); push(d.stream, rate(m.stream, prev.stream))
          push(d.pub, rate(m.pub, prev.pub)); push(d.mem, m.mem / 1048576)
        }
        d.prev = m
        setSnap({ live: true, cur: m,
          ops: [d.t.slice(), d.cmds.slice(), d.str.slice(), d.list.slice(), d.stream.slice(), d.pub.slice()],
          mem: [d.t.slice(), d.mem.slice()] })
      }
    }
    connect()
    return () => { stop = true; if (ws) ws.close() }
  }, [])
  return snap
}

const Stat = ({ k, v, sub }) => (
  <div class="card"><div class="k">{k}</div><div class="v">{v}</div>{sub != null && <div class="r">{sub}</div>}</div>
)
const rateOf = (s, i) => (s.ops && s.ops[i] && s.ops[i].length ? s.ops[i][s.ops[i].length - 1] : 0)

function Overview({ snap }) {
  const m = snap.cur || {}, mm = m.maxmem > 0 ? m.maxmem : 0, pct = mm ? Math.min(100, (m.mem / mm) * 100) : 0
  return (
    <div>
      <div class="cards">
        <Stat k="commands/s" v={fmt(rateOf(snap, 1))} sub={fmt(m.cmds) + ' total'} />
        <Stat k="strings/s" v={fmt(rateOf(snap, 2))} sub={fmt(m.str) + ' total'} />
        <Stat k="lists/s" v={fmt(rateOf(snap, 3))} sub={fmt(m.list) + ' total'} />
        <Stat k="streams/s" v={fmt(rateOf(snap, 4))} sub={fmt(m.stream) + ' total'} />
        <Stat k="publish/s" v={fmt(rateOf(snap, 5))} sub={fmt(m.pub) + ' total'} />
        <Stat k="keys" v={fmt(m.keys)} sub={(m.channels || 0) + ' ch / ' + (m.patterns || 0) + ' pat'} />
        <Stat k="clients" v={m.clients || 0} sub={(m.blocked || 0) + ' blocked'} />
        <Stat k="expired / evicted" v={fmt(m.expired) + ' / ' + fmt(m.evicted)} />
      </div>
      <div class="panel"><div class="title">Operations / sec</div>
        {snap.ops && <Chart data={snap.ops} series={[{}, line('cmds', '#2f81f7'), line('str', '#3fb950'),
          line('list', '#d29922'), line('stream', '#a371f7'), line('pub', '#f778ba')]} />}</div>
      <div class="panel"><div class="title">Memory (MB) &mdash; {bytes(m.mem)}{mm ? ' / ' + bytes(mm) : ' / ∞'}</div>
        <div class="bar"><div class="fill" style={{ width: (mm ? pct : 4) + '%' }} /></div>
        {snap.mem && <Chart data={snap.mem} height={150} series={[{}, line('used', '#2f81f7')]} />}</div>
    </div>
  )
}

// ---------- Console: run Redis commands from the browser ----------
function Console() {
  const [hist, setHist] = useState([{ cmd: '', out: 'Type a Redis command, e.g.  SET foo bar   or   LRANGE mylist 0 -1', cls: 'muted' }])
  const [input, setInput] = useState('')
  const past = useRef([]); const pi = useRef(-1)
  const bodyRef = useRef(null)
  useEffect(() => { if (bodyRef.current) bodyRef.current.scrollTop = bodyRef.current.scrollHeight }, [hist])
  const run = async () => {
    const l = input.trim(); if (!l) return
    past.current.unshift(l); pi.current = -1; setInput('')
    const args = tokenize(l)
    const v = await exec(args)
    setHist((h) => [...h, { cmd: l, out: replyText(v), cls: v.t === 'error' ? 'err' : '' }])
  }
  const onKey = (e) => {
    if (e.key === 'Enter') run()
    else if (e.key === 'ArrowUp') { if (pi.current + 1 < past.current.length) setInput(past.current[++pi.current]); e.preventDefault() }
    else if (e.key === 'ArrowDown') { if (pi.current > 0) setInput(past.current[--pi.current]); else { pi.current = -1; setInput('') } }
  }
  return (
    <div class="term">
      <div class="termbody" ref={bodyRef}>
        {hist.map((h, i) => (
          <div key={i}>
            {h.cmd && <div class="cmdline"><span class="prompt">&gt;</span> {h.cmd}</div>}
            <pre class={'reply ' + (h.cls || '')}>{h.out}</pre>
          </div>
        ))}
      </div>
      <div class="terminput">
        <span class="prompt">&gt;</span>
        <input autofocus spellcheck={false} value={input} placeholder="command…"
          onInput={(e) => setInput(e.target.value)} onKeyDown={onKey} />
      </div>
    </div>
  )
}

// ---------- Lua editor (CodeJar + Prism) ----------
function LuaEditor({ code, onChange }) {
  const el = useRef(null), jar = useRef(null)
  useEffect(() => {
    jar.current = CodeJar(el.current, (e) => {
      e.innerHTML = Prism.highlight(e.textContent, Prism.languages.lua, 'lua')
    }, { tab: '  ' })
    jar.current.updateCode(code)
    jar.current.onUpdate((c) => onChange(c))
    return () => jar.current.destroy()
  }, [])
  useEffect(() => { if (jar.current && jar.current.toString() !== code) jar.current.updateCode(code) }, [code])
  return <div ref={el} class="editor language-lua" />
}

// ---------- Playground: Lua scripts via EVAL, with a per-SHA sidebar ----------
const DEFAULT_LUA = "-- KEYS[] and ARGV[] are available\nreturn redis.call('SET', KEYS[1], ARGV[1])"

// A small hello-world library — click to load into the editor (with sample KEYS/ARGV).
const EXAMPLES = [
  { name: 'hello', keys: '', argv: '',
    code: "-- the basics — KEYS[] and ARGV[] are available\nreturn 'Hello from Lua on dreads!'" },
  { name: 'json', keys: '', argv: '',
    code: "-- cjson: encode & decode\nlocal obj = { name = 'dreads', langs = { 'd', 'lua' }, fast = true }\nlocal s = cjson.encode(obj)\nlocal back = cjson.decode(s)\nreturn cjson.encode({ name = back.name, langs = #back.langs, json = s })" },
  { name: 'msgpack', keys: '', argv: '',
    code: "-- cmsgpack: pack & unpack (compact binary)\nlocal packed = cmsgpack.pack({ 1, 'two', { three = 3 } })\nlocal value = cmsgpack.unpack(packed)\nreturn cjson.encode({ bytes = #packed, unpacked = value })" },
  { name: 'migration', keys: 'user:old user:new', argv: '',
    code: "-- migration: copy a hash into a new key, versioning each field\nlocal data = redis.call('HGETALL', KEYS[1])\nfor i = 1, #data, 2 do\n  redis.call('HSET', KEYS[2], 'v2:' .. data[i], data[i + 1])\nend\nreturn redis.call('HLEN', KEYS[2])" },
  { name: 'rate limit', keys: 'rl:user1', argv: '5',
    code: "-- fixed-window rate limiter (INCR + EXPIRE)\nlocal n = redis.call('INCR', KEYS[1])\nif n == 1 then redis.call('EXPIRE', KEYS[1], 60) end\nif n > tonumber(ARGV[1]) then return 'blocked' end\nreturn 'allowed (' .. n .. ')'" },
  { name: 'atomic swap', keys: 'a b', argv: '',
    code: "-- atomic get-and-swap of two keys' values\nlocal va = redis.call('GET', KEYS[1])\nlocal vb = redis.call('GET', KEYS[2])\nredis.call('SET', KEYS[1], vb or '')\nredis.call('SET', KEYS[2], va or '')\nreturn cjson.encode({ [KEYS[1]] = vb, [KEYS[2]] = va })" },
]

function Playground() {
  const [code, setCode] = useState(DEFAULT_LUA)
  const [keys, setKeys] = useState('mykey')
  const [argv, setArgv] = useState('hello')
  const [out, setOut] = useState({ out: '', cls: 'muted' })
  const [scripts, setScripts] = useState([])
  // pull EVERY cached script from the server (SCRIPT LIST -> [ [sha, src], ... ])
  const refresh = useCallback(async () => {
    const v = await exec(['SCRIPT', 'LIST'])
    if (v.t !== 'array') { setScripts([]); return }
    setScripts(v.v.map((p) => {
      const sha = p.v?.[0]?.v || '', src = p.v?.[1]?.v || ''
      return { sha, code: src, preview: src.split('\n')[0].slice(0, 42) }
    }))
  }, [])
  useEffect(() => { refresh() }, [])
  const run = async () => {
    const ks = keys.split(/\s+/).filter(Boolean), as = argv.split(/\s+/).filter(Boolean)
    const v = await exec(['EVAL', code, '' + ks.length, ...ks, ...as])
    setOut({ out: replyText(v), cls: v.t === 'error' ? 'err' : '' })
    refresh() // the script is now cached server-side
  }
  const load = (s) => { setCode(s.code); setOut({ out: '', cls: 'muted' }) }
  const remove = async (sha) => { await exec(['SCRIPT', 'REMOVE', sha]); refresh() }
  const newScript = () => {
    setCode("-- new script — KEYS[] and ARGV[] are available\nreturn 'ok'")
    setKeys(''); setArgv(''); setOut({ out: '', cls: 'muted' })
  }
  const useExample = (ex) => { setCode(ex.code); setKeys(ex.keys); setArgv(ex.argv); setOut({ out: '', cls: 'muted' }) }
  const save = async () => {
    // SCRIPT LOAD caches the script by SHA WITHOUT running it (create a new script)
    const v = await exec(['SCRIPT', 'LOAD', code])
    setOut({ out: v.t === 'error' ? v.v : 'cached as ' + (v.v || ''), cls: v.t === 'error' ? 'err' : '' })
    refresh()
  }
  return (
    <div class="pg">
      <aside class="scripts">
        <div class="title">examples</div>
        <div class="examples">
          {EXAMPLES.map((ex) => (
            <button class="ex" key={ex.name} title={ex.code.split('\n')[0]} onClick={() => useExample(ex)}>{ex.name}</button>
          ))}
        </div>
        <div class="title" style="margin-top:.9rem">scripts <span class="dim">({scripts.length})</span>
          <button class="mini" title="new script" onClick={newScript}>＋</button>
          <button class="mini" title="refresh" onClick={refresh}>⟳</button></div>
        {scripts.length === 0 && <div class="dim small">no cached scripts</div>}
        {scripts.map((s) => (
          <div class="script" key={s.sha}>
            <div class="sbody" onClick={() => load(s)} title={s.code}>
              <div class="sha">{s.sha.slice(0, 12)}</div>
              <div class="prev">{s.preview || '(empty)'}</div>
            </div>
            <button class="x" title="remove" onClick={() => remove(s.sha)}>×</button>
          </div>
        ))}
      </aside>
      <div class="pgmain">
        <div class="title">Lua playground <span class="dim">— EVAL</span></div>
        <LuaEditor code={code} onChange={setCode} />
        <div class="pgargs">
          <label>KEYS <input value={keys} onInput={(e) => setKeys(e.target.value)} placeholder="space-separated" /></label>
          <label>ARGV <input value={argv} onInput={(e) => setArgv(e.target.value)} placeholder="space-separated" /></label>
          <button class="mini big" title="new script" onClick={newScript}>New</button>
          <button class="mini big" title="cache without running (SCRIPT LOAD)" onClick={save}>Save</button>
          <button class="run" onClick={run}>Run ▶</button>
        </div>
        <pre class={'reply ' + (out.cls || '')}>{out.out || 'reply appears here'}</pre>
      </div>
    </div>
  )
}

// ---------- Queues: Redis lists as message queues (Celery/RQ/Sidekiq), RabbitMQ-style ----------
async function scanList(pattern) {
  let cursor = '0', keys = []
  do {
    const args = ['SCAN', cursor]
    if (pattern) args.push('MATCH', '*' + pattern + '*')
    args.push('COUNT', '500', 'TYPE', 'list')
    const v = await exec(args)
    if (v.t !== 'array') break
    cursor = v.v[0].v
    for (const k of (v.v[1].v || [])) keys.push(k.v)
  } while (cursor !== '0' && keys.length < 5000)
  return keys
}

function Queues() {
  const [rows, setRows] = useState([])
  const [sel, setSel] = useState(null)
  const [msgs, setMsgs] = useState([])
  const [nameQ, setNameQ] = useState('')
  const [msgQ, setMsgQ] = useState('')
  const [sort, setSort] = useState({ k: 'depth', dir: -1 })
  const [pub, setPub] = useState('')
  const [chart, setChart] = useState(null)
  const hist = useRef({})
  const selH = useRef({ name: null, t: [], in: [], out: [] })
  const nameQR = useRef(nameQ), selR = useRef(sel)
  nameQR.current = nameQ; selR.current = sel

  const poll = useCallback(async () => {
    const keys = await scanList(nameQR.current.trim())
    const now = Date.now() / 1000
    // QSTATS gives {enqueued, dequeued, depth} in ONE call — real incoming/deliver
    // rates (RabbitMQ-style), not the net LLEN delta that hides in≈out throughput.
    const stats = await Promise.all(keys.map((k) => exec(['QSTATS', k])))
    const next = keys.map((k, i) => {
      const s = kvNums(stats[i])
      const depth = s.depth || 0, enq = s.enqueued || 0, deq = s.dequeued || 0
      let h = hist.current[k]
      // first sight, or counters reset (key recreated ⇒ enq/deq drop): rebaseline
      if (!h || enq < h.enq || deq < h.deq) h = hist.current[k] = { enq, deq, t: now, inR: 0, outR: 0 }
      const dt = now - h.t
      if (dt > 0.1) {
        h.inR = (enq - h.enq) / dt
        h.outR = (deq - h.deq) / dt
        h.enq = enq; h.deq = deq; h.t = now
      }
      return { name: k, depth, enq, deq, inR: h.inR, outR: h.outR }
    })
    setRows(next)
    const sname = selR.current
    if (sname) {
      const d = next.find((r) => r.name === sname)
      const sh = selH.current
      if (sh.name !== sname) { sh.name = sname; sh.t = []; sh.in = []; sh.out = [] }
      sh.t.push(now); sh.in.push(d ? d.inR : 0); sh.out.push(d ? d.outR : 0)
      if (sh.t.length > 120) { sh.t.shift(); sh.in.shift(); sh.out.shift() }
      setChart([sh.t.slice(), sh.in.slice(), sh.out.slice()])
    }
  }, [])

  useEffect(() => { poll(); const id = setInterval(poll, 2000); return () => clearInterval(id) }, [poll])

  const open = async (name) => {
    setSel(name); selR.current = name; selH.current = { name, t: [], in: [], out: [] }
    const v = await exec(['LRANGE', name, '0', '199'])
    setMsgs(v.t === 'array' ? v.v.map((x) => (x.v == null ? '(nil)' : x.v)) : [])
  }
  const publish = async () => {
    if (!pub || !sel) return
    await exec(['RPUSH', sel, pub]); setPub(''); open(sel); poll()
  }
  const sortBy = (k) => setSort((s) => ({ k, dir: s.k === k ? -s.dir : (k === 'name' ? 1 : -1) }))
  const sorted = [...rows].sort((a, b) => (a[sort.k] < b[sort.k] ? -1 : a[sort.k] > b[sort.k] ? 1 : 0) * sort.dir)
  const shown = msgs.filter((m) => !msgQ || m.includes(msgQ))
  const arrow = (k) => (sort.k === k ? (sort.dir > 0 ? ' ▲' : ' ▼') : '')
  const selRow = sel && rows.find((r) => r.name === sel)
  const rate = (v) => (v ? v.toFixed(v < 10 ? 1 : 0) : '0')

  return (
    <div>
      <div class="qbar">
        <input class="qsearch wide" spellcheck={false} placeholder="filter queues (regex-ish)…"
          value={nameQ} onInput={(e) => setNameQ(e.target.value)} />
        <span class="dim small">{rows.length} queues · auto-refresh 2s</span>
        <button class="mini" title="refresh now" onClick={poll}>⟳</button>
      </div>
      <div class="panel nopad">
        <table class="qtable">
          <thead><tr>
            <th onClick={() => sortBy('name')}>Name{arrow('name')}</th>
            <th class="num" onClick={() => sortBy('depth')}>Ready{arrow('depth')}</th>
            <th class="num" onClick={() => sortBy('inR')}>incoming/s{arrow('inR')}</th>
            <th class="num" onClick={() => sortBy('outR')}>deliver/s{arrow('outR')}</th>
          </tr></thead>
          <tbody>
            {sorted.map((q) => (
              <tr key={q.name} class={sel === q.name ? 'on' : ''} onClick={() => open(q.name)}>
                <td class="qname">{q.name}</td>
                <td class="num strong">{fmt(q.depth)}</td>
                <td class={'num ' + (q.inR > 0.05 ? 'up' : 'dim')}>{q.inR > 0.05 ? '+' + rate(q.inR) : '0'}</td>
                <td class={'num ' + (q.outR > 0.05 ? 'down' : 'dim')}>{q.outR > 0.05 ? '−' + rate(q.outR) : '0'}</td>
              </tr>
            ))}
            {rows.length === 0 && <tr><td colspan="4" class="dim small" style="padding:1rem">no list keys</td></tr>}
          </tbody>
        </table>
      </div>

      {sel && (
        <div class="panel">
          <div class="title">{sel} <span class="dim">— message rates</span></div>
          {selRow && (
            <div class="qstat">
              <span><b>{fmt(selRow.depth)}</b> ready</span>
              <span class="up">▲ {rate(selRow.inR)}/s in</span>
              <span class="down">▼ {rate(selRow.outR)}/s out</span>
              <span class="dim">{fmt(selRow.enq)} enq · {fmt(selRow.deq)} deq lifetime</span>
            </div>
          )}
          {chart && <Chart data={chart} height={130}
            series={[{}, line('incoming', '#3fb950'), line('deliver', '#db6d28')]} />}
          <div class="pgargs">
            <label style="flex:1">publish <input value={pub} placeholder="message body (RPUSH)"
              onInput={(e) => setPub(e.target.value)} onKeyDown={(e) => e.key === 'Enter' && publish()} style="flex:1" /></label>
            <button class="run" onClick={publish}>Publish ▶</button>
          </div>
          <div class="title">get messages <span class="dim">— head 200</span></div>
          <input class="qsearch wide" spellcheck={false} placeholder="search in messages…"
            value={msgQ} onInput={(e) => setMsgQ(e.target.value)} />
          <div class="msgs">
            {shown.map((m, i) => <pre class="msg" key={i}><span class="mi">{i}</span>{m}</pre>)}
            {shown.length === 0 && <div class="dim small">no messages</div>}
          </div>
        </div>
      )}
    </div>
  )
}

// ---------- Keys: type-aware inspector (browse + view content + edit/delete) ----------
const SIZECMD = { string: 'STRLEN', list: 'LLEN', hash: 'HLEN', set: 'SCARD', zset: 'ZCARD', stream: 'XLEN' }
const ttlText = (pttl) => (pttl === -1 ? 'no expiry' : pttl === -2 ? 'expired' : pttl >= 1000 ? (pttl / 1000).toFixed(pttl < 10000 ? 1 : 0) + 's' : pttl + 'ms')

function Keys() {
  const [rows, setRows] = useState([])
  const [sel, setSel] = useState(null)
  const [det, setDet] = useState(null)     // { type, enc, pttl, size, items }
  const [nameQ, setNameQ] = useState('')
  const [typeQ, setTypeQ] = useState('')   // '' = all
  const [a, setA] = useState(''); const [b, setB] = useState('')  // add-value inputs
  const [err, setErr] = useState('')
  const nameQR = useRef(nameQ), typeQR = useRef(typeQ)
  nameQR.current = nameQ; typeQR.current = typeQ

  const scan = useCallback(async () => {
    let cursor = '0', keys = []
    do {
      const q = ['SCAN', cursor, 'COUNT', '400']
      const nq = nameQR.current.trim(); if (nq) q.push('MATCH', '*' + nq + '*')
      if (typeQR.current) q.push('TYPE', typeQR.current)
      const v = await exec(q)
      if (v.t !== 'array') break
      cursor = rval(rarr(v)[0])
      for (const k of rarr(rarr(v)[1])) keys.push(rval(k))
    } while (cursor !== '0' && keys.length < 400)
    const types = typeQR.current ? keys.map(() => typeQR.current)
      : (await Promise.all(keys.map((k) => exec(['TYPE', k])))).map((v) => rval(v) || '?')
    setRows(keys.map((k, i) => ({ key: k, type: types[i] })))
  }, [])
  useEffect(() => { scan() }, [])

  const open = async (key, type) => {
    setSel(key); setA(''); setB(''); setErr('')
    const [encR, ttlR] = await Promise.all([exec(['OBJECT', 'ENCODING', key]), exec(['PTTL', key])])
    let size = 0, items = []
    if (SIZECMD[type]) { const r = await exec([SIZECMD[type], key]); size = r.t === 'int' ? r.v : 0 }
    if (type === 'string') { items = [rval(await exec(['GET', key]))] }
    else if (type === 'list') { items = rarr(await exec(['LRANGE', key, '0', '199'])).map(rval) }
    else if (type === 'hash') { items = rpairs(rarr(await exec(['HGETALL', key]))) }
    else if (type === 'set') { items = rarr(rarr(await exec(['SSCAN', key, '0', 'COUNT', '400']))[1]).map(rval) }
    else if (type === 'zset') { items = rpairs(rarr(await exec(['ZRANGE', key, '0', '199', 'WITHSCORES']))) }
    else if (type === 'stream') {
      items = rarr(await exec(['XRANGE', key, '-', '+', 'COUNT', '100']))
        .map((e) => ({ id: rval(rarr(e)[0]), fields: rpairs(rarr(rarr(e)[1])) }))
    }
    setDet({ type, enc: rval(encR), pttl: ttlR.t === 'int' ? ttlR.v : -1, size, items })
  }

  const write = async (args) => {
    const v = await exec(args)
    if (v.t === 'error') { setErr(v.v); return false }
    setErr(''); setA(''); setB(''); await open(sel, det.type); scan(); return true
  }
  const del = async () => { if (await write(['DEL', sel])) { setSel(null); setDet(null) } }
  const add = () => {
    const t = det.type
    if (t === 'string') write(['SET', sel, a])
    else if (t === 'list') write(['RPUSH', sel, a])
    else if (t === 'set') write(['SADD', sel, a])
    else if (t === 'hash') write(['HSET', sel, a, b])
    else if (t === 'zset') write(['ZADD', sel, a, b])
    else if (t === 'stream') write(['XADD', sel, '*', a, b])
  }
  const addPlaceholder = { string: ['value (SET)'], list: ['value (RPUSH)'], set: ['member (SADD)'],
    hash: ['field', 'value'], zset: ['score', 'member'], stream: ['field', 'value'] }

  const shown = [...rows].sort((x, y) => (x.key < y.key ? -1 : x.key > y.key ? 1 : 0))
  const two = det && ['hash', 'zset', 'stream'].includes(det.type)

  return (
    <div class="kwrap">
      <div class="kside">
        <div class="qbar">
          <input class="qsearch" spellcheck={false} placeholder="filter keys…" value={nameQ}
            onInput={(e) => setNameQ(e.target.value)} onKeyDown={(e) => e.key === 'Enter' && scan()} />
          <button class="mini" title="scan" onClick={scan}>⟳</button>
        </div>
        <div class="ktypes">
          {['', 'string', 'list', 'hash', 'set', 'zset', 'stream'].map((t) => (
            <button key={t} class={'ktf ' + (typeQ === t ? 'on' : '')}
              onClick={() => { setTypeQ(t); setTimeout(scan, 0) }}>{t || 'all'}</button>
          ))}
        </div>
        <div class="klist">
          {shown.map((r) => (
            <div key={r.key} class={'krow ' + (sel === r.key ? 'on' : '')} onClick={() => open(r.key, r.type)}>
              <span class="kn">{r.key}</span><span class={'kt kt-' + r.type}>{r.type}</span>
            </div>
          ))}
          {rows.length === 0 && <div class="dim small" style="padding:.8rem">no keys</div>}
        </div>
        <div class="dim small kcount">{rows.length} keys</div>
      </div>

      <div class="kmain">
        {!sel && <div class="dim" style="padding:2rem">select a key to inspect its value</div>}
        {sel && det && (
          <div class="panel">
            <div class="khead">
              <span class="ktitle">{sel}</span>
              <span class={'kt kt-' + det.type}>{det.type}</span>
              <span class="dim small">{det.enc} · {fmt(det.size)} {det.type === 'string' ? 'bytes' : 'items'} · {ttlText(det.pttl)}</span>
              <button class="del" title="DEL key" onClick={del}>Delete</button>
            </div>

            {det.type === 'string' && <pre class="kstr">{det.items[0] == null ? '(nil)' : det.items[0]}</pre>}
            {det.type === 'list' && <div class="msgs">
              {det.items.map((m, i) => <pre class="msg" key={i}><span class="mi">{i}</span>{m}</pre>)}</div>}
            {det.type === 'set' && <div class="msgs">
              {det.items.map((m, i) => <pre class="msg" key={i}>{m}</pre>)}</div>}
            {(det.type === 'hash' || det.type === 'zset') && <table class="qtable"><thead><tr>
              <th>{det.type === 'zset' ? 'member' : 'field'}</th><th class="num">{det.type === 'zset' ? 'score' : 'value'}</th>
            </tr></thead><tbody>
              {det.items.map((p, i) => (
                <tr key={i}><td class="qname">{p.f}</td>
                  <td class={det.type === 'zset' ? 'num' : ''}>{p.v}</td></tr>
              ))}
            </tbody></table>}
            {det.type === 'stream' && <div class="msgs">
              {det.items.map((e, i) => <pre class="msg" key={i}><span class="mi">{e.id}</span>
                {e.fields.map((f) => f.f + '=' + f.v).join('  ')}</pre>)}</div>}

            <div class="pgargs">
              <input value={a} placeholder={addPlaceholder[det.type][0]} onInput={(e) => setA(e.target.value)}
                onKeyDown={(e) => e.key === 'Enter' && !two && add()} style="flex:1" />
              {two && <input value={b} placeholder={addPlaceholder[det.type][1]} onInput={(e) => setB(e.target.value)}
                onKeyDown={(e) => e.key === 'Enter' && add()} style="flex:1" />}
              <button class="run" onClick={add}>Add ▶</button>
            </div>
            {err && <div class="err small" style="padding:.2rem .1rem">{err}</div>}
          </div>
        )}
      </div>
    </div>
  )
}

// ---------- Pubsub: active channels / patterns / subscriber counts + publish tester ----------
function Pubsub() {
  const [chans, setChans] = useState([])   // [{ ch, subs }]
  const [shard, setShard] = useState([])   // [{ ch, subs }]
  const [npat, setNpat] = useState(0)
  const [nameQ, setNameQ] = useState('')
  const [pch, setPch] = useState(''); const [pmsg, setPmsg] = useState(''); const [pres, setPres] = useState('')
  const [tailing, setTailing] = useState(false)
  const [feed, setFeed] = useState([])
  const [tailQ, setTailQ] = useState('')
  const feedR = useRef([])
  const nameQR = useRef(nameQ); nameQR.current = nameQ

  const poll = useCallback(async () => {
    const q = nameQR.current.trim()
    const chArgs = ['PUBSUB', 'CHANNELS']; if (q) chArgs.push('*' + q + '*')
    const list = rarr(await exec(chArgs)).map(rval)
    const subs = list.length ? rpairs(rarr(await exec(['PUBSUB', 'NUMSUB', ...list]))) : []
    setChans(subs.map((p) => ({ ch: p.f, subs: Number(p.v) })))
    setNpat(Number(rval(await exec(['PUBSUB', 'NUMPAT']))) || 0)
    const sh = rarr(await exec(['PUBSUB', 'SHARDCHANNELS'])).map(rval)
    const shs = sh.length ? rpairs(rarr(await exec(['PUBSUB', 'SHARDNUMSUB', ...sh]))) : []
    setShard(shs.map((p) => ({ ch: p.f, subs: Number(p.v) })))
  }, [])
  useEffect(() => { poll(); const id = setInterval(poll, 2000); return () => clearInterval(id) }, [poll])

  // Live-ish tail: poll PUBSUB TAP (dreads-native) — the server buffers published
  // messages while armed and returns them in a batch each poll (each poll rearms;
  // the tap self-expires when we stop). Newest-first, capped client-side.
  useEffect(() => {
    if (!tailing) return
    const tick = async () => {
      const pairs = rpairs(rarr(await exec(['PUBSUB', 'TAP'])))
      if (pairs.length) {
        const ts = new Date().toLocaleTimeString()
        feedR.current = [...pairs.map((p) => ({ ts, ch: p.f, msg: p.v })), ...feedR.current].slice(0, 500)
        setFeed(feedR.current)
      }
    }
    tick(); const id = setInterval(tick, 1500); return () => clearInterval(id)
  }, [tailing])
  const clearFeed = () => { feedR.current = []; setFeed([]) }
  const shownFeed = feed.filter((f) => !tailQ || f.ch.includes(tailQ) || (f.msg || '').includes(tailQ))

  const publish = async () => {
    if (!pch) return
    const r = await exec(['PUBLISH', pch, pmsg])
    setPres(r.t === 'int' ? `delivered to ${r.v} subscriber${r.v === 1 ? '' : 's'}` : (r.v || 'error'))
    poll()
  }
  const totSubs = chans.reduce((s, c) => s + c.subs, 0)

  return (
    <div>
      <div class="qbar">
        <input class="qsearch wide" spellcheck={false} placeholder="filter channels…"
          value={nameQ} onInput={(e) => setNameQ(e.target.value)} />
        <span class="dim small">{chans.length} channels · {npat} pattern{npat === 1 ? '' : 's'} · {totSubs} subs · auto-refresh 2s</span>
        <button class="mini" title="refresh now" onClick={poll}>⟳</button>
      </div>

      <div class="panel">
        <div class="pgargs">
          <input value={pch} placeholder="channel" onInput={(e) => setPch(e.target.value)} style="flex:0 0 30%" />
          <input value={pmsg} placeholder="message" onInput={(e) => setPmsg(e.target.value)}
            onKeyDown={(e) => e.key === 'Enter' && publish()} style="flex:1" />
          <button class="run" onClick={publish}>Publish ▶</button>
        </div>
        {pres && <div class="dim small" style="padding:.1rem">{pres}</div>}
      </div>

      <div class="panel">
        <div class="tailbar">
          <button class={'run ' + (tailing ? 'stop' : '')} style="margin:0"
            onClick={() => setTailing((t) => !t)}>{tailing ? '⏸ Stop tail' : '▶ Live tail'}</button>
          <input class="qsearch" spellcheck={false} placeholder="filter messages…" value={tailQ}
            onInput={(e) => setTailQ(e.target.value)} style="margin:0;flex:1" />
          <span class="dim small">{feed.length} captured{tailing ? ' · buffering' : ''}</span>
          <button class="mini" title="clear" onClick={clearFeed}>✕</button>
        </div>
        <div class="feed">
          {shownFeed.map((f, i) => (
            <div class="fmsg" key={i}>
              <span class="ft">{f.ts}</span><span class="fch">{f.ch}</span>
              <span class="fm">{f.msg == null ? '(nil)' : f.msg}</span>
            </div>
          ))}
          {shownFeed.length === 0 && <div class="dim small" style="padding:.6rem">
            {tailing ? 'waiting for messages…' : 'press Live tail to capture published messages'}</div>}
        </div>
      </div>

      <div class="panel nopad">
        <table class="qtable">
          <thead><tr><th>Channel</th><th class="num">Subscribers</th></tr></thead>
          <tbody>
            {chans.sort((a, b) => b.subs - a.subs).map((c) => (
              <tr key={c.ch} onClick={() => { setPch(c.ch) }}>
                <td class="qname">{c.ch}</td><td class="num strong">{c.subs}</td>
              </tr>
            ))}
            {chans.length === 0 && <tr><td colspan="2" class="dim small" style="padding:1rem">no active channels</td></tr>}
          </tbody>
        </table>
      </div>

      {shard.length > 0 && (
        <div class="panel nopad">
          <div class="title" style="padding:.6rem .9rem 0">shard channels</div>
          <table class="qtable">
            <thead><tr><th>Shard channel</th><th class="num">Subscribers</th></tr></thead>
            <tbody>
              {shard.map((c) => (
                <tr key={c.ch}><td class="qname">{c.ch}</td><td class="num strong">{c.subs}</td></tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  )
}

// ---------- Streams: entries + consumer groups + consumers + PEL (XINFO/XPENDING) ----------
function Streams() {
  const [rows, setRows] = useState([])
  const [sel, setSel] = useState(null)
  const [info, setInfo] = useState(null)     // XINFO STREAM header
  const [groups, setGroups] = useState([])   // XINFO GROUPS
  const [entries, setEntries] = useState([]) // XREVRANGE
  const [selG, setSelG] = useState(null)
  const [cons, setCons] = useState([])       // XINFO CONSUMERS (of selG)
  const [pel, setPel] = useState(null)       // XPENDING summary (of selG)
  const [nameQ, setNameQ] = useState('')
  const [af, setAf] = useState(''); const [av, setAv] = useState(''); const [err, setErr] = useState('')
  const nameQR = useRef(nameQ); nameQR.current = nameQ

  const scan = useCallback(async () => {
    let cursor = '0', names = []
    do {
      const q = ['SCAN', cursor, 'COUNT', '400', 'TYPE', 'stream']
      const nq = nameQR.current.trim(); if (nq) q.push('MATCH', '*' + nq + '*')
      const v = await exec(q)
      if (v.t !== 'array') break
      cursor = rval(rarr(v)[0])
      for (const k of rarr(rarr(v)[1])) names.push(rval(k))
    } while (cursor !== '0' && names.length < 400)
    const lens = await Promise.all(names.map((k) => exec(['XLEN', k])))
    setRows(names.map((k, i) => ({ name: k, len: lens[i] && lens[i].t === 'int' ? lens[i].v : 0 })))
  }, [])
  useEffect(() => { scan() }, [])

  const open = async (name) => {
    setSel(name); setSelG(null); setCons([]); setPel(null); setErr(''); setAf(''); setAv('')
    const [inf, grp, ent] = await Promise.all([
      exec(['XINFO', 'STREAM', name]), exec(['XINFO', 'GROUPS', name]),
      exec(['XREVRANGE', name, '+', '-', 'COUNT', '50']),
    ])
    setInfo(kvObj(inf))
    setGroups(rarr(grp).map((g) => kvObj(g)))
    setEntries(rarr(ent).map((e) => ({ id: rval(rarr(e)[0]), fields: rpairs(rarr(rarr(e)[1])) })))
  }
  const openGroup = async (g) => {
    setSelG(g)
    const [c, p] = await Promise.all([exec(['XINFO', 'CONSUMERS', sel, g]), exec(['XPENDING', sel, g])])
    setCons(rarr(c).map((x) => kvObj(x)))
    const pa = rarr(p) // [count, min, max, [[consumer,count],…]]
    setPel({ count: rval(pa[0]), min: rval(pa[1]), max: rval(pa[2]),
      byC: rarr(pa[3]).map((x) => ({ c: rval(rarr(x)[0]), n: rval(rarr(x)[1]) })) })
  }
  const addEntry = async () => {
    if (!af) return
    const v = await exec(['XADD', sel, '*', af, av])
    if (v.t === 'error') { setErr(v.v); return }
    setErr(''); setAf(''); setAv(''); open(sel); scan()
  }

  const sorted = [...rows].sort((x, y) => y.len - x.len)
  return (
    <div class="kwrap">
      <div class="kside">
        <div class="qbar">
          <input class="qsearch" spellcheck={false} placeholder="filter streams…" value={nameQ}
            onInput={(e) => setNameQ(e.target.value)} onKeyDown={(e) => e.key === 'Enter' && scan()} />
          <button class="mini" title="scan" onClick={scan}>⟳</button>
        </div>
        <div class="klist">
          {sorted.map((r) => (
            <div key={r.name} class={'krow ' + (sel === r.name ? 'on' : '')} onClick={() => open(r.name)}>
              <span class="kn">{r.name}</span><span class="num dim small">{fmt(r.len)}</span>
            </div>
          ))}
          {rows.length === 0 && <div class="dim small" style="padding:.8rem">no streams</div>}
        </div>
        <div class="dim small kcount">{rows.length} streams</div>
      </div>

      <div class="kmain">
        {!sel && <div class="dim" style="padding:2rem">select a stream</div>}
        {sel && info && (
          <div>
            <div class="panel">
              <div class="khead"><span class="ktitle">{sel}</span><span class="kt kt-stream">stream</span></div>
              <div class="qstat">
                <span><b>{fmt(Number(info.length))}</b> entries</span>
                <span class="dim">last id {info['last-generated-id']}</span>
                <span class="dim">{info.groups} group{info.groups === '1' ? '' : 's'}</span>
                <span class="dim">{info['entries-added']} added · {info['max-deleted-entry-id']} max-deleted</span>
              </div>
              <div class="pgargs">
                <input value={af} placeholder="field" onInput={(e) => setAf(e.target.value)} style="flex:0 0 30%" />
                <input value={av} placeholder="value" onInput={(e) => setAv(e.target.value)}
                  onKeyDown={(e) => e.key === 'Enter' && addEntry()} style="flex:1" />
                <button class="run" onClick={addEntry}>XADD ▶</button>
              </div>
              {err && <div class="err small">{err}</div>}
            </div>

            <div class="panel nopad">
              <div class="title" style="padding:.6rem .9rem 0">consumer groups</div>
              <table class="qtable"><thead><tr>
                <th>Group</th><th class="num">Consumers</th><th class="num">Pending</th>
                <th class="num">Lag</th><th>Last delivered</th>
              </tr></thead><tbody>
                {groups.map((g) => (
                  <tr key={g.name} class={selG === g.name ? 'on' : ''} onClick={() => openGroup(g.name)}>
                    <td class="qname">{g.name}</td><td class="num">{g.consumers}</td>
                    <td class={'num ' + (Number(g.pending) > 0 ? 'down' : 'dim')}>{g.pending}</td>
                    <td class="num">{g.lag == null ? '?' : g.lag}</td>
                    <td class="dim small">{g['last-delivered-id']}</td>
                  </tr>
                ))}
                {groups.length === 0 && <tr><td colspan="5" class="dim small" style="padding:.8rem">no consumer groups</td></tr>}
              </tbody></table>
            </div>

            {selG && (
              <div class="panel nopad">
                <div class="title" style="padding:.6rem .9rem 0">group <b>{selG}</b> — consumers
                  {pel && <span class="dim small"> · PEL {pel.count} ({pel.min}…{pel.max})</span>}</div>
                <table class="qtable"><thead><tr>
                  <th>Consumer</th><th class="num">Pending</th><th class="num">Idle (ms)</th>
                </tr></thead><tbody>
                  {cons.map((c) => (
                    <tr key={c.name}><td class="qname">{c.name}</td>
                      <td class={'num ' + (Number(c.pending) > 0 ? 'down' : 'dim')}>{c.pending}</td>
                      <td class="num dim">{c.idle}</td></tr>
                  ))}
                  {cons.length === 0 && <tr><td colspan="3" class="dim small" style="padding:.8rem">no consumers</td></tr>}
                </tbody></table>
              </div>
            )}

            <div class="panel nopad">
              <div class="title" style="padding:.6rem .9rem 0">recent entries <span class="dim">— newest 50</span></div>
              <div class="msgs" style="padding:.5rem .9rem">
                {entries.map((e, i) => <pre class="msg" key={i}><span class="mi">{e.id}</span>
                  {e.fields.map((f) => f.f + '=' + f.v).join('  ')}</pre>)}
                {entries.length === 0 && <div class="dim small">empty</div>}
              </div>
            </div>
          </div>
        )}
      </div>
    </div>
  )
}

// ---- ACL rule model: parse a canonical rule string <-> a form model, build tokens ----
function parseAcl(rulesStr) {
  const m = { enabled: false, nopass: false, pwHashes: [], newpw: '', allkeys: false, keys: [],
    allchannels: false, channels: [], cmdBase: 'none', cats: {}, cmds: {} }
  for (const t of rulesStr.split(/\s+/).filter(Boolean)) {
    if (t === 'on') m.enabled = true
    else if (t === 'off') m.enabled = false
    else if (t === 'nopass') m.nopass = true
    else if (t[0] === '#' || t[0] === '>') m.pwHashes.push(t) // keep existing hash (or literal) tokens
    else if (t === '~*' || t === 'allkeys') m.allkeys = true
    else if (t === '&*' || t === 'allchannels') m.allchannels = true
    else if (t === 'resetchannels' || t === 'nochannels') m.allchannels = false
    else if (t[0] === '~') m.keys.push({ pat: t.slice(1), mode: 'RW' })
    else if (t[0] === '%') { const mm = t.match(/^%(RW|R|W)~(.*)$/); if (mm) m.keys.push({ pat: mm[2], mode: mm[1] }) }
    else if (t[0] === '&') m.channels.push(t.slice(1))
    else if (t === '+@all' || t === 'allcommands') m.cmdBase = 'all'
    else if (t === '-@all' || t === 'nocommands') m.cmdBase = 'none'
    else if (t.startsWith('+@')) m.cats[t.slice(2)] = '+'
    else if (t.startsWith('-@')) m.cats[t.slice(2)] = '-'
    else if (t[0] === '+') m.cmds[t.slice(1)] = '+'
    else if (t[0] === '-') m.cmds[t.slice(1)] = '-'
    // reset / resetkeys / resetpass / resetdbs / clearselectors: ignored (the form is authoritative)
  }
  return m
}
function buildAcl(m) {
  const t = ['reset'] // form is the full spec — start clean, then apply
  t.push(m.enabled ? 'on' : 'off')
  if (m.nopass) t.push('nopass')
  else { m.pwHashes.forEach((h) => t.push(h)); if (m.newpw.trim()) t.push('>' + m.newpw.trim()) }
  if (m.allkeys) t.push('~*')
  else m.keys.forEach((k) => t.push(k.mode === 'RW' ? '~' + k.pat : '%' + k.mode + '~' + k.pat))
  if (m.allchannels) t.push('&*')
  else m.channels.forEach((c) => t.push('&' + c))
  t.push(m.cmdBase === 'all' ? '+@all' : '-@all')
  for (const [c, s] of Object.entries(m.cats)) if (s) t.push(s + '@' + c)
  for (const [c, s] of Object.entries(m.cmds)) if (s) t.push(s + c)
  return t
}

// ---- the ACL builder form (no hand-writing of rule tokens) ----
function AclForm({ m, set, cats }) {
  const [k, setK] = useState(''); const [ch, setCh] = useState(''); const [cmd, setCmd] = useState('')
  const up = (p) => set({ ...m, ...p })
  const addKey = () => { const v = k.trim(); if (v) { up({ keys: [...m.keys, { pat: v, mode: 'RW' }] }); setK('') } }
  const addChan = () => { const v = ch.trim(); if (v) { up({ channels: [...m.channels, v] }); setCh('') } }
  const setKeyMode = (i, mode) => { const ks = m.keys.slice(); ks[i] = { ...ks[i], mode }; up({ keys: ks }) }
  const cycCat = (c) => { const s = m.cats[c], nx = s === '+' ? '-' : s === '-' ? undefined : '+'
    const cc = { ...m.cats }; if (nx) cc[c] = nx; else delete cc[c]; up({ cats: cc }) }
  const addCmd = (sign) => { const v = cmd.trim().toLowerCase(); if (v) { up({ cmds: { ...m.cmds, [v]: sign } }); setCmd('') } }
  const rmCmd = (c) => { const cc = { ...m.cmds }; delete cc[c]; up({ cmds: cc }) }

  return (
    <div class="aclform">
      <div class="arow"><span class="alabel">status</span>
        <button class={'toggle ' + (m.enabled ? 'on' : '')} onClick={() => up({ enabled: !m.enabled })}>
          {m.enabled ? 'enabled' : 'disabled'}</button></div>

      <div class="arow"><span class="alabel">password</span>
        <label class="chk"><input type="checkbox" checked={m.nopass}
          onInput={(e) => up({ nopass: e.target.checked })} /> nopass</label>
        {!m.nopass && <input class="ain" type="password" placeholder={m.pwHashes.length ? 'set a new password' : 'password'}
          value={m.newpw} onInput={(e) => up({ newpw: e.target.value })} />}
        {!m.nopass && m.pwHashes.length > 0 && <span class="up small">✓ {m.pwHashes.length} set</span>}</div>

      <div class="arow"><span class="alabel">keys</span>
        <label class="chk"><input type="checkbox" checked={m.allkeys}
          onInput={(e) => up({ allkeys: e.target.checked })} /> all keys (~*)</label></div>
      {!m.allkeys && <div class="chips">
        {m.keys.map((kk, i) => (
          <span class="chip" key={i}>{kk.pat}
            <select value={kk.mode} onChange={(e) => setKeyMode(i, e.target.value)}>
              <option>RW</option><option>R</option><option>W</option></select>
            <button class="cx" onClick={() => up({ keys: m.keys.filter((_, j) => j !== i) })}>×</button></span>
        ))}
        <span class="chipadd"><input placeholder="key:pattern:*" value={k}
          onInput={(e) => setK(e.target.value)} onKeyDown={(e) => e.key === 'Enter' && addKey()} />
          <button onClick={addKey}>+</button></span>
      </div>}

      <div class="arow"><span class="alabel">channels</span>
        <label class="chk"><input type="checkbox" checked={m.allchannels}
          onInput={(e) => up({ allchannels: e.target.checked })} /> all channels (&amp;*)</label></div>
      {!m.allchannels && <div class="chips">
        {m.channels.map((cc, i) => (
          <span class="chip" key={i}>{cc}
            <button class="cx" onClick={() => up({ channels: m.channels.filter((_, j) => j !== i) })}>×</button></span>
        ))}
        <span class="chipadd"><input placeholder="channel:*" value={ch}
          onInput={(e) => setCh(e.target.value)} onKeyDown={(e) => e.key === 'Enter' && addChan()} />
          <button onClick={addChan}>+</button></span>
      </div>}

      <div class="arow"><span class="alabel">commands</span>
        <div class="seg">
          <button class={m.cmdBase === 'none' ? 'on' : ''} onClick={() => up({ cmdBase: 'none' })}>deny all</button>
          <button class={m.cmdBase === 'all' ? 'on' : ''} onClick={() => up({ cmdBase: 'all' })}>allow all</button></div>
        <span class="dim small">base {m.cmdBase === 'all' ? '+@all' : '-@all'} — click categories to override</span></div>
      <div class="cats">
        {cats.map((c) => (
          <button key={c} class={'cat ' + (m.cats[c] === '+' ? 'allow' : m.cats[c] === '-' ? 'deny' : '')}
            onClick={() => cycCat(c)} title="click: neutral → allow → deny">
            <span class="cs">{m.cats[c] === '+' ? '+' : m.cats[c] === '-' ? '−' : '·'}</span>{c}</button>
        ))}
      </div>
      {Object.keys(m.cmds).length > 0 && <div class="chips">
        {Object.entries(m.cmds).map(([c, s]) => (
          <span class={'chip ' + (s === '+' ? 'callow' : 'cdeny')} key={c}>{s}{c}
            <button class="cx" onClick={() => rmCmd(c)}>×</button></span>
        ))}</div>}
      <div class="chipadd solo"><input placeholder="individual command (e.g. flushdb)" value={cmd}
        onInput={(e) => setCmd(e.target.value)} />
        <button onClick={() => addCmd('+')}>allow</button><button onClick={() => addCmd('-')}>deny</button></div>

      <div class="apreview"><span class="dim small">rule preview</span>
        <code>{buildAcl(m).slice(1).join(' ')}</code></div>
    </div>
  )
}

// ---------- Admin: ACL users + memory + a saveable command log (needs dashboard-admin) ----------
function Admin({ snap }) {
  const [users, setUsers] = useState([])
  const [sel, setSel] = useState(null)      // editing username, or '' for new, or null
  const [name, setName] = useState('')
  const [m, setM] = useState(null)          // parsed ACL model for the form
  const [cats, setCats] = useState([])      // ACL CAT categories
  const [msg, setMsg] = useState('')
  const [gate, setGate] = useState('')      // set if dashboard-admin is off
  const [log, setLog] = useState([])        // emitted commands, saveable
  const [memIn, setMemIn] = useState('')
  const [memMsg, setMemMsg] = useState('')

  // run a command AND record it in the log so the admin can save the setup script
  const runLogged = async (args, human) => {
    const v = await exec(args)
    if (v.t !== 'error') setLog((l) => [...l, human || args.join(' ')])
    return v
  }

  const refresh = useCallback(async () => {
    const v = await exec(['ACL', 'LIST'])
    if (v.t === 'error') { setGate(v.v); setUsers([]); return }
    setGate('')
    setUsers(rarr(v).map((u) => {
      const a = rarr(u).map(rval) // ["ACL","SETUSER",name,"reset","on",...rules]
      return { name: a[2], on: a.includes('on'), rules: a.slice(3).join(' ') }
    }))
  }, [])
  useEffect(() => { refresh(); exec(['ACL', 'CAT']).then((v) => setCats(rarr(v).map(rval).sort())) }, [])

  const editUser = (u) => { setSel(u.name); setName(u.name); setM(parseAcl(u.rules)); setMsg('') }
  const newUser = () => { setSel(''); setName(''); setM(parseAcl('on -@all')); setMsg('') }
  const save = async () => {
    if (!name.trim()) { setMsg('username required'); return }
    const toks = buildAcl(m)
    const v = await runLogged(['ACL', 'SETUSER', name.trim(), ...toks])
    setMsg(v.t === 'error' ? v.v : '✓ saved ' + name.trim())
    if (v.t !== 'error') setSel(name.trim())
    refresh()
  }
  const del = async (n) => {
    const v = await runLogged(['ACL', 'DELUSER', n])
    setMsg(v.t === 'error' ? v.v : '✓ deleted ' + n)
    if (sel === n) { setSel(null); setName(''); setRules('') }
    refresh()
  }

  // memory (from the live metrics) + set maxmemory (bump OR shrink)
  const used = snap && snap.cur ? snap.cur.mem : 0
  const maxm = snap && snap.cur ? snap.cur.maxmem : 0
  const pct = maxm > 0 ? Math.min(100, used / maxm * 100) : 0
  const setMax = async () => {
    const b = parseBytes(memIn)
    if (b == null) { setMemMsg('bad value — try 512mb, 2gb, or 0'); return }
    const v = await runLogged(['CONFIG', 'SET', 'maxmemory', '' + b])
    setMemMsg(v.t === 'error' ? v.v : (b === 0 ? '✓ maxmemory: unlimited' : '✓ maxmemory → ' + fmtBytes(b)))
    setMemIn('')
  }
  const copyLog = () => navigator.clipboard && navigator.clipboard.writeText(log.join('\n'))

  if (gate) return <div class="panel"><div class="dim" style="padding:1rem">{gate}</div></div>
  return (
    <div>
      <div class="panel">
        <div class="title">memory</div>
        <div class="qstat">
          <span><b>{fmtBytes(used)}</b> used</span>
          <span class="dim">of {maxm > 0 ? fmtBytes(maxm) : 'unlimited'}{maxm > 0 ? ' (' + pct.toFixed(1) + '%)' : ''}</span>
        </div>
        <div class="membar"><div class="memfill" style={'width:' + (maxm > 0 ? pct : 0) + '%'} /></div>
        <div class="pgargs">
          <label style="flex:1">set maxmemory
            <input value={memIn} placeholder="e.g. 512mb · 2gb · 0 = unlimited" style="flex:1"
              onInput={(e) => setMemIn(e.target.value)} onKeyDown={(e) => e.key === 'Enter' && setMax()} /></label>
          <button class="run" onClick={setMax}>Apply ▶</button>
          {memMsg && <span class={'small ' + (memMsg[0] === '✓' ? 'up' : 'err')}>{memMsg}</span>}
        </div>
      </div>

      <div class="kwrap">
        <div class="kside">
          <div class="qbar">
            <span class="dim small">{users.length} users</span>
            <button class="mini" title="new user" onClick={newUser}>＋</button>
            <button class="mini" title="refresh" onClick={refresh}>⟳</button>
          </div>
          <div class="klist">
            {users.map((u) => (
              <div key={u.name} class={'krow ' + (sel === u.name ? 'on' : '')} onClick={() => editUser(u)}>
                <span class="kn">{u.name}</span>
                <span class={'kt ' + (u.on ? 'kt-list' : '')}>{u.on ? 'on' : 'off'}</span>
              </div>
            ))}
          </div>
        </div>
        <div class="kmain">
          {sel === null && <div class="dim" style="padding:2rem">select a user, or ＋ to create one</div>}
          {sel !== null && (
            <div class="panel">
              <div class="khead">
                <span class="ktitle">{sel === '' ? 'new user' : sel}</span>
                {sel && sel !== 'default' && <button class="del" onClick={() => del(sel)}>Delete</button>}
              </div>
              {sel === '' && <div>
                <label class="dim small">username</label>
                <input class="qsearch wide" spellcheck={false} value={name}
                  onInput={(e) => setName(e.target.value)} placeholder="username" />
              </div>}
              {m && <AclForm m={m} set={setM} cats={cats} />}
              <div class="pgargs">
                <button class="run" onClick={save}>{sel === '' ? 'Create ▶' : 'Save ▶'}</button>
                {msg && <span class={'small ' + (msg[0] === '✓' ? 'up' : 'err')}>{msg}</span>}
              </div>
            </div>
          )}
        </div>
      </div>

      <div class="panel">
        <div class="title">emitted commands <span class="dim">— every admin action, save it as a setup script</span>
          <button class="mini" title="copy" onClick={copyLog}>copy</button>
          <button class="mini" title="clear" onClick={() => setLog([])}>✕</button></div>
        <pre class="cmdlog">{log.length ? log.join('\n') : '# actions you take here appear as commands you can save'}</pre>
      </div>
    </div>
  )
}

// theme picker + uploader — custom themes live in the dreads keyspace (hash dash:themes),
// the active one in dash:theme:active, so they persist server-side across browsers.
function ThemePicker() {
  const [name, setName] = useState('default')
  const [custom, setCustom] = useState({})
  const [open, setOpen] = useState(false)
  const [eName, setEName] = useState('')
  const [eJson, setEJson] = useState('')
  const [msg, setMsg] = useState('')
  const all = { ...THEMES, ...custom }

  const load = useCallback(async () => {
    const a = rarr(await exec(['HGETALL', 'dash:themes'])), c = {}
    for (let i = 0; i + 1 < a.length; i += 2) { try { c[rval(a[i])] = JSON.parse(rval(a[i + 1])) } catch (e) {} }
    setCustom(c)
    const act = rval(await exec(['GET', 'dash:theme:active'])) || 'default'
    const t = { ...THEMES, ...c }
    if (t[act]) { setName(act); applyTheme(t[act]) }
  }, [])
  useEffect(() => { load() }, [])

  const pick = async (n) => { setName(n); applyTheme(all[n]); await exec(['SET', 'dash:theme:active', n]) }
  const openEdit = () => {
    setEName(name in THEMES ? '' : name)
    setEJson(JSON.stringify(all[name] || THEMES.default, null, 2))
    setMsg(''); setOpen(true)
  }
  const save = async () => {
    const n = eName.trim()
    if (!n) { setMsg('name required'); return }
    if (n in THEMES) { setMsg('that is a built-in name'); return }
    let vars; try { vars = JSON.parse(eJson) } catch (e) { setMsg('invalid JSON'); return }
    const v = await exec(['HSET', 'dash:themes', n, JSON.stringify(vars)])
    if (v.t === 'error') { setMsg(v.v); return }
    setCustom((c) => ({ ...c, [n]: vars })); setName(n); applyTheme(vars)
    await exec(['SET', 'dash:theme:active', n]); setOpen(false)
  }
  const del = async () => {
    if (name in THEMES) return
    await exec(['HDEL', 'dash:themes', name])
    setCustom((c) => { const x = { ...c }; delete x[name]; return x }); pick('default'); setOpen(false)
  }

  return (
    <div class="themebar">
      <select class="themesel" value={name} onChange={(e) => pick(e.target.value)}>
        {Object.keys(all).map((n) => <option key={n} value={n}>{n}</option>)}
      </select>
      <button class="mini big" title="edit / upload theme" onClick={openEdit}>🎨</button>
      {open && (
        <div class="themeedit">
          <div class="title">theme editor <span class="dim small">— saved to the dreads keyspace</span></div>
          <input class="qsearch wide" placeholder="theme name (custom)" value={eName}
            onInput={(e) => setEName(e.target.value)} />
          <textarea class="themejson" spellcheck={false} value={eJson} onInput={(e) => setEJson(e.target.value)} />
          <div class="swatches">
            {(() => { try { const v = JSON.parse(eJson); return THEME_VARS.map((k) =>
              <span class="sw" key={k} title={k} style={'background:' + (v[k] || '#000')} />) } catch (e) { return null } })()}
          </div>
          <div class="pgargs">
            <button class="run" onClick={save}>Save to dreads ▶</button>
            {!(name in THEMES) && <button class="del" onClick={del}>Delete “{name}”</button>}
            <button class="mini big" onClick={() => setOpen(false)}>close</button>
            {msg && <span class="err small">{msg}</span>}
          </div>
        </div>
      )}
    </div>
  )
}

function Login({ onOk }) {
  const [pw, setPw] = useState('')
  const [err, setErr] = useState('')
  const [busy, setBusy] = useState(false)
  const submit = async () => {
    setBusy(true); setErr(''); AUTH = pw
    const v = await exec(['PING'])
    setBusy(false)
    if (v.t === 'error') { AUTH = ''; setErr('wrong password') }
    else { sessionStorage.setItem('dash-auth', pw); onOk() }
  }
  return (
    <div class="login">
      <div class="loginbox">
        <h1>dreads <span class="zap">⚡</span></h1>
        <div class="dim small" style="margin-bottom:1rem">dashboard is password protected</div>
        <input type="password" autofocus placeholder="password" value={pw}
          onInput={(e) => setPw(e.target.value)} onKeyDown={(e) => e.key === 'Enter' && submit()} />
        <button class="run" style="width:100%;margin:.6rem 0 0" onClick={submit} disabled={busy}>
          {busy ? '…' : 'Sign in'}</button>
        {err && <div class="err small" style="margin-top:.5rem">{err}</div>}
      </div>
    </div>
  )
}

function App() {
  const [authed, setAuthed] = useState(null) // null=probing · false=login · true=ok
  useEffect(() => {
    onAuthFail = () => setAuthed(false)
    exec(['PING']).then((v) => { if (v.t !== 'error') setAuthed(true) })
  }, [])
  if (authed === null) return null
  if (!authed) return <Login onOk={() => setAuthed(true)} />
  return <Dashboard />
}

function Dashboard() {
  const snap = useMetrics()
  const [tab, setTab] = useState('overview')
  const logout = () => { AUTH = ''; sessionStorage.removeItem('dash-auth'); location.reload() }
  return (
    <div class="wrap">
      <header>
        <h1>dreads <span class="zap">⚡</span> dashboard</h1>
        <nav>
          {['overview', 'console', 'keys', 'pubsub', 'queues', 'streams', 'playground', 'admin'].map((t) => (
            <button key={t} class={'tab ' + (tab === t ? 'on' : '')} onClick={() => setTab(t)}>
              {t === 'playground' ? 'lua playground' : t}</button>
          ))}
        </nav>
        <span class={'st ' + (snap.live ? 'live' : 'off')}>{snap.live ? 'live' : 'offline'}</span>
        <ThemePicker />
        {AUTH && <button class="mini big" style="margin-left:.6rem" onClick={logout}>logout</button>}
      </header>
      {tab === 'overview' && <Overview snap={snap} />}
      {tab === 'console' && <Console />}
      {tab === 'keys' && <Keys />}
      {tab === 'pubsub' && <Pubsub />}
      {tab === 'queues' && <Queues />}
      {tab === 'streams' && <Streams />}
      {tab === 'playground' && <Playground />}
      {tab === 'admin' && <Admin snap={snap} />}
    </div>
  )
}

render(<App />, document.getElementById('app'))
