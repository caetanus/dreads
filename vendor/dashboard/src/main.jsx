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
  return [{ t: 'raw', v: head }, np]
}

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

async function exec(args) {
  const res = await fetch('/api/exec', { method: 'POST', body: toResp(args) })
  if (res.status === 401) return { t: 'error', v: 'ERR unauthorized (set X-Dashboard-Auth)' }
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
      ws = new WebSocket((location.protocol === 'https:' ? 'wss' : 'ws') + '://' + location.host + '/ws')
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
  const load = (s) => setCode(s.code)
  const remove = async (sha) => { await exec(['SCRIPT', 'REMOVE', sha]); refresh() }
  return (
    <div class="pg">
      <aside class="scripts">
        <div class="title">scripts <span class="dim">({scripts.length})</span>
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
          <button class="run" onClick={run}>Run ▶</button>
        </div>
        <pre class={'reply ' + (out.cls || '')}>{out.out || 'reply appears here'}</pre>
      </div>
    </div>
  )
}

function App() {
  const snap = useMetrics()
  const [tab, setTab] = useState('overview')
  return (
    <div class="wrap">
      <header>
        <h1>dreads <span class="zap">⚡</span> dashboard</h1>
        <nav>
          {['overview', 'console', 'playground'].map((t) => (
            <button key={t} class={'tab ' + (tab === t ? 'on' : '')} onClick={() => setTab(t)}>{t}</button>
          ))}
        </nav>
        <span class={'st ' + (snap.live ? 'live' : 'off')}>{snap.live ? 'live' : 'offline'}</span>
      </header>
      {tab === 'overview' && <Overview snap={snap} />}
      {tab === 'console' && <Console />}
      {tab === 'playground' && <Playground />}
    </div>
  )
}

render(<App />, document.getElementById('app'))
