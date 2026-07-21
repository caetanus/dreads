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

function App() {
  const snap = useMetrics()
  const [tab, setTab] = useState('overview')
  return (
    <div class="wrap">
      <header>
        <h1>dreads <span class="zap">⚡</span> dashboard</h1>
        <nav>
          {['overview', 'console', 'keys', 'queues', 'playground'].map((t) => (
            <button key={t} class={'tab ' + (tab === t ? 'on' : '')} onClick={() => setTab(t)}>{t}</button>
          ))}
        </nav>
        <span class={'st ' + (snap.live ? 'live' : 'off')}>{snap.live ? 'live' : 'offline'}</span>
      </header>
      {tab === 'overview' && <Overview snap={snap} />}
      {tab === 'console' && <Console />}
      {tab === 'keys' && <Keys />}
      {tab === 'queues' && <Queues />}
      {tab === 'playground' && <Playground />}
    </div>
  )
}

render(<App />, document.getElementById('app'))
