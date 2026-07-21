import { render } from 'preact'
import { useState, useEffect, useRef } from 'preact/hooks'
import uPlot from 'uplot'
import 'uplot/dist/uPlot.min.css'
import './style.css'

const WINDOW = 240 // samples kept per series (~2min at 500ms)

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

// One uPlot instance, created once; setData on each update, auto-resizes to width.
function Chart({ data, series, height = 190 }) {
  const el = useRef(null)
  const plot = useRef(null)
  useEffect(() => {
    const opts = {
      width: el.current.clientWidth,
      height,
      series,
      cursor: { points: { size: 5 } },
      legend: { show: true, live: true },
      axes: [
        { stroke: '#5c6b82', grid: { stroke: '#1a2230' }, ticks: { stroke: '#1a2230' } },
        { stroke: '#5c6b82', grid: { stroke: '#1a2230' }, ticks: { stroke: '#1a2230' },
          size: 60, values: (u, vs) => vs.map(fmt) },
      ],
    }
    plot.current = new uPlot(opts, data, el.current)
    const ro = new ResizeObserver(() =>
      plot.current.setSize({ width: el.current.clientWidth, height }))
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
    const push = (arr, v) => { arr.push(v); if (arr.length > WINDOW) arr.shift() }
    const connect = () => {
      ws = new WebSocket((location.protocol === 'https:' ? 'wss' : 'ws') + '://' + location.host + '/ws')
      ws.onclose = () => { setSnap((s) => ({ ...s, live: false })); if (!stop) setTimeout(connect, 1000) }
      ws.onmessage = (e) => {
        let m; try { m = JSON.parse(e.data) } catch { return }
        if (!m.t) return
        const d = buf.current, prev = d.prev
        const dt = prev ? (m.t - prev.t) / 1000 : 0
        const rate = (a, b) => (dt > 0 ? Math.max(0, (a - b) / dt) : 0)
        if (prev && dt > 0) {
          push(d.t, m.t / 1000)
          push(d.cmds, rate(m.cmds, prev.cmds))
          push(d.str, rate(m.str, prev.str))
          push(d.list, rate(m.list, prev.list))
          push(d.stream, rate(m.stream, prev.stream))
          push(d.pub, rate(m.pub, prev.pub))
          push(d.mem, m.mem / 1048576)
        }
        d.prev = m
        setSnap({
          live: true,
          cur: m,
          ops: [d.t.slice(), d.cmds.slice(), d.str.slice(), d.list.slice(), d.stream.slice(), d.pub.slice()],
          mem: [d.t.slice(), d.mem.slice()],
        })
      }
    }
    connect()
    return () => { stop = true; if (ws) ws.close() }
  }, [])
  return snap
}

function Stat({ k, v, sub }) {
  return (
    <div class="card">
      <div class="k">{k}</div>
      <div class="v">{v}</div>
      {sub != null && <div class="r">{sub}</div>}
    </div>
  )
}

function rateOf(snap, key) {
  const o = snap.ops
  if (!o || !o[key] || o[key].length < 1) return 0
  return o[key][o[key].length - 1]
}

function App() {
  const snap = useMetrics()
  const m = snap.cur || {}
  const mm = m.maxmem > 0 ? m.maxmem : 0
  const pct = mm ? Math.min(100, (m.mem / mm) * 100) : 0
  return (
    <div class="wrap">
      <header>
        <h1>dreads <span class="zap">⚡</span> dashboard</h1>
        <span class={'st ' + (snap.live ? 'live' : 'off')}>{snap.live ? 'live' : 'offline'}</span>
      </header>

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

      <div class="panel">
        <div class="title">Operations / sec</div>
        {snap.ops && <Chart data={snap.ops} series={[
          {},
          line('cmds', '#2f81f7'),
          line('str', '#3fb950'),
          line('list', '#d29922'),
          line('stream', '#a371f7'),
          line('pub', '#f778ba'),
        ]} />}
      </div>

      <div class="panel">
        <div class="title">Memory (MB) &mdash; {bytes(m.mem)}{mm ? ' / ' + bytes(mm) : ' / ∞'}</div>
        <div class="bar"><div class="fill" style={{ width: (mm ? pct : 4) + '%' }} /></div>
        {snap.mem && <Chart data={snap.mem} height={150} series={[{}, line('used', '#2f81f7')]} />}
      </div>
    </div>
  )
}

render(<App />, document.getElementById('app'))
