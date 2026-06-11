/**
 * AndroidWsTransport —— 真机 / Capacitor WebView 用
 *
 * 与 src/common/network-transport-ws.js (浏览器版，用 'ws' npm) 接口完全一致。
 * 实现差异：
 *   - Host：把 server 部分 delegate 给原生 WsServer Capacitor 插件
 *     (Java-WebSocket 1.5.4, WsServer.java)。本地不持有 socket 列表，
 *     send 走 plugin.sendToClient / plugin.broadcast。
 *   - Client (joiner): WebView 自带 WebSocket 全局,直接 new WebSocket(url)。
 *   - 收到消息:Host 侧 plugin 'message' 事件携带 seat+text;
 *     Client 侧 WebSocket onmessage 携带 Event.data (text)。
 *
 * 消息格式：{type, payload, from, to?, ts} —— 跟 BC / 浏览器 WS 模式完全一致。
 *
 * 异步 open:
 *   - open('self'): 等 WsServer.startServer() resolve 才返回
 *   - open('client', hostIp, hostPort): 等 WebSocket onopen 才返回
 *   - send() 在 ready 之前会被缓存，open 成功后 flush
 *
 * bindLastSenderSeat / forceDisconnectSeat:
 *   - Host: plugin 没有给我们底层 WebSocket 句柄,无法 close 单个 seat;
 *     v1 实现 forceDisconnectSeat 是 no-op,改用 broadcast PEER_LEAVE 让
 *     joiner 自己断开 (host 自己的网络层会标记 seat 释放)。
 *   - 这种简化在 v1 是 OK 的:真机联机场景下 joiner 物理离开 = 拔 wifi,
 *     plugin 的 onClose 会发 'clientDisconnected',host 端自然清理 seat。
 */

import WsServer, { isNativeCapacitor } from './ws-server.js'

const DEFAULT_PORT = 8848

export class AndroidWsTransport {
  /**
   * @param {object} [opts]
   * @param {number} [opts.port=8848] —— server bind 端口 (Host 端)
   * @param {string} [opts.host='0.0.0.0'] —— server bind 地址 (Host 端, 仅用于 WsServer.getLocalIp 之外的诊断)
   * @param {string} [opts.path='/'] —— ws path
   */
  constructor(opts = {}) {
    this._port = opts.port != null ? opts.port : DEFAULT_PORT
    this._host = opts.host || '0.0.0.0'
    this._path = opts.path || '/'

    this._mode = null        // 'self' | 'client'
    this._ws = null          // client 端 native WebSocket
    this._listeners = []
    this._outbox = []        // ready 之前的消息队列
    this._ready = false
    this._hostIp = null      // host 启动后从 plugin.getLocalIp 拿到的实际 IP
    this._lastSenderSeat = -1 // host 端最近一次收到消息的 seat (调试用)
    this._lastSenderConnId = -1 // host 端最近一次收到消息的 conn 稳定 ID (用于 bindSeat)
    this._listenersHandle = [] // plugin addListener 句柄,用于 close 时清理
  }

  async open(mode, hostIp, hostPort) {
    if (mode === 'self') {
      return await this._openServer()
    } else if (mode === 'client') {
      if (!hostIp) throw new Error('client mode 需要 hostIp')
      return await this._openClient(hostIp, hostPort)
    } else {
      throw new Error('未知 mode: ' + mode)
    }
  }

  async _openServer() {
    this._mode = 'self'
    if (!isNativeCapacitor()) {
      throw new Error('AndroidWsTransport host mode requires Capacitor native runtime')
    }
    // 1) 启动原生 WsServer 插件
    const res = await WsServer.startServer({ port: this._port })
    if (!res || !res.ok) {
      throw new Error('WsServer.startServer failed: ' + (res && res.error || 'unknown'))
    }
    this._port = res.port
    this._hostIp = res.ip

    // 2) 订阅 plugin 事件 → 转发给 network.js
    const offConn = await WsServer.addListener('clientConnected', (data) => {
      // data.seat === -1 表示未分配 seat;data.connId 是稳定 conn 标识。
      // network.js 处理 JOIN 时会通过 bindLastSenderSeat 告知真正的 seat。
      this._lastSenderSeat = -1
      // 不在此处 emit _DISCONNECT,留到 bindLastSenderSeat 流程结束后
    })
    const offDisc = await WsServer.addListener('clientDisconnected', (data) => {
      const seat = (data && typeof data.seat === 'number') ? data.seat : -1
      this._emit({ type: '_DISCONNECT', payload: { seat }, ts: Date.now() })
    })
    const offMsg = await WsServer.addListener('message', (data) => {
      const seat = (data && typeof data.seat === 'number') ? data.seat : -1
      const connId = (data && typeof data.connId === 'number') ? data.connId : -1
      const raw = (data && typeof data.message === 'string') ? data.message : ''
      this._lastSenderSeat = seat
      this._lastSenderConnId = connId
      try {
        const msg = JSON.parse(raw)
        this._emit(msg)
      } catch (e) {
        // 非法 JSON 忽略
      }
    })
    this._listenersHandle.push(offConn, offDisc, offMsg)

    this._ready = true
    this._flushOutbox()
  }

  async _openClient(hostIp, hostPort) {
    this._mode = 'client'
    this._hostIp = hostIp
    const port = hostPort != null ? hostPort : this._port
    const url = `ws://${hostIp}:${port}${this._path}`
    return new Promise((resolve, reject) => {
      try {
        // WebView 自带 WebSocket;在 Node 测试环境(WsServer fake)无 WebSocket 时会 throw
        if (typeof WebSocket === 'undefined') {
          throw new Error('WebSocket 不支持 (Node 测试环境请用 WebSocketTransport)')
        }
        this._ws = new WebSocket(url)
      } catch (e) {
        reject(e)
        return
      }
      this._ws.onopen = () => {
        this._ready = true
        this._flushOutbox()
        resolve()
      }
      this._ws.onmessage = (event) => {
        const data = event.data
        if (typeof data !== 'string') return
        try {
          const msg = JSON.parse(data)
          this._emit(msg)
        } catch (e) {
          // 非法 JSON 忽略
        }
      }
      this._ws.onclose = () => {
        const wasReady = this._ready
        this._ready = false
        if (wasReady) {
          this._emit({ type: '_DISCONNECT', payload: { seat: -1 }, ts: Date.now() })
        }
      }
      this._ws.onerror = (e) => {
        // error 通常紧跟 close;让 close handler 发 _DISCONNECT
        // 这里只 reject 第一次连接失败
        if (!this._ready) {
          reject(new Error('WebSocket connect failed: ' + (e && e.message || 'unknown')))
        }
      }
    })
  }

  send(msg) {
    const data = JSON.stringify(msg)
    if (!this._ready) {
      this._outbox.push(data)
      return true
    }
    if (this._mode === 'self') {
      this._sendHost(data, msg)
    } else if (this._mode === 'client') {
      this._sendClient(data)
    }
    return true
  }

  async _sendHost(data, msg) {
    try {
      if (msg.to != null) {
        await WsServer.sendToClient({ seat: msg.to, message: data })
      } else {
        await WsServer.broadcast({ message: data })
      }
    } catch (e) {
      // swallow
    }
  }

  _sendClient(data) {
    if (this._ws && this._ws.readyState === 1 /* OPEN */) {
      try { this._ws.send(data) } catch (e) { /* swallow */ }
    }
  }

  _flushOutbox() {
    if (!this._ready) return
    const pending = this._outbox
    this._outbox = []
    for (const data of pending) {
      if (this._mode === 'self') {
        // 异步 flush 时没有完整 msg 对象了,统一广播
        WsServer.broadcast({ message: data }).catch(() => {})
      } else if (this._mode === 'client') {
        if (this._ws && this._ws.readyState === 1) {
          try { this._ws.send(data) } catch (e) { /* swallow */ }
        }
      }
    }
  }

  /**
   * Host:把最近一次收到消息的 conn 绑定到指定 seat (network.js 处理完 JOIN 后调用)
   * t2.1: 真正调 WsServer.bindSeat plugin,这样后续 sendToClient(seat, msg) 才能正确路由。
   * @note 失败时(例如 plugin 调用抛错或 connId 已无效)静默忽略 — 已绑定的 seat 不影响。
   */
  bindLastSenderSeat(seat) {
    if (this._mode !== 'self') return
    this._lastSenderSeat = seat
    if (this._lastSenderConnId < 0) return
    WsServer.bindSeat({ connId: this._lastSenderConnId, seat }).catch((e) => {
      // swallow: 上层 sendToClient 会因 seatMap 仍 -1 而失败,但不破坏 host 自身状态
    })
  }

  /** Host:手动断开指定 seat (v1 no-op,见文件头注释) */
  forceDisconnectSeat(seat) {
    if (this._mode !== 'self') return
    // 不主动 close,因为 plugin 没暴露 ws 句柄;
    // network.js 收到 disconnect 后会 broadcast PEER_LEAVE 让 joiner 端自行处理
  }

  async close() {
    this._ready = false
    // 清理 plugin 事件订阅
    for (const h of this._listenersHandle) {
      try { await h.remove() } catch (e) { /* swallow */ }
    }
    this._listenersHandle = []
    if (this._mode === 'self' && isNativeCapacitor()) {
      try { await WsServer.stopServer() } catch (e) { /* swallow */ }
    }
    if (this._ws) {
      try { this._ws.close() } catch (e) { /* swallow */ }
      this._ws = null
    }
    this._listeners = []
    this._mode = null
    this._lastSenderSeat = -1
    this._lastSenderConnId = -1
  }

  onMessage(cb) {
    if (typeof cb !== 'function') return
    this._listeners.push(cb)
  }

  offMessage(cb) {
    const i = this._listeners.indexOf(cb)
    if (i >= 0) this._listeners.splice(i, 1)
  }

  /** @returns {Array<{seat:number,info:any}>} —— Android host 模式下不可知 peer 列表 */
  getPeers() {
    if (this._mode !== 'self') return []
    return [] // v1:无法从 plugin 反查
  }

  /** 测试 / 诊断:server 实际绑定的端口 */
  getBoundPort() {
    return this._mode === 'self' ? this._port : null
  }

  /** 诊断:host 端 hostIp (plugin getLocalIp 拿到的) */
  getHostIp() {
    return this._hostIp
  }

  isReady() { return this._ready }

  _emit(msg) {
    for (const cb of this._listeners) {
      try { cb(msg) } catch (e) { /* swallow */ }
    }
  }
}
