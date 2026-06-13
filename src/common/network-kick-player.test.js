/**
 * v2.1 P1 host 主动踢人 —— transport + network 集成单测
 *
 * v2.1 owner steer 修正:
 *   - **不**改 network.js 内部 peers Map / lastHeartbeat(留给心跳 6-8s)
 *   - transport forceDisconnectSeat 只做"真断连接"+"broadcast PEER_LEAVE { kick: true }"
 *   - host UI 立即反映由调用方 (RoomView) 同步改 reactive peers Map
 *
 * 覆盖三路径对称:
 *   - WebSocketTransport.forceDisconnectSeat(seat):
 *       立即 _clients.delete + ws.close() + broadcast PEER_LEAVE { kick: true } 给其它 joiner
 *   - AndroidWsTransport.forceDisconnectSeat(seat):
 *       broadcast PEER_LEAVE { kick: true } + 调 WsServer.closeClient({ seat })
 *   - BroadcastChannelTransport.forceDisconnectSeat(seat):
 *       broadcast PEER_LEAVE { kick: true } 给所有其它 tab
 *
 * 网络层集成(BC 路径):
 *   - host 调 transport.forceDisconnectSeat → 其它 joiner 收到 PEER_LEAVE { kick: true, seat }
 *   - 被踢 joiner (seat===selfSeat && kick=true) → 触发 self:kicked 事件
 *   - 其它 joiner (seat !== selfSeat) → 旁观者,正常 peers.delete + peer:leave
 *
 * v2.1 心跳路径**不**被 kick 触发:
 *   - host 端 peers Map 在 kick 后**仍**保留该 seat (因为我们不动它)
 *   - 6-8s 后 _tickHeartbeatChecker 才自动清理(已被 multitab test 覆盖)
 */

let pass = 0, fail = 0
function assert(name, cond) {
  if (cond) { console.log(`  ✓ ${name}`); pass++ }
  else { console.log(`  ✗ ${name}`); fail++ }
}
function eq(name, actual, expected) {
  const a = JSON.stringify(actual)
  const e = JSON.stringify(expected)
  if (a === e) { console.log(`  ✓ ${name}`); pass++ }
  else { console.log(`  ✗ ${name}  期望=${e} 实际=${a}`); fail++ }
}

async function makeFakeInstance(tag, fixedUuid) {
  const url = './network.js?tag=' + tag + '&t=' + Date.now() + '_' + Math.random()
  const mod = await import(url)
  const captured = { intervals: [], timeouts: [], cleared: [] }

  if (fixedUuid !== undefined) {
    globalThis.sessionStorage = {
      _store: { 'guandan_session_uuid': fixedUuid },
      getItem(k) { return this._store[k] || null },
      setItem(k, v) { this._store[k] = v },
    }
  }

  mod.__installFakeTimers({
    setInterval: (fn, ms) => { captured.intervals.push({ fn, ms, cancelled: false }); return captured.intervals.length },
    clearInterval: (id) => { captured.cleared.push({ type: 'interval', id }); if (id >= 1 && id <= captured.intervals.length) captured.intervals[id - 1].cancelled = true },
    setTimeout: (fn, ms) => { captured.timeouts.push({ fn, ms, cancelled: false }); return captured.timeouts.length },
    clearTimeout: (id) => { captured.cleared.push({ type: 'timeout', id }); if (id >= 1 && id <= captured.timeouts.length) captured.timeouts[id - 1].cancelled = true },
  })

  return { mod, captured }
}

function resetSessionStorage() {
  globalThis.sessionStorage = {
    _store: {},
    getItem(k) { return this._store[k] || null },
    setItem(k, v) { this._store[k] = v },
  }
}

async function settle(ms = 50) {
  await new Promise(r => setTimeout(r, ms))
}

// ============================================================
// 块 1: WebSocketTransport.forceDisconnectSeat 单元测试
// ============================================================
console.log('\n=== 1. WebSocketTransport.forceDisconnectSeat 真做(立即 _clients.delete + ws.close + broadcast PEER_LEAVE) ===')
{
  const { WebSocketTransport } = await import('./network-transport-ws.js?tag=ws-kick-' + Date.now())
  const t = new WebSocketTransport({ port: 0 })
  await t.open('self')
  const port = t.getBoundPort()

  // 模拟 3 个 joiner 连进来
  const { WebSocket } = await import('ws')
  const clients = []
  for (let i = 0; i < 3; i++) {
    const ws = new WebSocket(`ws://127.0.0.1:${port}`)
    await new Promise(r => ws.once('open', r))
    clients.push(ws)
  }
  await settle(30)

  const wsList = Array.from(t._clients.keys())
  eq('host 持有 3 个 ws client', t._clients.size, 3)
  wsList[0]._seat = 1
  wsList[1]._seat = 2
  wsList[2]._seat = 3
  t._clients.get(wsList[0]).seat = 1
  t._clients.get(wsList[1]).seat = 2
  t._clients.get(wsList[2]).seat = 3

  // 收集 joiner 收到的消息
  const recv1 = [], recv3 = []
  clients[0].on('message', (d) => { try { recv1.push(JSON.parse(d.toString())) } catch (e) {} })
  clients[2].on('message', (d) => { try { recv3.push(JSON.parse(d.toString())) } catch (e) {} })

  // ★ 调 forceDisconnectSeat(2) 踢 seat=2 的 joiner
  const result = t.forceDisconnectSeat(2)
  assert('forceDisconnectSeat(2) 返回 true', result === true)

  // 1) _clients map 立即删除
  eq('_clients.size 从 3 降到 2', t._clients.size, 2)
  assert('seat=2 已从 _clients 删除', !Array.from(t._clients.keys()).some(w => w._seat === 2))

  // 2) 不存在的 seat 返回 false
  const r2 = t.forceDisconnectSeat(99)
  assert('不存在的 seat 返回 false', r2 === false)

  // 3) 其它 joiner (seat=1, seat=3) 收到 broadcast PEER_LEAVE { kick: true }
  await settle(50)
  const leave1 = recv1.find(m => m.type === 'PEER_LEAVE')
  const leave3 = recv3.find(m => m.type === 'PEER_LEAVE')
  assert('joiner seat=1 收到 PEER_LEAVE (broadcast)', leave1 != null)
  assert('joiner seat=3 收到 PEER_LEAVE (broadcast)', leave3 != null)
  eq('PEER_LEAVE.payload.seat = 2', leave1?.payload?.seat, 2)
  eq('PEER_LEAVE.payload.kick = true', leave1?.payload?.kick, true)
  eq('PEER_LEAVE.payload.reason = kicked', leave1?.payload?.reason, 'kicked')
  eq('PEER_LEAVE.from = 0 (host)', leave1?.from, 0)

  // 4) joiner seat=2 的 ws 被关
  await settle(100)
  assert('joiner seat=2 的 ws 被关闭', clients[1].readyState === 3 /* CLOSED */)
  assert('joiner seat=1 仍 OPEN', clients[0].readyState === 1)
  assert('joiner seat=3 仍 OPEN', clients[2].readyState === 1)

  // 5) 验证 transport 不 emit _DISCONNECT 给 host network.js(v2.1 owner steer)
  //    transport 只做真断 + broadcast,host 端 peers Map 由心跳 6-8s 后释放
  //    此处无法直接验证 network.js 不处理 _DISCONNECT(单元测试层面),留给集成测试覆盖

  for (const ws of clients) try { ws.close() } catch (e) {}
  await settle(30)
  t.close()
}

console.log('\n=== 2. WebSocketTransport.forceDisconnectSeat 在 client mode 下是 no-op ===')
{
  const { WebSocketTransport } = await import('./network-transport-ws.js?tag=ws-kick-client-' + Date.now())
  const t = new WebSocketTransport()
  t._mode = 'client'
  const r = t.forceDisconnectSeat(2)
  assert('client mode 返回 false (不踢自己)', r === false)
  t.close()
}

// ============================================================
// 块 3: AndroidWsTransport.forceDisconnectSeat
// ============================================================
console.log('\n=== 3. AndroidWsTransport.forceDisconnectSeat 真做(plugin.broadcast + plugin.closeClient) ===')
{
  // 必须在 ws-server.js 的 web fallback 闭包捕获 log 引用之前设上
  globalThis.__wsServerTestLog = []
  if (!globalThis.window) globalThis.window = {}
  const origCap = globalThis.window.Capacitor
  globalThis.window.Capacitor = {
    isNativePlatform: () => true,
    Plugins: { WsServer: {} },
  }

  try {
    const { AndroidWsTransport } = await import('./network-transport-android-ws.js?tag=android-kick-' + Date.now())
    const t = new AndroidWsTransport({ port: 0 })
    t._mode = 'self'  // 假装是 host (跳过 isNativeCapacitor 检查)

    const r = t.forceDisconnectSeat(2)
    assert('AndroidWsTransport forceDisconnectSeat(2) 返回 true', r === true)

    await settle(20)
    const log = globalThis.__wsServerTestLog
    const broadcastCall = log.find(e => e.m === 'broadcast')
    const closeCall = log.find(e => e.m === 'closeClient')
    assert('plugin.broadcast 被调用', broadcastCall != null)
    assert('plugin.closeClient 被调用', closeCall != null)
    eq('plugin.closeClient 收到 seat=2', closeCall?.opts, { seat: 2 })
    // broadcast 的 message 含 PEER_LEAVE { kick: true }
    const msg = broadcastCall?.opts?.message
    assert('broadcast message 含 PEER_LEAVE', typeof msg === 'string' && msg.indexOf('"PEER_LEAVE"') >= 0)
    assert('broadcast message 含 kick:true', typeof msg === 'string' && msg.indexOf('"kick":true') >= 0)
    assert('broadcast message 含 reason:kicked', typeof msg === 'string' && msg.indexOf('"reason":"kicked"') >= 0)

    // 不存在的 seat 也返回 true(plugin 永远被调,room 自带防骚扰)
    const logBefore = log.length
    const r2 = t.forceDisconnectSeat(99)
    assert('不存在的 seat 也返回 true', r2 === true)
    await settle(20)  // 等 microtask:plugin.broadcast + closeClient 是 async
    const newCalls = log.slice(logBefore)
    assert('plugin.broadcast + closeClient 各被调 1 次 (kick 99)',
      newCalls.filter(e => e.m === 'broadcast').length === 1 && newCalls.filter(e => e.m === 'closeClient').length === 1)
  } finally {
    globalThis.window.Capacitor = origCap
  }
}

console.log('\n=== 4. AndroidWsTransport.forceDisconnectSeat 在 client mode 下是 no-op ===')
{
  globalThis.__wsServerTestLog = []
  if (!globalThis.window) globalThis.window = {}
  const origCap = globalThis.window.Capacitor
  globalThis.window.Capacitor = { isNativePlatform: () => true, Plugins: { WsServer: {} } }

  try {
    const { AndroidWsTransport } = await import('./network-transport-android-ws.js?tag=android-kick-client-' + Date.now())
    const t = new AndroidWsTransport({ port: 0 })
    t._mode = 'client'
    const r = t.forceDisconnectSeat(2)
    assert('client mode 返回 false', r === false)
    assert('client mode plugin 不被调', globalThis.__wsServerTestLog.length === 0)
  } finally {
    globalThis.window.Capacitor = origCap
  }
}

// ============================================================
// 块 5: BroadcastChannelTransport.forceDisconnectSeat
// ============================================================
console.log('\n=== 5. BroadcastChannelTransport.forceDisconnectSeat 真做(broadcast PEER_LEAVE { kick: true }) ===')
{
  const { BroadcastChannelTransport } = await import('./network-transport-bc.js?tag=bc-kick-' + Date.now())

  if (typeof BroadcastChannel === 'undefined') {
    console.log('  - 跳过:Node24 BroadcastChannel 全局不可用')
  } else {
    const tHost = new BroadcastChannelTransport()
    const tJoiner1 = new BroadcastChannelTransport()

    await tHost.open('self', 'kick-bc-test-unit')
    await tJoiner1.open('client', 'kick-bc-test-unit')

    const j1Recv = []
    tJoiner1.onMessage((msg) => j1Recv.push(msg))

    // host 调 forceDisconnectSeat(2)
    const r = tHost.forceDisconnectSeat(2)
    assert('BC forceDisconnectSeat(2) 返回 true', r === true)

    // joiner 收到 broadcast PEER_LEAVE { kick: true }
    await settle(50)
    const leaveMsg = j1Recv.find(m => m.type === 'PEER_LEAVE')
    assert('BC joiner 收到 PEER_LEAVE', leaveMsg != null)
    eq('BC PEER_LEAVE.payload.seat = 2', leaveMsg?.payload?.seat, 2)
    eq('BC PEER_LEAVE.payload.kick = true', leaveMsg?.payload?.kick, true)
    eq('BC PEER_LEAVE.from = 0 (host)', leaveMsg?.from, 0)

    // 验证 host 不应收到自己 broadcast 的回环(BC spec)
    const hostRecv = []
    tHost.onMessage((msg) => hostRecv.push(msg))
    tHost.forceDisconnectSeat(3)
    await settle(30)
    assert('BC host 不回环收到 PEER_LEAVE', hostRecv.length === 0)

    // channel 没初始化时返回 false
    const tNoCh = new BroadcastChannelTransport()
    const rNoCh = tNoCh.forceDisconnectSeat(2)
    assert('BC channel 未开时 forceDisconnectSeat 返回 false', rNoCh === false)

    tHost.close(); tJoiner1.close()
  }
}

// ============================================================
// 块 6: 网络层集成(BC 路径)— 被踢 joiner 触发 self:kicked
// ============================================================
console.log('\n=== 6. 集成(BC):host 调 forceDisconnectSeat(seat=1) → 被踢 joiner self:kicked 触发 ===')
{
  resetSessionStorage()
  const { mod: Host } = await makeFakeInstance('h-kick-int', 'h-uuid-kick-int')
  Host.setRoomId('multitab-kick-int')
  Host.startAsHost({ nickname: 'H', avatar: 'H' })
  await settle()

  // 三个 joiner (BC 路径分配 seat=1/2/3)
  const { mod: J1 } = await makeFakeInstance('j1-kick-int', 'j1-uuid-kick-int')
  J1.joinRoom('multitab-kick-int', { nickname: 'A', avatar: 'A' })
  let s1 = -1
  for (let i = 0; i < 50 && s1 === -1; i++) { await settle(10); s1 = J1.getSelfSeat() }
  assert('J1 seat=1', s1 === 1)

  const { mod: J2 } = await makeFakeInstance('j2-kick-int', 'j2-uuid-kick-int')
  J2.joinRoom('multitab-kick-int', { nickname: 'B', avatar: 'B' })
  let s2 = -1
  for (let i = 0; i < 50 && s2 === -1; i++) { await settle(10); s2 = J2.getSelfSeat() }
  assert('J2 seat=2', s2 === 2)

  const { mod: J3 } = await makeFakeInstance('j3-kick-int', 'j3-uuid-kick-int')
  J3.joinRoom('multitab-kick-int', { nickname: 'C', avatar: 'C' })
  let s3 = -1
  for (let i = 0; i < 50 && s3 === -1; i++) { await settle(10); s3 = J3.getSelfSeat() }
  assert('J3 seat=3', s3 === 3)
  assert('host peers.size = 4', Host.getPeers().size === 4)

  // host 视角监听 peer:leave(由其它 joiner 离开触发,host 端通过 joiner 自身 close 后 6-8s 才到)
  // 这里我们验证 host 的 peers Map 在 kick 后**仍保留**该 seat (v2.1 owner steer)
  // 因为 host 调 forceDisconnectSeat 不动 host 端 network.js 的 peers Map
  // 由 _tickHeartbeatChecker 6-8s 后释放(已在 multitab test 覆盖)
  const beforeKick = Host.getPeers().size

  // J1 (seat=1) 监听 self:kicked
  let j1SelfKicked = null
  J1.on('self:kicked', (e) => { j1SelfKicked = e })

  // J2 / J3 监听 peer:leave(旁观者,被踢的人从列表消失)
  let j2LeaveSeat = null
  let j3LeaveSeat = null
  J2.on('peer:leave', (e) => { j2LeaveSeat = e?.seat })
  J3.on('peer:leave', (e) => { j3LeaveSeat = e?.seat })

  // ★ host 调 transport.forceDisconnectSeat(1) 踢 J1
  const r = Host._getTransport().forceDisconnectSeat(1)
  assert('forceDisconnectSeat(1) 返回 true', r === true)
  await settle(100)

  // 1) J1 端:self:kicked 触发(seat=1 是自己 + kick:true)
  assert('J1 self:kicked 触发 (seat=1 是自己)', j1SelfKicked != null)
  assert('J1 self:kicked.reason = kicked', j1SelfKicked?.reason === 'kicked')

  // 2) J2 / J3 端:peer:leave seat=1 触发(旁观者收到 broadcast PEER_LEAVE)
  assert('J2 peer:leave seat=1 触发', j2LeaveSeat === 1)
  assert('J3 peer:leave seat=1 触发', j3LeaveSeat === 1)

  // 3) v2.1 owner steer: host 端 peers Map **不**被 kick 立即清 (留给 6-8s 心跳)
  assert('host peers.size 仍为 4 (owner steer: 不立即清,留给心跳)', Host.getPeers().size === beforeKick)
  assert('host peers 仍含 seat=1 (UI 层在 RoomView 里改,network.js 内部不动)', Host.getPeers().has(1))

  J1.off('self:kicked'); J2.off('peer:leave'); J3.off('peer:leave')
  J1.close(); J2.close(); J3.close(); Host.close()
  await settle(50)
}

// ============================================================
// 块 7: 集成(host 踢不存在的 seat → 不影响任何 joiner)
// ============================================================
console.log('\n=== 7. 集成:host 调 forceDisconnectSeat(99) 不存在的 seat → 静默 no-op (旁观 joiner 不收到 PEER_LEAVE) ===')
{
  resetSessionStorage()
  const { mod: Host } = await makeFakeInstance('h-kick-noop', 'h-uuid-noop')
  Host.setRoomId('multitab-kick-noop')
  Host.startAsHost({ nickname: 'H', avatar: 'H' })
  await settle()

  const { mod: J1 } = await makeFakeInstance('j1-kick-noop', 'j1-uuid-noop')
  J1.joinRoom('multitab-kick-noop', { nickname: 'A', avatar: 'A' })
  let s1 = -1
  for (let i = 0; i < 50 && s1 === -1; i++) { await settle(10); s1 = J1.getSelfSeat() }
  assert('J1 seat=1', s1 === 1)

  let leaveCount = 0
  let selfKickedCount = 0
  J1.on('peer:leave', () => leaveCount++)
  J1.on('self:kicked', () => selfKickedCount++)

  // 踢不存在的 seat (BC 路径 — 永远返回 true 但因为没人在 seat=99,joiner 不会响应)
  const r = Host._getTransport().forceDisconnectSeat(99)
  assert('BC 路径 forceDisconnectSeat(99) 返回 true', r === true)
  await settle(100)
  assert('J1 收到 peer:leave 0 次 (无 false alarm)', leaveCount === 0)
  assert('J1 没收到 self:kicked (没人被踢)', selfKickedCount === 0)

  J1.off('peer:leave'); J1.off('self:kicked')
  J1.close(); Host.close()
  await settle(30)
}

// ============================================================
// 块 8: 集成(self:kicked 事件 payload 完整性)
// ============================================================
console.log('\n=== 8. 集成:self:kicked 事件 payload 含 reason 字段 ===')
{
  resetSessionStorage()
  const { mod: Host } = await makeFakeInstance('h-kick-payload', 'h-uuid-payload')
  Host.setRoomId('multitab-kick-payload')
  Host.startAsHost({ nickname: 'H', avatar: 'H' })
  await settle()

  const { mod: J1 } = await makeFakeInstance('j1-kick-payload', 'j1-uuid-payload')
  J1.joinRoom('multitab-kick-payload', { nickname: 'A', avatar: 'A' })
  let s1 = -1
  for (let i = 0; i < 50 && s1 === -1; i++) { await settle(10); s1 = J1.getSelfSeat() }
  assert('J1 seat=1', s1 === 1)

  let capturedPayload = null
  J1.on('self:kicked', (e) => { capturedPayload = e })

  Host._getTransport().forceDisconnectSeat(1)
  await settle(100)

  assert('self:kicked 事件触发', capturedPayload != null)
  assert('payload.reason = kicked', capturedPayload?.reason === 'kicked')

  J1.off('self:kicked')
  J1.close(); Host.close()
  await settle(30)
}

console.log(`\n========== 测试结果: ${pass} 通过 / ${fail} 失败 ==========`)
if (fail > 0) process.exit(1)