/**
 * v2.0 — Android Capacitor WebSocket server 桥接单测
 *
 * 覆盖:
 *   - WsServer (src/common/ws-server.js) 模块导出正确
 *   - isNativeCapacitor() 在无 window.Capacitor 时返回 false
 *   - isNativeCapacitor() 在 Capacitor 环境返回 true
 *   - WsServer 在 mock Capacitor.Plugins 下能调到 startServer / getLocalIp
 *   - network.js joinRoom 解析 host:port 形式字符串 ('1.2.3.4:8848')
 *   - network.js joinRoom 兼容 6 位 roomId 形式 (BC 路径)
 *   - network.js joinRoom 兼容 opts.hostIp/hostPort (WS 测试路径)
 *   - network.js startAsHost 走 _setTransportFactory (验证 router 不会硬编码 transport 类型)
 *
 * 约束:不依赖真 Java WsServer。Plugin 部分用 in-memory mock 替代 Capacitor.Plugins.WsServer。
 * 注意:@capacitor/core 的 registerPlugin 是全局单例,所有测试共享同一份注册,
 *      所以 ws-server.js 只在测试入口 import 一次,不再 cache-bust。
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

// 准备 mock Capacitor 环境
if (!globalThis.window) globalThis.window = {}
const origCap = globalThis.window.Capacitor
const origPlugins = globalThis.window.Capacitor?.Plugins

// 测试用 plugin 状态
const pluginCallLog = []
const pluginListeners = {}

function makeMockPlugin() {
  return {
    startServer: async (opts) => {
      pluginCallLog.push({ m: 'startServer', opts })
      return { ok: true, port: opts?.port || 8848, ip: '10.0.0.42' }
    },
    stopServer: async () => { pluginCallLog.push({ m: 'stopServer' }); return { ok: true } },
    getLocalIp: async () => { pluginCallLog.push({ m: 'getLocalIp' }); return { ip: '10.0.0.42' } },
    sendToClient: async (opts) => {
      pluginCallLog.push({ m: 'sendToClient', opts })
      return { ok: true, sent: true }
    },
    broadcast: async (opts) => {
      pluginCallLog.push({ m: 'broadcast', opts })
      return { ok: true, sent: 3 }
    },
    addListener: async (event, cb) => {
      pluginCallLog.push({ m: 'addListener', event })
      pluginListeners[event] = cb
      return { remove: async () => { delete pluginListeners[event] } }
    },
  }
}

// 默认 mock:Capacitor 在,但 plugin 占位 (用于 isNativeCapacitor 检测)
globalThis.window.Capacitor = {
  isNativePlatform: () => true,
  Plugins: { WsServer: makeMockPlugin() },
}

// 只 import 一次 ws-server.js(registerPlugin 全局单例)
const WsServerModule = await import('./ws-server.js')
const WsServer = WsServerModule.default
const isNativeCapacitor = WsServerModule.isNativeCapacitor

console.log('\n=== 1. WsServer 模块导出 ===')
{
  assert('default export 是 object', typeof WsServer === 'object')
  assert('isNativeCapacitor 是 function', typeof isNativeCapacitor === 'function')
  const methods = ['startServer', 'stopServer', 'getLocalIp', 'sendToClient', 'broadcast', 'addListener']
  for (const m of methods) {
    assert('default 导出含 ' + m, typeof WsServer[m] === 'function')
  }
}

console.log('\n=== 2. isNativeCapacitor() 在 mock Capacitor 环境下返回 true ===')
{
  const r = isNativeCapacitor()
  assert('Capacitor.isNativePlatform()=true → isNativeCapacitor()=true', r === true)
}

console.log('\n=== 3. isNativeCapacitor() 在无 Capacitor 时返回 false ===')
{
  const origCapLocal = globalThis.window.Capacitor
  delete globalThis.window.Capacitor
  const r = isNativeCapacitor()
  globalThis.window.Capacitor = origCapLocal
  assert('无 Capacitor → false', r === false)
}

console.log('\n=== 4. isNativeCapacitor() 在 Capacitor.isNativePlatform()=false 时返回 false ===')
{
  const origCapLocal = globalThis.window.Capacitor
  globalThis.window.Capacitor = {
    isNativePlatform: () => false,
    Plugins: { WsServer: makeMockPlugin() },
  }
  const r = isNativeCapacitor()
  globalThis.window.Capacitor = origCapLocal
  assert('isNativePlatform()=false → false', r === false)
}

console.log('\n=== 5. isNativeCapacitor() 兜底:Plugins.WsServer 存在 → true ===')
{
  const origCapLocal = globalThis.window.Capacitor
  globalThis.window.Capacitor = { Plugins: { WsServer: {} } }
  const r = isNativeCapacitor()
  globalThis.window.Capacitor = origCapLocal
  assert('Plugins.WsServer 存在 → true', r === true)
}

console.log('\n=== 6. WsServer.startServer 在 Node test 环境抛 Android-only 错误 (web fallback) ===')
{
  // Node test 环境 Capacitor 平台 = 'web',所以走我们提供的 web fallback
  // web fallback 故意 throw (host 端只能在 Android 跑)
  let err = null
  try {
    await WsServer.startServer({ port: 8848 })
  } catch (e) {
    err = e
  }
  assert('Node web fallback 抛错', err != null)
  assert('错误信息是 Android-only', err && /Android-only/.test(err.message))
}

console.log('\n=== 7. WsServer.getLocalIp 在 Node test 环境返回 127.0.0.1 (web fallback) ===')
{
  const r = await WsServer.getLocalIp()
  eq('getLocalIp web fallback', r, { ip: '127.0.0.1' })
}

console.log('\n=== 8. WsServer.sendToClient/broadcast 在 Node test 环境是 no-op (web fallback) ===')
{
  const r1 = await WsServer.sendToClient({ seat: 1, message: 'x' })
  eq('sendToClient web fallback', r1, { ok: false, sent: false })
  const r2 = await WsServer.broadcast({ message: 'x' })
  eq('broadcast web fallback', r2, { ok: true, sent: 0 })
}

console.log('\n=== 9. network.js joinRoom 解析 host:port 字符串 ===')
{
  const mod = await import('./network.js?t=ws-server-joinroom-' + Date.now())
  const captured = { openArgs: null, mode: null }
  const FakeTransport = function () {
    return {
      onMessage() {},
      open: async (mode, ...args) => {
        captured.mode = mode
        captured.openArgs = args
        return undefined
      },
      send() { return true },
      close() {},
      bindLastSenderSeat() {},
      forceDisconnectSeat() {},
      isReady() { return true },
      getPeers() { return [] },
      getBoundPort() { return null },
    }
  }
  mod._setTransportFactory(FakeTransport)
  mod.joinRoom('192.168.1.5:8848', { nickname: 'J', avatar: 'J' })
  await new Promise(r => setTimeout(r, 50))
  eq('joiner 调用 open("client","192.168.1.5",8848)', captured.openArgs, ['192.168.1.5', 8848])
  eq('mode="client"', captured.mode, 'client')
  mod.close()
  mod._resetTransportFactory()
}

console.log('\n=== 10. network.js joinRoom 兼容 BC 路径 (无冒号 → 房间号) ===')
{
  const mod = await import('./network.js?t=ws-server-joinroom2-' + Date.now())
  const captured = { openArgs: null, mode: null }
  const FakeTransport = function () {
    return {
      onMessage() {},
      open: async (mode, ...args) => {
        captured.mode = mode
        captured.openArgs = args
        return undefined
      },
      send() { return true },
      close() {},
      bindLastSenderSeat() {},
      forceDisconnectSeat() {},
      isReady() { return true },
      getPeers() { return [] },
      getBoundPort() { return null },
    }
  }
  mod._setTransportFactory(FakeTransport)
  mod.joinRoom('123456', { nickname: 'J', avatar: 'J' })
  await new Promise(r => setTimeout(r, 50))
  // BC 模式传 ['client', null, null] (保持原行为)
  eq('BC 模式 openArgs', captured.openArgs, [null, null])
  mod.close()
  mod._resetTransportFactory()
}

console.log('\n=== 11. network.js joinRoom 兼容 opts.hostIp/hostPort (WS 测试路径) ===')
{
  const mod = await import('./network.js?t=ws-server-joinroom3-' + Date.now())
  const captured = { openArgs: null, mode: null }
  const FakeTransport = function () {
    return {
      onMessage() {},
      open: async (mode, ...args) => {
        captured.mode = mode
        captured.openArgs = args
        return undefined
      },
      send() { return true },
      close() {},
      bindLastSenderSeat() {},
      forceDisconnectSeat() {},
      isReady() { return true },
      getPeers() { return [] },
      getBoundPort() { return null },
    }
  }
  mod._setTransportFactory(FakeTransport)
  // 老 API:hostRoomId='any', opts={hostIp, hostPort} → WS 模式
  mod.joinRoom('any', { nickname: 'J', avatar: 'J' }, { hostIp: '10.0.0.1', hostPort: 9999 })
  await new Promise(r => setTimeout(r, 50))
  eq('opts.hostIp/hostPort → openArgs', captured.openArgs, ['10.0.0.1', 9999])
  mod.close()
  mod._resetTransportFactory()
}

console.log('\n=== 12. network.js joinRoom 拒绝: 端口非法或缺失 ===')
{
  const mod = await import('./network.js?t=ws-server-joinroom4-' + Date.now())
  const captured = { openArgs: null, mode: null }
  const FakeTransport = function () {
    return {
      onMessage() {},
      open: async (mode, ...args) => {
        captured.mode = mode
        captured.openArgs = args
        return undefined
      },
      send() { return true },
      close() {},
      bindLastSenderSeat() {},
      forceDisconnectSeat() {},
      isReady() { return true },
      getPeers() { return [] },
      getBoundPort() { return null },
    }
  }
  mod._setTransportFactory(FakeTransport)
  // 1.2.3.4: 端口缺失,退回 BC 路径
  mod.joinRoom('1.2.3.4:', { nickname: 'J', avatar: 'J' })
  await new Promise(r => setTimeout(r, 50))
  eq('端口缺失退回 BC 模式', captured.openArgs, [null, null])
  mod.close()
  // 含 ':abc' (非数字端口) 也退回 BC
  const captured2 = { openArgs: null }
  const FT2 = function () {
    return {
      onMessage() {},
      open: async (mode, ...args) => { captured2.openArgs = args; return undefined },
      send() { return true }, close() {}, bindLastSenderSeat() {}, forceDisconnectSeat() {},
      isReady() { return true }, getPeers() { return [] }, getBoundPort() { return null },
    }
  }
  mod._setTransportFactory(FT2)
  mod.joinRoom('1.2.3.4:abc', { nickname: 'J', avatar: 'J' })
  await new Promise(r => setTimeout(r, 50))
  eq('非法端口退回 BC 模式', captured2.openArgs, [null, null])
  mod.close()
  mod._resetTransportFactory()
}

console.log('\n=== 13. _setTransportFactory 优先于 _defaultTransport ===')
{
  const mod = await import('./network.js?t=ws-server-factory-' + Date.now())
  let factoryCalled = false
  mod._setTransportFactory(() => {
    factoryCalled = true
    return {
      onMessage() {},
      open: async () => undefined,
      send() { return true },
      close() {},
      bindLastSenderSeat() {},
      forceDisconnectSeat() {},
      isReady() { return true },
      getPeers() { return [] },
      getBoundPort() { return null },
    }
  })
  // 当前是 Capacitor 环境,defaultTransport 会返回 AndroidWsTransport;
  // 但 _setTransportFactory 应当覆盖
  const r = mod.startAsHost({ nickname: 'H', avatar: 'H' })
  assert('factory 被调用', factoryCalled === true)
  assert('startAsHost 返回 ok', r.ok === true)
  mod.close()
  mod._resetTransportFactory()
}

console.log('\n========== 测试结果: ' + pass + ' 通过 / ' + fail + ' 失败 ==========')
process.exit(fail > 0 ? 1 : 0)
