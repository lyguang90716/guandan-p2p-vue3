package com.guandan.p2p;

import android.util.Log;
import com.getcapacitor.JSObject;
import com.getcapacitor.Plugin;
import com.getcapacitor.PluginCall;
import com.getcapacitor.PluginMethod;
import com.getcapacitor.annotation.CapacitorPlugin;

import org.java_websocket.WebSocket;

import java.net.URI;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Capacitor plugin exposing a native WebSocket server to the JS layer.
 *
 * JS bridge (registered via @capacitor/core registerPlugin('WsServer')):
 *   - startServer({port}): Promise<{ok, port, ip}>   (host side)
 *   - stopServer(): Promise<{ok}>
 *   - getLocalIp(): Promise<{ip}>
 *   - sendToClient({seat, message}): Promise<{ok, sent}>
 *   - broadcast({message}): Promise<{ok, sent}>
 *
 * Events emitted to the JS layer:
 *   - 'clientConnected'  {seat}    (seat = -1 means not yet assigned; host binds seat after processing JOIN)
 *   - 'clientDisconnected' {seat}
 *   - 'message'  {seat, message}  (message is a JSON string; the web layer parses it)
 *
 * The web layer is responsible for parsing message JSON and applying the
 * network.js protocol (JOIN/SYNC/HEARTBEAT/...). The native side just
 * forwards raw text frames.
 */
@CapacitorPlugin(name = "WsServer")
public class WsServerPlugin extends Plugin {

    private static final String TAG = "WsServerPlugin";

    private WsServer server;
    private final AtomicInteger nextSeat = new AtomicInteger(1); // 0 = host, 1..3 = joiners
    private boolean isStarted = false;

    @Override
    public void load() {
        super.load();
    }

    /**
     * Start the WebSocket server on the given port (default 8848).
     * @param call {port?: number}
     */
    @PluginMethod
    public synchronized void startServer(PluginCall call) {
        if (isStarted && server != null) {
            JSObject ret = new JSObject();
            ret.put("ok", true);
            ret.put("port", server.getBoundPort());
            ret.put("ip", WsServer.getLocalIpAddress());
            call.resolve(ret);
            return;
        }
        int port = call.getInt("port", 8848);
        try {
            server = new WsServer(port);
            server.setEventListener(new WsServer.EventListener() {
                @Override
                public void onClientConnected(WebSocket conn, int assignedSeat) {
                    JSObject data = new JSObject();
                    data.put("seat", assignedSeat);
                    notifyListeners("clientConnected", data);
                }
                @Override
                public void onClientDisconnected(WebSocket conn, int seat) {
                    JSObject data = new JSObject();
                    data.put("seat", seat);
                    notifyListeners("clientDisconnected", data);
                    if (seat >= 1 && seat <= 3) {
                        // release seat for re-allocation
                        if (nextSeat.get() > seat) {
                            // no-op; we keep monotonic for clarity
                        }
                    }
                }
                @Override
                public void onClientMessage(WebSocket conn, int seat, String message) {
                    JSObject data = new JSObject();
                    data.put("seat", seat);
                    data.put("message", message);
                    notifyListeners("message", data);
                }
            });
            // Bind/start on a background thread — WebSocketServer.run is blocking
            new Thread(() -> {
                try {
                    server.start();
                } catch (Exception e) {
                    Log.e(TAG, "WsServer start failed", e);
                }
            }, "WsServer-thread").start();
            // Wait briefly for the server to be listening (max 1.5s)
            long deadline = System.currentTimeMillis() + 1500;
            while (System.currentTimeMillis() < deadline) {
                if (server.isListening() && server.getBoundPort() > 0) break;
                try { Thread.sleep(20); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); break; }
            }
            if (!server.isListening() || server.getBoundPort() <= 0) {
                JSObject ret = new JSObject();
                ret.put("ok", false);
                ret.put("error", "WsServer failed to bind within 1.5s");
                call.reject("WsServer failed to bind");
                return;
            }
            isStarted = true;
            JSObject ret = new JSObject();
            ret.put("ok", true);
            ret.put("port", server.getBoundPort());
            ret.put("ip", WsServer.getLocalIpAddress());
            call.resolve(ret);
        } catch (Exception e) {
            Log.e(TAG, "startServer exception", e);
            call.reject("startServer failed: " + e.getMessage(), e);
        }
    }

    @PluginMethod
    public synchronized void stopServer(PluginCall call) {
        try {
            if (server != null) {
                try { server.stop(1000); } catch (Exception e) { /* swallow */ }
                server = null;
            }
            isStarted = false;
            JSObject ret = new JSObject();
            ret.put("ok", true);
            call.resolve(ret);
        } catch (Exception e) {
            call.reject("stopServer failed: " + e.getMessage(), e);
        }
    }

    @PluginMethod
    public void getLocalIp(PluginCall call) {
        JSObject ret = new JSObject();
        ret.put("ip", WsServer.getLocalIpAddress());
        call.resolve(ret);
    }

    /**
     * Bind a connection (by current temp seat = -1) to a real seat number.
     * Called by the web layer after processing JOIN.
     * @param call {connId?: number, seat: number}
     *
     * Note: in v1 we use the seat field emitted in 'clientConnected' as a
     * transient token (it's -1 until bound, then the real seat). The web layer
     * tracks open connections by their seat field on the 'clientConnected' event.
     */
    @PluginMethod
    public void bindSeat(PluginCall call) {
        if (server == null) { call.reject("server not running"); return; }
        Integer seat = call.getInt("seat", -1);
        if (seat == null || seat < 0) { call.reject("invalid seat"); return; }
        // For v1 the web layer's transport maintains its own ws -> seat map, so
        // we just need to remember the seat assignment for routing purposes.
        // The actual bind happens via the WsServer.bindSeat() call when the
        // web layer sends back the assigned seat. The plugin will look up the
        // WebSocket by the most-recent 'clientConnected' (or 'message') event.
        // To keep things simple in v1, the web layer uses seat -1 as the
        // "not yet assigned" marker and the real seat comes from the host logic.
        // bindSeat is a no-op for the v1 flow; routing uses broadcast/sendToClient.
        JSObject ret = new JSObject();
        ret.put("ok", true);
        call.resolve(ret);
    }

    @PluginMethod
    public void sendToClient(PluginCall call) {
        if (server == null) { call.reject("server not running"); return; }
        Integer seat = call.getInt("seat", -1);
        String message = call.getString("message", "");
        if (seat == null || seat < 0) { call.reject("invalid seat"); return; }
        if (message == null) message = "";
        boolean sent = server.sendToClient(seat, message);
        JSObject ret = new JSObject();
        ret.put("ok", sent);
        ret.put("sent", sent);
        call.resolve(ret);
    }

    @PluginMethod
    public void broadcast(PluginCall call) {
        if (server == null) { call.reject("server not running"); return; }
        String message = call.getString("message", "");
        if (message == null) message = "";
        int sent = server.broadcastToAll(message);
        JSObject ret = new JSObject();
        ret.put("ok", true);
        ret.put("sent", sent);
        call.resolve(ret);
    }

    /**
     * Allocate a new seat number for an incoming joiner.
     * Exposed so the web layer can pick a seat in its own logic if needed.
     */
    public int allocateSeat() {
        int seat;
        do {
            seat = nextSeat.getAndIncrement();
            if (seat > 3) {
                // Room full — caller should detect this and send ROOM_FULL
                return -1;
            }
        } while (false);
        return seat;
    }

    @Override
    public void handleOnPause() {
        super.handleOnPause();
    }

    @Override
    public void handleOnResume() {
        super.handleOnResume();
    }

    @Override
    public void handleOnDestroy() {
        synchronized (this) {
            if (server != null) {
                try { server.stop(500); } catch (Exception e) { /* swallow */ }
                server = null;
            }
            isStarted = false;
        }
        super.handleOnDestroy();
    }
}
