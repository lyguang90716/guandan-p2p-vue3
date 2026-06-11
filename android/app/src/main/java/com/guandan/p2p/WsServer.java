package com.guandan.p2p;

import org.java_websocket.WebSocket;
import org.java_websocket.framing.CloseFrame;
import org.java_websocket.handshake.ClientHandshake;
import org.java_websocket.server.WebSocketServer;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.URI;
import java.util.Collections;
import java.util.Enumeration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Native WebSocket server (host side) for the LAN P2P game.
 *
 * - Listens on 0.0.0.0:8848 (or the port passed in)
 * - Maintains a (WebSocket -> seat) map
 * - Exposes sendToClient / broadcast
 * - Forwards onOpen / onClose / onMessage back to the web layer via WsServerPlugin listeners
 *
 * This is the Java counterpart of the JS WebSocketTransport / src/common/network-transport-ws.js
 * used in the v1.0 browser version with the 'ws' npm package. On Android we cannot use 'ws',
 * so we delegate the server to a real Java process via the Capacitor plugin bridge.
 */
public class WsServer extends WebSocketServer {

    /** seat (>=0) -> WebSocket connection. seat=-1 means "not yet assigned by host". */
    private final Map<Integer, WebSocket> seatMap = new ConcurrentHashMap<>();
    /** WebSocket -> seat inverse map (used for fast lookup on close). */
    private final Map<WebSocket, Integer> connMap = new ConcurrentHashMap<>();

    /** callback hooks — set by WsServerPlugin */
    private EventListener listener;

    public interface EventListener {
        void onClientConnected(WebSocket conn, int assignedSeat);
        void onClientDisconnected(WebSocket conn, int seat);
        void onClientMessage(WebSocket conn, int seat, String message);
    }

    public WsServer(int port) {
        super(new InetSocketAddress("0.0.0.0", port));
        // Reuse addr to allow rapid restart on the same port (esp. dev hot-reload)
        setReuseAddr(true);
    }

    public void setEventListener(EventListener listener) {
        this.listener = listener;
    }

    public boolean isListening() {
        // WebSocketServer doesn't expose isClosed publicly in 1.5.x; track our own state.
        return listening;
    }
    private volatile boolean listening = false;

    @Override
    public void onStart() {
        listening = true;
    }

    public int getBoundPort() {
        if (getAddress() == null) return -1;
        return getAddress().getPort();
    }

    @Override
    public void onOpen(WebSocket conn, ClientHandshake handshake) {
        // Assign a temporary seat = -1 (host will allocate proper seat on JOIN)
        connMap.put(conn, -1);
        seatMap.put(-1, conn);
        if (listener != null) listener.onClientConnected(conn, -1);
    }

    @Override
    public void onClose(WebSocket conn, int code, String reason, boolean remote) {
        Integer seat = connMap.remove(conn);
        if (seat != null) seatMap.remove(seat);
        if (listener != null) listener.onClientDisconnected(conn, seat != null ? seat : -1);
    }

    @Override
    public void onMessage(WebSocket conn, String message) {
        Integer seat = connMap.get(conn);
        if (listener != null) listener.onClientMessage(conn, seat != null ? seat : -1, message);
    }

    @Override
    public void onError(WebSocket conn, Exception ex) {
        // Errors are usually transport-level (client dropped, etc.) — ignore in plugin layer.
    }

    @Override
    public void stop() throws InterruptedException {
        listening = false;
        super.stop();
    }

    @Override
    public void stop(int timeout) throws InterruptedException {
        listening = false;
        super.stop(timeout);
    }

    /**
     * Bind a connection to a seat. Called by the web layer after processing JOIN.
     */
    public void bindSeat(WebSocket conn, int seat) {
        Integer prev = connMap.get(conn);
        if (prev != null) seatMap.remove(prev);
        connMap.put(conn, seat);
        seatMap.put(seat, conn);
    }

    /**
     * Send a JSON message to a specific seat.
     * @return true if sent, false if seat not found or socket not open
     */
    public boolean sendToClient(int seat, String message) {
        WebSocket conn = seatMap.get(seat);
        if (conn == null) return false;
        if (!conn.isOpen()) return false;
        try {
            conn.send(message);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * Broadcast a JSON message to all connected clients.
     * @return number of clients the message was sent to
     * @note Renamed from broadcast() to broadcastToAll() to avoid clashing with
     *       org.java_websocket.server.WebSocketServer.broadcast(String) which returns void.
     */
    public int broadcastToAll(String message) {
        int n = 0;
        // Snapshot to avoid ConcurrentModificationException
        for (Map.Entry<Integer, WebSocket> e : seatMap.entrySet()) {
            if (e.getKey() == -1) continue; // skip unassigned
            WebSocket conn = e.getValue();
            if (conn == null || !conn.isOpen()) continue;
            try { conn.send(message); n++; } catch (Exception ex) { /* swallow */ }
        }
        return n;
    }

    /** Number of currently connected clients (excluding the unassigned slot). */
    public int clientCount() {
        return (int) connMap.values().stream().filter(s -> s >= 0).count();
    }

    /**
     * Compute the device's primary non-loopback IPv4 address.
     * Returns "127.0.0.1" as fallback if no suitable interface is found
     * (which can happen on emulators / some sandboxed devices).
     */
    public static String getLocalIpAddress() {
        try {
            Enumeration<NetworkInterface> nis = NetworkInterface.getNetworkInterfaces();
            for (NetworkInterface ni : Collections.list(nis)) {
                if (ni.isLoopback() || ni.isPointToPoint() || !ni.isUp()) continue;
                Enumeration<InetAddress> addrs = ni.getInetAddresses();
                for (InetAddress addr : Collections.list(addrs)) {
                    if (addr instanceof Inet4Address && !addr.isLoopbackAddress()) {
                        String host = addr.getHostAddress();
                        if (host != null && !host.isEmpty()) return host;
                    }
                }
            }
        } catch (Exception e) {
            // fall through
        }
        return "127.0.0.1";
    }
}
