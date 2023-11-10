package com.github.aayushjn.keyvaluestore.model.node;

import com.github.aayushjn.keyvaluestore.model.MessageType;
import com.github.aayushjn.keyvaluestore.net.tcp.TCPMessenger;

import java.io.*;
import java.net.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.logging.Level;

public class TCPNode extends Node {
    private final ServerSocket listenSocket;
    private final Map<String, Socket> socketMap;

    public TCPNode(String addr, int port, String id, String... peers) throws IOException {
        super(NodeType.TCP, id, peers);

        InetAddress bindAddr;
        try {
            bindAddr = InetAddress.getByName(addr);
        } catch (UnknownHostException e) {
            bindAddr = InetAddress.getLoopbackAddress();
        }
        listenSocket = new ServerSocket(port, peers.length, bindAddr);
        listenSocket.setReuseAddress(true);
        socketMap = HashMap.newHashMap(peers.length);
        messenger = new TCPMessenger(addr + ":" + port, socketMap);
        logger.info(() -> id + " listening on " + listenSocket.getLocalSocketAddress());

        state.compareAndSet(NodeState.READY, NodeState.RUNNING);
    }

    @Override
    protected void listenOnSocket() {
        List<Future<?>> futures = new ArrayList<>(peers.size());
        while (state.get() == NodeState.RUNNING) {
            if (futures.size() == peers.size()) {
                futures.removeIf(Future::isDone);
            }
            if (futures.size() == peers.size()) continue;

            Runnable task = () -> {
                Socket socket;
                try {
                    socket = listenSocket.accept();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                while (!socket.isClosed()) {
                    try {
                        BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                        BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));

                        while (!reader.ready());
                        MessageType mt = MessageType.parseString(reader.readLine());

                        MessageType resp = handleRemoteMessage(mt);
                        if (resp != null) {
                            writer.write(resp + "\n");
                            writer.flush();
                        }
                        if (mt instanceof MessageType.Exit) {
                            socket.close();
                        }
                    } catch (SocketException ignored) {
                        // ignore this since the socket is closed
                    } catch (IOException e) {
                        logger.log(Level.WARNING, e, e::getMessage);
                    }
                }
            };
            futures.add(executorService.submit(task));
        }
    }

    @Override
    public void close() throws IOException {
        super.close();
        listenSocket.close();
        for (Socket socket : socketMap.values()) {
            if (socket.isConnected()) socket.close();
        }
    }
}
