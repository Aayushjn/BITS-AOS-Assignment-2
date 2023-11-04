package com.github.aayushjn.keyvaluestore.model;

import com.github.aayushjn.keyvaluestore.util.ConsoleColor;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.io.IOException;
import java.net.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.logging.Level;

public class UDPNode extends Node {
    private final DatagramSocket listenSocket;

    public UDPNode(String addr, int port, String... peers) throws IOException {
        super(NodeType.UDP, peers);

        InetAddress bindAddr;
        try {
            bindAddr = InetAddress.getByName(addr);
        } catch (UnknownHostException e) {
            bindAddr = InetAddress.getLoopbackAddress();
        }
        listenSocket = new DatagramSocket(port, bindAddr);
        listenSocket.setReuseAddress(true);
        listenSocket.setOption(StandardSocketOptions.SO_LINGER, 0);
        bw.write(id + " listening on " + listenSocket.getLocalSocketAddress() + "\n");
        bw.flush();

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
                try {
                    byte[] buf = new byte[1024];
                    DatagramPacket packet = new DatagramPacket(buf, buf.length);
                    listenSocket.receive(packet);
                    InetAddress remoteAddress = packet.getAddress();
                    int remotePort = packet.getPort();

                    MessageType mt = MessageType.parseString(new String(packet.getData(), 0, packet.getLength()));
                    MessageType resp = handleRemoteMessage(mt, remoteAddress + ":" + remotePort);
                    if (resp != null) {
                        buf = resp.toString().getBytes();
                        packet = new DatagramPacket(buf, buf.length, remoteAddress, remotePort);
                        listenSocket.send(packet);
                    }
                } catch (IOException e) {
                    logger.log(Level.WARNING, e, e::getMessage);
                }
            };
            futures.add(executorService.submit(task));
        }
    }

    @Override
    protected void listenOnConsole() throws IOException {
        String input;
        do {
            bw.write("> ");
            bw.flush();
            input = br.readLine();
            MessageType mt;
            try {
                mt = MessageType.parseString(input);
            } catch (IllegalArgumentException e) {
                bw.write(ConsoleColor.withForegroundColor(e.getMessage(), 184, 0, 0) + "\n");
                bw.flush();
                continue;
            }
            switch (mt) {
                case GET -> {
                    if (store.hasLocally(mt.key)) {
                        bw.write(store.get(mt.key).toString() + "\n");
                    } else if (store.hasRemotely(mt.key)) {
                        String peer = store.getPeerForKey(mt.key);
                        // TODO: send GET message to peer
                        try {
                            Thread.sleep(500);
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                        Object rData = requestedData.get();
                        if (rData != null) {
                            bw.write(rData.toString() + "\n");
                        } else {
                            bw.write(ConsoleColor.withForegroundColor("Could not get data from peer", 184, 0, 0) + "\n");
                        }
                    } else {
                        bw.write(ConsoleColor.withForegroundColor(MSG_KEY_NOT_LOCAL, 184, 0, 0) + "\n");
                    }
                    bw.flush();
                }
                case PUT -> {
                    if (store.hasLocally(mt.key)) {
                        store.put(mt.key, mt.value);
                        bw.write(ConsoleColor.withForegroundColor(MSG_OK, 166, 166, 166) + "\n");
                    } else if (store.hasRemotely(mt.key)) {
                        bw.write(ConsoleColor.withForegroundColor(MSG_KEY_NOT_LOCAL, 184, 0, 0) + "\n");
                    } else {
                        awaitingData.set(Map.entry(mt.key, mt.value));
                        for (String peer : peers) {
                            // TODO: send OWNER message to peer
                        }
                        try {
                            Thread.sleep(AGREEMENT_DELAY);
                        } catch (InterruptedException e) {
                            logger.log(Level.WARNING, e, e::getMessage);
                        }
                        if (hasMajority()) {
                            store.put(mt.key, mt.value);
                            for (String peer : peers) {
                                // TODO: send COMMIT message to peer
                            }
                            bw.write(ConsoleColor.withForegroundColor(MSG_OK, 166, 166, 166) + "\n");
                        } else {
                            bw.write(ConsoleColor.withForegroundColor("Cannot write data", 184, 0, 0) + "\n");
                        }
                        acks.set(0);
                        naks.set(0);
                        awaitingData.set(null);
                    }
                    bw.flush();
                }
                case DELETE -> {
                    if (store.hasLocally(mt.key)) {
                        store.delete(mt.key);
                        for (String peer : peers) {
                            // TODO: send DELETE message to peer
                        }
                        bw.write(ConsoleColor.withForegroundColor(MSG_OK, 166, 166, 166) + "\n");
                    } else if (store.hasRemotely(mt.key)) {
                        bw.write(ConsoleColor.withForegroundColor(MSG_KEY_NOT_LOCAL, 184, 0, 0) + "\n");
                    } else {
                        bw.write(ConsoleColor.withForegroundColor(MSG_KEY_NOT_LOCAL, 184, 0, 0) + "\n");
                    }
                    bw.flush();
                }
                case STORE -> {
                    Map<String, Object> localStore = store.getAll();
                    for (String peer : peers) {
                        // TODO: send STORE message to peer
                    }
                    // TODO: Combine received stores
                    Gson gson = new GsonBuilder().setPrettyPrinting().create();
                    String data = gson.toJson(localStore);
                    if (data.length() > 65535) {
                        bw.write(data.substring(0, 65535) + " <trimmed>...\n");
                    } else {
                        bw.write(data + "\n");
                    }
                    bw.flush();
                }
                case EXIT -> {
                    for (String peer : peers) {
                        // TODO: send EXIT message to peer
                    }
                    close();
                }
                default -> logger.warning("Unsupported message type received");
            }
        } while (state.get() == NodeState.RUNNING);
    }

    @Override
    public void close() throws IOException {
        super.close();
        listenSocket.close();
        bw.write("Shutting down...\n");
        bw.flush();
    }
}
