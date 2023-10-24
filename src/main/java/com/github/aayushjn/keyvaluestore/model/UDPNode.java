package com.github.aayushjn.keyvaluestore.model;

import com.github.aayushjn.keyvaluestore.util.ConsoleColor;

import java.io.IOException;
import java.net.*;
import java.util.ArrayList;
import java.util.List;
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
        List<Future<?>> completedFutures = new ArrayList<>(peers.size());
        while (state.get() == NodeState.RUNNING) {
            if (futures.size() == peers.size()) {
                for (Future<?> future : futures) {
                    if (future.isDone()) {
                        completedFutures.add(future);
                    }
                }
            }
            futures.removeAll(completedFutures);
            completedFutures.clear();
            if (futures.size() == peers.size()) continue;
            futures.add(
                executorService.submit(() -> {
                    try {
                        byte[] buf = new byte[1024];
                        DatagramPacket packet = new DatagramPacket(buf, buf.length);
                        listenSocket.receive(packet);
                        InetAddress remoteAddress = packet.getAddress();
                        int remotePort = packet.getPort();

                        MessageType mt = MessageType.parseString(new String(packet.getData(), 0, packet.getLength()));
                        switch (mt) {
                            case GET -> {
                                if (store.hasLocally(mt.key)) {
                                    MessageType resp = MessageType.DATA;
                                    resp.key = mt.key;
                                    resp.value = store.get(mt.key);
                                    buf = resp.toString().getBytes();
                                    packet = new DatagramPacket(buf, buf.length, remoteAddress, remotePort);
                                    listenSocket.send(packet);
                                }
                            }
                            case DELETE -> store.removePeerForKey(mt.key);
                            case STORE -> {
                                // TODO: Return all local data
                            }
                            case OWNER -> {
                                MessageType resp;
                                if (store.hasKey(mt.key) || awaitingOwnershipOn.equals(mt.key)) {
                                    resp = MessageType.NAK;
                                } else {
                                    resp = MessageType.ACK;
                                }
                                resp.key = mt.key;
                                buf = resp.toString().getBytes();
                                packet = new DatagramPacket(buf, buf.length, remoteAddress, remotePort);
                                listenSocket.send(packet);
                            }
                            case COMMIT -> store.putPeerForKey(mt.key, packet.getAddress().toString());
                            case ACK -> {
                                // TODO: Handle agreement logic
                            }
                            case NAK -> {
                                // TODO: Handle agreement logic
                            }
                            default -> logger.warning("Unsupported message type received");
                        }
                    } catch (IOException e) {
                        logger.log(Level.WARNING, e, e::getMessage);
                    }
                })
            );
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
                        // TODO: Get key from peer
                    } else {
                        bw.write(ConsoleColor.withForegroundColor(MSG_KEY_NOT_LOCAL, 184, 0, 0) + "\n");
                    }
                    bw.flush();
                }
                case PUT -> {
                    if (store.hasLocally(mt.key)) {
                        System.out.println("has locally");
                        store.put(mt.key, mt.value);
                        bw.write(ConsoleColor.withForegroundColor(MSG_OK, 166, 166, 166) + "\n");
                    } else if (store.hasRemotely(mt.key)) {
                        System.out.println("has remotely");
                        bw.write(ConsoleColor.withForegroundColor(MSG_KEY_NOT_LOCAL, 184, 0, 0) + "\n");
                    } else {
                        awaitingOwnershipOn = mt.key;
                        // TODO: Start agreement protocol
                        store.put(mt.key, mt.value);
                        bw.write(ConsoleColor.withForegroundColor(MSG_OK, 166, 166, 166) + "\n");
                        awaitingOwnershipOn = "";
                    }
                    bw.flush();
                }
                case DELETE -> {
                    if (store.hasLocally(mt.key)) {
                        store.delete(mt.key);
                        // TODO: Send DELETE message to peers
                        bw.write(ConsoleColor.withForegroundColor(MSG_OK, 166, 166, 166) + "\n");
                    } else if (store.hasRemotely(mt.key)) {
                        bw.write(ConsoleColor.withForegroundColor(MSG_KEY_NOT_LOCAL, 184, 0, 0) + "\n");
                    } else {
                        bw.write(ConsoleColor.withForegroundColor(MSG_KEY_NOT_LOCAL, 184, 0, 0) + "\n");
                    }
                    bw.flush();
                }
                case STORE -> {
                    // TODO: Fetch all data
                }
                case EXIT -> {
                    // TODO: Send EXIT message to peers
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
