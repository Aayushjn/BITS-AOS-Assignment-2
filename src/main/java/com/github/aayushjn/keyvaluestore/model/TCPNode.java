package com.github.aayushjn.keyvaluestore.model;

import com.github.aayushjn.keyvaluestore.util.ConsoleColor;
import com.github.aayushjn.keyvaluestore.util.MessageUtils;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.io.*;
import java.net.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Future;
import java.util.logging.Level;

public class TCPNode extends Node {
    private final ServerSocket listenSocket;
    private final Socket sendSocket;

    public TCPNode(String addr, int port, String... peers) throws IOException {
        super(NodeType.TCP, peers);

        InetAddress bindAddr;
        try {
            bindAddr = InetAddress.getByName(addr);
        } catch (UnknownHostException e) {
            bindAddr = InetAddress.getLoopbackAddress();
        }
        listenSocket = new ServerSocket(port, peers.length, bindAddr);
        listenSocket.setReuseAddress(true);
        sendSocket = new Socket();
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
                try (Socket socket = listenSocket.accept()) {
                    System.out.println("accepted");
                    BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                    BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));

                    MessageType mt = MessageType.parseString(reader.readLine());
                    System.out.println(mt);

                    MessageType resp = handleRemoteMessage(mt, socket.getRemoteSocketAddress().toString());
                    if (resp != null) {
                        writer.write(resp.toString());
                    }
                } catch (SocketException ignored) {
                    logger.log(Level.WARNING, ignored, ignored::getMessage);
                    // ignore this since the socket is closed
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
                        Object data = MessageUtils.sendGetMessage(sendSocket, peer, mt);

                        bw.write(
                        Objects.requireNonNullElseGet(
                                data,
                                () -> ConsoleColor.withForegroundColor("Could not get data from peer", 184, 0, 0)
                            ) + "\n"
                        );
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
                        MessageType msg = MessageType.OWNER;
                        msg.key = mt.key;
                        MessageType resp;
                        for (String peer : peers) {
                            resp = MessageUtils.sendOwnerMessage(sendSocket, peer, msg);
                            if (resp == MessageType.ACK) {
                                acks.getAndUpdate(i -> i + 1);
                            } else if (resp == MessageType.NAK) {
                                naks.getAndUpdate(i -> i + 1);
                            }
                        }
                        if (hasMajority()) {
                            store.put(mt.key, mt.value);
                            msg = MessageType.COMMIT;
                            msg.key = mt.key;
                            for (String peer : peers) {
                                MessageUtils.sendCommitMessage(sendSocket, peer, msg);
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
        sendSocket.close();
        bw.write("Shutting down...\n");
        bw.flush();
    }
}
