package com.github.aayushjn.keyvaluestore.model;

import java.io.*;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Logger;

/**
 * Node represents an arbitrary physical node running a socket connection and capable of managing a distributed K/V
 * store.
 *
 */
public abstract class Node extends Agreeable<Map.Entry<String, Object>, String> implements Closeable {
    protected final String id;
    protected Store store;
    protected List<String> peers;
    protected final NodeType type;
    protected AtomicReference<NodeState> state;
    protected AtomicReference<Object> requestedData;
    protected ExecutorService executorService;
    private final ExecutorService es;
    public final Logger logger;

    protected final BufferedReader br;
    protected final BufferedWriter bw;

    protected Node(NodeType type, String... peers) {
        super(peers.length);

        int peerCount = peers.length;

        id = "node-" + UUID.randomUUID();
        this.type = type;
        state = new AtomicReference<>(NodeState.READY);
        requestedData = new AtomicReference<>(null);

        executorService = Executors.newFixedThreadPool(peerCount > 0 ? peerCount : 1);
        es = Executors.newSingleThreadExecutor();
        logger = Logger.getLogger(id);

        this.peers = List.of(peers);
        store = new Store();

        br = new BufferedReader(new InputStreamReader(System.in));
        bw = new BufferedWriter(new OutputStreamWriter(System.out));
    }

    protected MessageType handleRemoteMessage(MessageType mt, String peer) {
        MessageType resp = null;
        switch (mt) {
            case GET -> {
                if (store.hasLocally(mt.key)) {
                    resp = MessageType.DATA;
                    resp.key = mt.key;
                    resp.value = store.get(mt.key);
                }
            }
            case DELETE -> store.removePeerForKey(mt.key);
            case DATA -> requestedData.set(mt.value);
            case STORE -> {
                resp = MessageType.DATA_ALL;
                resp.value = store.getAll();
            }
            case OWNER -> {
                if (store.hasKey(mt.key) || awaitingData.get().getKey().equals(mt.key) || votedOn.contains(mt.key)) {
                    resp = MessageType.NAK;
                } else {
                    resp = MessageType.ACK;
                }
                resp.key = mt.key;
            }
            case COMMIT -> {
                store.putPeerForKey(mt.key, peer);
                votedOn.remove(mt.key);
            }
            case ACK -> {
                if (awaitingData.get() != null) {
                    acks.getAndUpdate(i -> i + 1);
                }
            }
            case NAK -> {
                if (awaitingData.get() != null) {
                    naks.getAndUpdate(i -> i + 1);
                }
            }
            case EXIT -> store.removePeer(peer);
            default -> logger.warning("Unsupported message type received");
        }
        return resp;
    }

    protected abstract void listenOnSocket();

    protected abstract void listenOnConsole() throws IOException;

    public void listen() throws IOException, InterruptedException {
        es.submit(this::listenOnSocket);
        listenOnConsole();
    }

    @Override
    public void close() throws IOException {
        if (state.compareAndSet(NodeState.RUNNING, NodeState.STOPPED)) {
            es.shutdownNow();
            executorService.shutdownNow();
        }
    }

    protected enum NodeState {
        READY,
        RUNNING,
        STOPPED;
    }

    protected enum NodeType {
        TCP,
        UDP,
        RMI;
    }

    protected static final String MSG_KEY_NOT_LOCAL = "Key not present here";
    protected static final String MSG_OK = "<OK>";
}
