package com.github.aayushjn.keyvaluestore.model;

import java.io.*;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Logger;

public abstract class Node implements Closeable {
    protected final String id;
    protected Store store;
    protected List<String> peers;
    protected final NodeType type;
    protected AtomicReference<NodeState> state;
    protected String awaitingOwnershipOn;
    protected ExecutorService executorService;
    private final ExecutorService es;
    public final Logger logger;

    protected final BufferedReader br;
    protected final BufferedWriter bw;

    protected Node(NodeType type, String... peers) {
        int peerCount = peers.length;

        id = "node-" + UUID.randomUUID();
        this.type = type;
        state = new AtomicReference<>(NodeState.READY);
        logger = Logger.getLogger(id);

        executorService = Executors.newFixedThreadPool(peerCount > 0 ? peerCount : 1);
        es = Executors.newSingleThreadExecutor();

        this.peers = List.of(peers);
        store = new Store();

        br = new BufferedReader(new InputStreamReader(System.in));
        bw = new BufferedWriter(new OutputStreamWriter(System.out));
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
