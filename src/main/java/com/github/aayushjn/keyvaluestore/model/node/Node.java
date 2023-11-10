package com.github.aayushjn.keyvaluestore.model.node;

import com.github.aayushjn.keyvaluestore.model.Agreeable;
import com.github.aayushjn.keyvaluestore.model.MessageType;
import com.github.aayushjn.keyvaluestore.model.Store;
import com.github.aayushjn.keyvaluestore.net.Messenger;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Logger;

/**
 * Node represents an arbitrary physical node running a socket connection and capable of managing a distributed K/V
 * store.
 *
 */
public abstract class Node extends Agreeable<String> implements Closeable {
    protected final String id;
    protected Store store;
    protected final NodeType type;
    protected AtomicReference<NodeState> state;
    protected AtomicReference<Object> requestedData;
    protected Messenger messenger;
    protected ExecutorService executorService;
    protected final ExecutorService es;
    public final Logger logger;

    protected Node(NodeType type, String id, String... peers) {
        super(peers);

        int peerCount = peers.length;

        this.id = id;
        this.type = type;
        state = new AtomicReference<>(NodeState.READY);
        requestedData = new AtomicReference<>(null);

        executorService = Executors.newFixedThreadPool(peerCount > 0 ? peerCount : 1);
        es = Executors.newSingleThreadExecutor();
        logger = Logger.getLogger(id);

        store = new Store();
    }

    public Store getStore() {
        return store;
    }

    public Messenger getMessenger() {
        return messenger;
    }

    protected MessageType handleRemoteMessage(MessageType mt) {
        MessageType resp = null;
        if (mt instanceof MessageType.Get) {
            if (store.hasLocally(mt.key)) {
                resp = new MessageType.Data();
                resp.key = mt.key;
                resp.value = store.get(mt.key);
            }
        } else if (mt instanceof MessageType.Del) {
            store.removePeerForKey(mt.key);
            votedOn.remove(mt.key);
        } else if (mt instanceof MessageType.Store) {
            resp = new MessageType.DataAll();
            resp.value = store.getAll();
        } else if (mt instanceof MessageType.Owner) {
            if (store.hasKey(mt.key) || votedOn.contains(mt.key)) {
                resp = new MessageType.Nak();
            } else {
                resp = new MessageType.Ack();
            }
            resp.key = mt.key;
            votedOn.add(mt.key);
        } else if (mt instanceof MessageType.Commit) {
            store.putPeerForKey(mt.key, mt.peer);
            votedOn.remove(mt.key);
        } else if (mt instanceof MessageType.Exit) {
            store.removePeer(mt.peer);
            peers.remove(mt.peer);
            recomputeMajority();
        } else {
            logger.warning(() -> "Unsupported message type received " + mt);
        }
        return resp;
    }

    protected abstract void listenOnSocket();

    public void listen() {
        es.submit(this::listenOnSocket);
    }

    @Override
    public void close() throws IOException {
        if (state.compareAndSet(NodeState.RUNNING, NodeState.STOPPED)) {
            logger.info("Shutting down");
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

    public static final String MSG_KEY_NOT_LOCAL = "Key not present here";
    public static final String MSG_OK = "<OK>";
}
