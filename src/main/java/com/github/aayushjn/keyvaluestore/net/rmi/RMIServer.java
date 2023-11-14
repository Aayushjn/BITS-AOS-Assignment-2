package com.github.aayushjn.keyvaluestore.net.rmi;

import com.github.aayushjn.keyvaluestore.model.Agreeable;
import com.github.aayushjn.keyvaluestore.model.MessageType;
import com.github.aayushjn.keyvaluestore.model.Store;

import java.io.Serial;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.Objects;


/**
 * RMI capable server implementation
 */
public class RMIServer extends UnicastRemoteObject implements ServerInterface {
    @Serial private static final long serialVersionUID = 20231114095600L;

    private final transient Store store;
    private final transient Agreeable<String> agreeable;

    public RMIServer(Store store, Agreeable<String> agreeable) throws RemoteException {
        super();

        this.store = store;
        this.agreeable = agreeable;
    }

    public MessageType getValueForKey(MessageType mt, String peer) throws RemoteException {
        MessageType resp = null;
        if (store.hasLocally(mt.getKey())) {
            resp = new MessageType.Data(mt.getKey(), store.get(mt.getKey()));
        }
        return resp;
    }

    public MessageType requestAcknowledgement(MessageType mt, String peer) throws RemoteException {
        MessageType resp;
        if (store.hasKey(mt.getKey()) || agreeable.getVotedOn().contains(mt.getKey())) {
            resp = new MessageType.Nak(mt.getKey());
        } else {
            resp = new MessageType.Ack(mt.getKey());
        }
        agreeable.getVotedOn().add(mt.getKey());
        return resp;
    }

    public MessageType getAllData(String peer) throws RemoteException {
        return new MessageType.DataAll(store.getAll());
    }

    public void deleteKey(MessageType mt, String peer) throws RemoteException {
        store.removePeerForKey(mt.getKey());
        agreeable.getVotedOn().remove(mt.getKey());
    }

    public void commitKey(MessageType mt, String peer) throws RemoteException {
        store.putPeerForKey(mt.getKey(), peer);
        agreeable.getVotedOn().remove(mt.getKey());
    }

    public void exit(String peer) throws RemoteException {
        store.removePeer(peer);
        agreeable.getPeers().remove(peer);
        agreeable.recomputeMajority();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (!(obj instanceof RMIServer)) {
            return false;
        }

        return Objects.equals(store, ((RMIServer) obj).store) && Objects.equals(agreeable, ((RMIServer) obj).agreeable);
    }

    @Override
    public int hashCode() {
        return Objects.hash(store, agreeable);
    }
}
