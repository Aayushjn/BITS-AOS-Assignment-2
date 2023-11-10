package com.github.aayushjn.keyvaluestore.net.rmi;

import com.github.aayushjn.keyvaluestore.model.Agreeable;
import com.github.aayushjn.keyvaluestore.model.MessageType;
import com.github.aayushjn.keyvaluestore.model.Store;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.Objects;

public class RMIServer extends UnicastRemoteObject implements ServerInterface {
    private final transient Store store;
    private final transient Agreeable<String> agreeable;

    public RMIServer(Store store, Agreeable<String> agreeable) throws RemoteException {
        super();

        this.store = store;
        this.agreeable = agreeable;
    }

    public MessageType getValueForKey(MessageType mt, String peer) throws RemoteException {
        MessageType resp = null;
        if (store.hasLocally(mt.key)) {
            resp = new MessageType.Data();
            resp.key = mt.key;
            resp.value = store.get(mt.key);
        }
        return resp;
    }

    public MessageType requestAcknowledgement(MessageType mt, String peer) throws RemoteException {
        MessageType resp;
        if (store.hasKey(mt.key) || agreeable.getVotedOn().contains(mt.key)) {
            resp = new MessageType.Nak();
        } else {
            resp = new MessageType.Ack();
        }
        resp.key = mt.key;
        agreeable.getVotedOn().add(mt.key);
        return resp;
    }

    public MessageType getAllData(String peer) throws RemoteException {
        MessageType resp = new MessageType.DataAll();
        resp.value = store.getAll();
        return resp;
    }

    public void deleteKey(MessageType mt, String peer) throws RemoteException {
        store.removePeerForKey(mt.key);
        agreeable.getVotedOn().remove(mt.key);
    }

    public void commitKey(MessageType mt, String peer) throws RemoteException {
        store.putPeerForKey(mt.key, peer);
        agreeable.getVotedOn().remove(mt.key);
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
