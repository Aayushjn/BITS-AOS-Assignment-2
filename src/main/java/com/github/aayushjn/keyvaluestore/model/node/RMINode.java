package com.github.aayushjn.keyvaluestore.model.node;

import com.github.aayushjn.keyvaluestore.net.rmi.RMIMessenger;
import com.github.aayushjn.keyvaluestore.net.rmi.RMIServer;

import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

public class RMINode extends Node {
    public RMINode(String addr, int port, String id, String... peers) throws RemoteException {
        super(NodeType.RMI, id, peers);

        RMIServer server = new RMIServer(store, this);
        Registry registry = LocateRegistry.getRegistry(addr, port);
        registry.rebind(id, server);

        messenger = new RMIMessenger(id, addr, port);

        state.compareAndSet(NodeState.READY, NodeState.RUNNING);
    }

    @Override
    protected void listenOnSocket() { /* RMI implementation will handle incoming connections on underlying socket */ }
}
