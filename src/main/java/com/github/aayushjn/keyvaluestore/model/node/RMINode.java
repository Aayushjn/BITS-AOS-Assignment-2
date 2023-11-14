package com.github.aayushjn.keyvaluestore.model.node;

import com.github.aayushjn.keyvaluestore.net.rmi.RMIMessenger;
import com.github.aayushjn.keyvaluestore.net.rmi.RMIServer;

import java.io.IOException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.logging.Level;

public class RMINode extends Node {
    private final Registry registry;

    public RMINode(String addr, int port, String id, String... peers) throws RemoteException {
        super(NodeType.RMI, id, peers);

        RMIServer server = new RMIServer(store, this);
        registry = LocateRegistry.getRegistry(addr, port);
        registry.rebind(id, server);

        messenger = new RMIMessenger(id, addr, port);

        state.compareAndSet(NodeState.READY, NodeState.RUNNING);
    }

    @Override
    protected void listenOnSocket() { /* RMI implementation will handle incoming connections on underlying socket */ }

    @Override
    public void close() throws IOException {
        super.close();
        try {
            registry.unbind(id);
        } catch (NotBoundException e) {
            logger.log(Level.WARNING, e, e::getMessage);
        }
    }
}
