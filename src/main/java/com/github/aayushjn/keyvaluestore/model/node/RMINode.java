package com.github.aayushjn.keyvaluestore.model.node;

import com.github.aayushjn.keyvaluestore.net.rmi.RMIMessenger;
import com.github.aayushjn.keyvaluestore.net.rmi.RMIServer;

import java.io.IOException;
import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.util.logging.Level;

public class RMINode extends Node {
    private final String rmiId;

    public RMINode(String addr, int port, String... peers) throws RemoteException, MalformedURLException {
        super(NodeType.RMI, peers);

        RMIServer server = new RMIServer(store, this);
        rmiId = "rmi://" + addr + ":" + port + "/remote";
        LocateRegistry.createRegistry(port);
        Naming.rebind(rmiId, server);

        messenger = new RMIMessenger(addr + ":" + port);

        logger.info(() -> "node ready\n");

        state.compareAndSet(NodeState.READY, NodeState.RUNNING);
    }

    @Override
    protected void listenOnSocket() { /* RMI implementation will handle incoming connections on underlying socket */ }

    @Override
    public void close() throws IOException {
        super.close();
        try {
            Naming.unbind(rmiId);
        } catch (NotBoundException e) {
            logger.log(Level.WARNING, e, e::getMessage);
        }
    }
}
