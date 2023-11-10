package com.github.aayushjn.keyvaluestore.net.rmi;

import com.github.aayushjn.keyvaluestore.model.MessageType;
import com.github.aayushjn.keyvaluestore.net.Messenger;

import java.io.IOException;
import java.rmi.NotBoundException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

public class RMIMessenger implements Messenger {
    private final String selfAddr;
    private final String addr;
    private final int port;

    public RMIMessenger(String selfAddr, String addr, int port) {
        this.selfAddr = selfAddr;
        this.addr = addr;
        this.port = port;
    }

    @Override
    public MessageType getValueForKey(MessageType mt, String peer) throws IOException {
        try {
            Registry registry = LocateRegistry.getRegistry(addr, port);
            ServerInterface server = (ServerInterface) registry.lookup(peer);
            return server.getValueForKey(mt, selfAddr);
        } catch (NotBoundException e) {
            e.printStackTrace();
            return null;
        }
    }

    @Override
    public MessageType requestAcknowledgement(MessageType mt, String peer) throws IOException {
        try {
            Registry registry = LocateRegistry.getRegistry(addr, port);
            ServerInterface server = (ServerInterface) registry.lookup(peer);
            return server.requestAcknowledgement(mt, selfAddr);
        } catch (NotBoundException e) {
            return null;
        }
    }

    @Override
    public MessageType getAllData(String peer) throws IOException {
        try {
            Registry registry = LocateRegistry.getRegistry(addr, port);
            ServerInterface server = (ServerInterface) registry.lookup(peer);
            return server.getAllData(selfAddr);
        } catch (NotBoundException e) {
            return null;
        }
    }

    @Override
    public void deleteKey(MessageType mt, String peer) throws IOException {
        try {
            Registry registry = LocateRegistry.getRegistry(addr, port);
            ServerInterface server = (ServerInterface) registry.lookup(peer);
            server.deleteKey(mt, selfAddr);
        } catch (NotBoundException ignored) {}
    }

    @Override
    public void commitKey(MessageType mt, String peer) throws IOException {
        try {
            Registry registry = LocateRegistry.getRegistry(addr, port);
            ServerInterface server = (ServerInterface) registry.lookup(peer);
            mt.peer = selfAddr;
            server.commitKey(mt, selfAddr);
        } catch (NotBoundException ignored) {}
    }

    @Override
    public void exit(String peer) throws IOException {
        try {
            Registry registry = LocateRegistry.getRegistry(addr, port);
            ServerInterface server = (ServerInterface) registry.lookup(peer);
            server.exit(selfAddr);
        } catch (NotBoundException ignored) {}
    }
}
