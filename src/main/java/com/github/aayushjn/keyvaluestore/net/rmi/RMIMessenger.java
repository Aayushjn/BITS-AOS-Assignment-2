package com.github.aayushjn.keyvaluestore.net.rmi;

import com.github.aayushjn.keyvaluestore.model.MessageType;
import com.github.aayushjn.keyvaluestore.net.Messenger;

import java.io.IOException;
import java.rmi.Naming;
import java.rmi.NotBoundException;

public class RMIMessenger implements Messenger {
    private final String selfAddr;

    public RMIMessenger(String selfAddr) {
        this.selfAddr = selfAddr;
    }

    @Override
    public MessageType getValueForKey(MessageType mt, String peer) throws IOException {
        try {
            ServerInterface server = (ServerInterface) Naming.lookup("rmi://" + peer + "/remote");
            return server.getValueForKey(mt, selfAddr);
        } catch (NotBoundException e) {
            e.printStackTrace();
            return null;
        }
    }

    @Override
    public MessageType requestAcknowledgement(MessageType mt, String peer) throws IOException {
        try {
            ServerInterface server = (ServerInterface) Naming.lookup("rmi://" + peer + "/remote");
            return server.requestAcknowledgement(mt, selfAddr);
        } catch (NotBoundException e) {
            return null;
        }
    }

    @Override
    public MessageType getAllData(String peer) throws IOException {
        try {
            ServerInterface server = (ServerInterface) Naming.lookup("rmi://" + peer + "/remote");
            return server.getAllData(selfAddr);
        } catch (NotBoundException e) {
            return null;
        }
    }

    @Override
    public void deleteKey(MessageType mt, String peer) throws IOException {
        try {
            ServerInterface server = (ServerInterface) Naming.lookup("rmi://" + peer + "/remote");
            server.deleteKey(mt, selfAddr);
        } catch (NotBoundException ignored) {}
    }

    @Override
    public void commitKey(MessageType mt, String peer) throws IOException {
        try {
            ServerInterface server = (ServerInterface) Naming.lookup("rmi://" + peer + "/remote");
            mt.setPeer(selfAddr);
            server.commitKey(mt, selfAddr);
        } catch (NotBoundException ignored) {}
    }

    @Override
    public void exit(String peer) throws IOException {
        try {
            ServerInterface server = (ServerInterface) Naming.lookup("rmi://" + peer + "/remote");
            server.exit(selfAddr);
        } catch (NotBoundException ignored) {}
    }
}
