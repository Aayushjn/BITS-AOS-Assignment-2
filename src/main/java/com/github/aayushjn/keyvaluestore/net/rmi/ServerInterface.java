package com.github.aayushjn.keyvaluestore.net.rmi;

import com.github.aayushjn.keyvaluestore.model.MessageType;

import java.rmi.Remote;
import java.rmi.RemoteException;

/**
 * RMI capable interface that mimics an arbitrary `Messenger`
 */
public interface ServerInterface extends Remote {
    MessageType getValueForKey(MessageType mt, String peer) throws RemoteException;

    MessageType requestAcknowledgement(MessageType mt, String peer) throws RemoteException;

    MessageType getAllData(String peer) throws RemoteException;

    void deleteKey(MessageType mt, String peer) throws RemoteException;

    void commitKey(MessageType mt, String peer) throws RemoteException;

    void exit(String peer) throws RemoteException;
}
