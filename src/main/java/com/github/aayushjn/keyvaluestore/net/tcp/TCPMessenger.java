package com.github.aayushjn.keyvaluestore.net.tcp;

import com.github.aayushjn.keyvaluestore.model.MessageType;
import com.github.aayushjn.keyvaluestore.net.Messenger;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.Socket;
import java.net.SocketAddress;
import java.util.Map;

public class TCPMessenger implements Messenger {
    private final String selfAddr;
    private final Map<String, Socket> socketMap;

    public TCPMessenger(String selfAddr, Map<String, Socket> socketMap) {
        this.selfAddr = selfAddr;
        this.socketMap = socketMap;
    }

    @Override
    public MessageType getValueForKey(MessageType mt, String peer) throws IOException {
        sendMessage(mt, peer);
        return readData(peer);
    }

    @Override
    public MessageType requestAcknowledgement(MessageType mt, String peer) throws IOException {
        sendMessage(mt, peer);
        return readData(peer);
    }

    @Override
    public MessageType getAllData(String peer) throws IOException {
        sendMessage(new MessageType.Store(), peer);
        return readData(peer);
    }

    @Override
    public void deleteKey(MessageType mt, String peer) throws IOException {
        sendMessage(mt, peer);
    }

    @Override
    public void commitKey(MessageType mt, String peer) throws IOException {
        mt.setPeer(selfAddr);
        sendMessage(mt, peer);
    }

    @Override
    public void exit(String peer) throws IOException {
        MessageType mt = new MessageType.Exit(selfAddr);
        sendMessage(mt, peer);
    }

    private void sendMessage(MessageType mt, String peer) throws IOException {
        String[] split = peer.split(":");
        Socket socket = socketMap.computeIfAbsent(peer, k -> new Socket(Proxy.NO_PROXY));
        if (!socket.isConnected()) {
            SocketAddress sockAddress = new InetSocketAddress(split[0], Integer.parseInt(split[1]));
            socket.connect(sockAddress);
        }
        BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
        bw.write(mt.toString() + "\n");
        bw.flush();
    }

    private MessageType readData(String peer) throws IOException {
        Socket socket = socketMap.get(peer);
        BufferedReader br = new BufferedReader(new InputStreamReader(socket.getInputStream()));
        while (!br.ready());
        return MessageType.parseString(br.readLine());
    }
}
