package com.github.aayushjn.keyvaluestore.net.udp;

import com.github.aayushjn.keyvaluestore.model.MessageType;
import com.github.aayushjn.keyvaluestore.net.Messenger;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;

public class UDPMessenger implements Messenger {
    private final DatagramSocket socket;
    private final String selfAddr;

    public UDPMessenger(String selfAddr, DatagramSocket socket) {
        this.socket = socket;
        this.selfAddr = selfAddr;
    }

    @Override
    public MessageType getValueForKey(MessageType mt, String peer) throws IOException {
        sendMessage(mt, peer);
        return readData();
    }

    @Override
    public MessageType requestAcknowledgement(MessageType mt, String peer) throws IOException {
        sendMessage(mt, peer);
        return readData();
    }

    @Override
    public MessageType getAllData(String peer) throws IOException {
        sendMessage(new MessageType.Store(), peer);
        return readData();
    }

    @Override
    public void deleteKey(MessageType mt, String peer) throws IOException {
        sendMessage(mt, peer);
    }

    @Override
    public void commitKey(MessageType mt, String peer) throws IOException {
        mt.peer = selfAddr;
        sendMessage(mt, peer);
    }

    @Override
    public void exit(String peer) throws IOException {
        MessageType mt = new MessageType.Exit();
        mt.peer = selfAddr;
        sendMessage(mt, peer);
    }

    private void sendMessage(MessageType mt, String peer) throws IOException {
        String[] split = peer.split(":");
        byte[] buf = mt.toString().getBytes();
        InetAddress remoteAddress = InetAddress.getByName(split[0]);
        int remotePort = Integer.parseInt(split[1]);

        DatagramPacket packet = new DatagramPacket(buf, buf.length, remoteAddress, remotePort);
        socket.send(packet);
    }

    private MessageType readData() throws IOException {
        byte[] buf = new byte[1024];
        DatagramPacket packet = new DatagramPacket(buf, buf.length);
        socket.receive(packet);
        return MessageType.parseString(new String(packet.getData(), 0, packet.getLength()));
    }
}
