package com.github.aayushjn.keyvaluestore.util;

import com.github.aayushjn.keyvaluestore.model.MessageType;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.Arrays;

public class MessageUtils {
    private MessageUtils() {}

    public static Object sendGetMessage(Socket socket, String peer, MessageType mt) throws IOException {
        String[] split = peer.split(":");
        byte[] data;
        socket.connect(new InetSocketAddress(split[0], Integer.parseInt(split[1])));
        try {
            socket.getOutputStream().write(mt.toString().getBytes());
            data = socket.getInputStream().readAllBytes();
        } catch (IOException e) {
            data = null;
        } finally {
            socket.shutdownInput();
            socket.shutdownOutput();
        }
        if (data == null) {
            return null;
        } else {
            MessageType resp = MessageType.parseString(new String(data));
            if (resp == MessageType.DATA) {
                return resp.value;
            } else {
                return null;
            }
        }
    }

    public static MessageType sendOwnerMessage(Socket socket, String peer, MessageType mt) throws IOException {
        String[] split = peer.split(":");
        byte[] data;
        socket.connect(new InetSocketAddress(split[0], Integer.parseInt(split[1])), 250);

        System.out.println("connected to " + peer);
        try {
            socket.getOutputStream().write(mt.toString().getBytes());
            System.out.println("sent " + mt);
            data = socket.getInputStream().readAllBytes();
            System.out.println(Arrays.toString(data));
        } catch (IOException e) {
            data = null;
        } finally {
            socket.shutdownInput();
            socket.shutdownOutput();
        }
        if (data == null) {
            return null;
        } else {
            return MessageType.parseString(new String(data));
        }
    }

    public static void sendCommitMessage(Socket socket, String peer, MessageType mt) throws IOException {
        String[] split = peer.split(":");
        socket.connect(new InetSocketAddress(split[0], Integer.parseInt(split[1])));
        try {
            socket.getOutputStream().write(mt.toString().getBytes());
        } catch (IOException e) {

        } finally {
            socket.shutdownInput();
            socket.shutdownOutput();
        }
    }
}
