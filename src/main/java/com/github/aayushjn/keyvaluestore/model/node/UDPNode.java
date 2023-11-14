package com.github.aayushjn.keyvaluestore.model.node;

import com.github.aayushjn.keyvaluestore.model.MessageType;
import com.github.aayushjn.keyvaluestore.net.udp.UDPMessenger;

import java.io.IOException;
import java.net.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;
import java.util.logging.Level;

import static com.github.aayushjn.keyvaluestore.model.MessageType.DataAll.DATA_LIMIT;

public class UDPNode extends Node {
    private final DatagramSocket listenSocket;
    // While UDP can utilize the same underlying socket for send/receive, to ensure minimal changes in remaining code,
    // a separate DatagramSocket is used.
    private final DatagramSocket sendSocket;

    public UDPNode(String addr, int port, String id, String... peers) throws IOException {
        super(NodeType.UDP, id, peers);

        InetAddress bindAddr;
        try {
            bindAddr = InetAddress.getByName(addr);
        } catch (UnknownHostException e) {
            bindAddr = InetAddress.getLoopbackAddress();
        }
        listenSocket = new DatagramSocket(port, bindAddr);
        listenSocket.setReuseAddress(true);

        sendSocket = new DatagramSocket();

        messenger = new UDPMessenger(addr + ":" + port, sendSocket);

        logger.info(() -> id + " listening on " + listenSocket.getLocalSocketAddress() + "\n");

        state.compareAndSet(NodeState.READY, NodeState.RUNNING);
    }

    @Override
    protected void listenOnSocket() {
        Runnable task = () -> {
            try {
                byte[] buf = new byte[DATA_LIMIT];
                DatagramPacket packet = new DatagramPacket(buf, buf.length);
                listenSocket.receive(packet);
                InetAddress remoteAddress = packet.getAddress();
                int remotePort = packet.getPort();


                MessageType mt = MessageType.parseString(new String(packet.getData(), 0, packet.getLength()).trim());
                MessageType resp = handleRemoteMessage(mt);
                if (resp != null) {
                    buf = resp.toString().getBytes();
                    packet = new DatagramPacket(buf, buf.length, remoteAddress, remotePort);
                    listenSocket.send(packet);
                }
            } catch (SocketException ignored) {
                // ignore this since socket is closed
            } catch (IOException e) {
                logger.log(Level.WARNING, e, e::getMessage);
            }
        };
        List<Future<?>> futures = new ArrayList<>(peers.size());
        while (state.get() == NodeState.RUNNING) {
            if (futures.size() == peers.size()) {
                futures.removeIf(Future::isDone);
            }
            if (futures.size() == peers.size()) continue;
            futures.add(executorService.submit(task));
        }
    }

    @Override
    public void close() throws IOException {
        super.close();
        listenSocket.close();
        sendSocket.close();
    }
}
