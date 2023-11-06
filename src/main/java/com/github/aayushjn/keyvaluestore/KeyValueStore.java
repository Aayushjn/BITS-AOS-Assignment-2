package com.github.aayushjn.keyvaluestore;

import com.github.aayushjn.keyvaluestore.model.Node;
import com.github.aayushjn.keyvaluestore.model.TCPNode;
import com.github.aayushjn.keyvaluestore.model.UDPNode;

import java.io.IOException;

public class KeyValueStore {
    public static void main(String[] args) {
        String mode = "tcp";
        String host = args[0];
        int port = Integer.parseInt(args[1]);
        String[] peers = args[2].split(",");

        Node node = null;
        try {
            switch (mode) {
                case "tcp" -> node = new TCPNode(host, port, peers);
                case "udp" -> node = new UDPNode(host, port, peers);
                default -> throw new IllegalArgumentException("unknown mode '" + mode + "'");
            }
            node.listen();
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        } finally {
            try {
                if (node != null) node.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
