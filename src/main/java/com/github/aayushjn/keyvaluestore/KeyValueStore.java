package com.github.aayushjn.keyvaluestore;

import com.github.aayushjn.keyvaluestore.model.Node;
import com.github.aayushjn.keyvaluestore.model.TCPNode;
import com.github.aayushjn.keyvaluestore.model.UDPNode;

import java.io.IOException;

public class KeyValueStore {
    public static void main(String[] args) {
        String mode = "tcp";

        Node node = null;
        try {
            switch (mode) {
                case "tcp" -> node = new TCPNode("127.0.0.1", 8551);
                case "udp" -> node = new UDPNode("127.0.0.1", 8551);
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
