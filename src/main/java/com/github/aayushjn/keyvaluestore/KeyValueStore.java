package com.github.aayushjn.keyvaluestore;

import com.github.aayushjn.keyvaluestore.model.Node;
import com.github.aayushjn.keyvaluestore.model.TCPNode;

import java.io.IOException;

public class KeyValueStore {
    public static void main(String[] args) {
        try (Node node = new TCPNode("127.0.0.1", 8551)) {
            node.listen();
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }
}
