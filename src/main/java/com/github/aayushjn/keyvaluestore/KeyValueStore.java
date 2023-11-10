package com.github.aayushjn.keyvaluestore;

import com.github.aayushjn.keyvaluestore.model.MessageType;
import com.github.aayushjn.keyvaluestore.model.Store;
import com.github.aayushjn.keyvaluestore.model.node.Node;
import com.github.aayushjn.keyvaluestore.model.node.RMINode;
import com.github.aayushjn.keyvaluestore.model.node.TCPNode;
import com.github.aayushjn.keyvaluestore.model.node.UDPNode;
import com.github.aayushjn.keyvaluestore.net.Messenger;
import com.github.aayushjn.keyvaluestore.util.ConsoleColor;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.io.*;
import java.util.Map;
import java.util.Objects;

import static com.github.aayushjn.keyvaluestore.model.node.Node.MSG_KEY_NOT_LOCAL;
import static com.github.aayushjn.keyvaluestore.model.node.Node.MSG_OK;

public class KeyValueStore {
    @SuppressWarnings("unchecked")
    public static void main(String[] args) throws IOException {
        final String mode = args[0];
        final String id = args[1];
        final String host = args[2];
        int port = 0;
        try {
            port = Integer.parseInt(args[3]);
            if (port < 0 || port > 65535) {
                System.out.println("Invalid port number provided");
                System.exit(1);
            }
        } catch (NumberFormatException e) {
            System.out.println("Invalid port number provided");
            System.exit(1);
        }
        final String[] peers = args[4].split(",");

        Node node;
        switch (mode) {
            case "tcp" -> node = new TCPNode(host, port, id, peers);
            case "udp" -> node = new UDPNode(host, port, id, peers);
            case "rmi" -> node = new RMINode(host, port, id, peers);
            default -> throw new IllegalArgumentException("unknown mode '" + mode + "'");
        }

        node.listen();

        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(System.out));
        boolean stopped = false;

        Store store = node.getStore();
        Messenger messenger = node.getMessenger();
        String input;
        do {
            bw.write("> ");
            bw.flush();
            input = br.readLine();
            if (input == null) {
                for (String peer : node.getPeers()) {
                    messenger.exit(peer);
                }
                node.close();
                break;
            }
            MessageType mt;
            try {
                mt = MessageType.parseString(input);
            } catch (IllegalArgumentException e) {
                bw.write(ConsoleColor.withForegroundColor(e.getMessage(), 184, 0, 0) + "\n");
                bw.flush();
                continue;
            }

            if (mt instanceof MessageType.Get) {
                if (store.hasLocally(mt.key)) {
                    bw.write(store.get(mt.key).toString() + "\n");
                } else if (store.hasRemotely(mt.key)) {
                    String peer = store.getPeerForKey(mt.key);
                    MessageType resp = messenger.getValueForKey(mt, peer);
                    bw.write(
                        Objects.requireNonNullElseGet(
                            resp.value,
                            () -> ConsoleColor.withForegroundColor("Could not get data from peer", 184, 0, 0)
                        ) + "\n"
                    );
                } else {
                    bw.write(ConsoleColor.withForegroundColor(MSG_KEY_NOT_LOCAL, 184, 0, 0) + "\n");
                }
            } else if (mt instanceof MessageType.Put) {
                if (store.hasLocally(mt.key)) {
                    store.put(mt.key, mt.value);
                    bw.write(ConsoleColor.withForegroundColor(MSG_OK, 166, 166, 166) + "\n");
                } else if (store.hasRemotely(mt.key)) {
                    bw.write(ConsoleColor.withForegroundColor(MSG_KEY_NOT_LOCAL, 184, 0, 0) + "\n");
                } else {
                    node.getVotedOn().add(mt.key);
                    MessageType msg = new MessageType.Owner();
                    msg.key = mt.key;
                    MessageType resp;
                    for (String peer : node.getPeers()) {
                        resp = messenger.requestAcknowledgement(msg, peer);
                        if (resp instanceof MessageType.Ack) {
                            node.updateAcks(i -> i + 1);
                        } else if (resp instanceof MessageType.Nak) {
                            node.updateNaks(i -> i + 1);
                        }
                    }
                    if (node.hasMajority()) {
                        store.put(mt.key, mt.value);
                        msg = new MessageType.Commit();
                        msg.key = mt.key;
                        for (String peer : node.getPeers()) {
                            messenger.commitKey(msg, peer);
                        }
                        bw.write(ConsoleColor.withForegroundColor(MSG_OK, 166, 166, 166) + "\n");
                    } else {
                        bw.write(ConsoleColor.withForegroundColor("Cannot write data", 184, 0, 0) + "\n");
                    }
                    node.resetAcks();
                    node.resetNaks();
                    node.getVotedOn().remove(mt.key);
                }
            } else if (mt instanceof MessageType.Del) {
                if (store.hasLocally(mt.key)) {
                    store.delete(mt.key);
                    for (String peer : node.getPeers()) {
                        messenger.deleteKey(mt, peer);
                    }
                    bw.write(ConsoleColor.withForegroundColor(MSG_OK, 166, 166, 166) + "\n");
                } else {
                    bw.write(ConsoleColor.withForegroundColor(MSG_KEY_NOT_LOCAL, 184, 0, 0) + "\n");
                }
            } else if (mt instanceof MessageType.Store) {
                Map<String, Object> localStore = store.getAll();
                MessageType resp;
                for (String peer : node.getPeers()) {
                    resp = messenger.getAllData(peer);
                    localStore.putAll((Map<String, Object>) resp.value);
                }
                Gson gson = new GsonBuilder().setPrettyPrinting().create();
                String data = gson.toJson(localStore);
                if (data.length() > 65535) {
                    bw.write(data.substring(0, 65535) + " <trimmed>...\n");
                } else {
                    bw.write(data + "\n");
                }
            } else if (mt instanceof MessageType.Exit) {
                for (String peer : node.getPeers()) {
                    messenger.exit(peer);
                }
                stopped = true;
            } else {
                bw.write("Unsupported message type received");
            }

            bw.flush();
        } while (!stopped);

        node.close();
        System.exit(0);
    }
}
