package com.github.aayushjn.keyvaluestore;

import com.github.aayushjn.keyvaluestore.model.MessageType;
import com.github.aayushjn.keyvaluestore.model.Store;
import com.github.aayushjn.keyvaluestore.model.node.Node;
import com.github.aayushjn.keyvaluestore.model.node.RMINode;
import com.github.aayushjn.keyvaluestore.model.node.TCPNode;
import com.github.aayushjn.keyvaluestore.model.node.UDPNode;
import com.github.aayushjn.keyvaluestore.net.Messenger;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.fusesource.jansi.AnsiConsole;

import java.io.*;
import java.util.Map;
import java.util.Objects;

import static com.github.aayushjn.keyvaluestore.model.MessageType.DataAll.DATA_LIMIT;
import static com.github.aayushjn.keyvaluestore.model.node.Node.MSG_KEY_NOT_LOCAL;
import static com.github.aayushjn.keyvaluestore.model.node.Node.MSG_OK;
import static org.fusesource.jansi.Ansi.ansi;

public class KeyValueStore {

    @SuppressWarnings("unchecked")
    public static void main(String[] args) {
        if (args.length < 4) {
            System.err.println("Insufficient arguments passed");
            System.exit(1);
        }
        final String mode = args[0];
        final String host = args[1];
        int port = 0;
        try {
            port = Integer.parseInt(args[2]);
            if (port < 0 || port > 65535) {
                System.err.println("Invalid port number provided");
                System.exit(1);
            }
        } catch (NumberFormatException e) {
            System.err.println("Invalid port number provided");
            System.exit(1);
        }
        final String[] peers = args[3].split(",");

        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(System.out));
        Node node = null;
        AnsiConsole.systemInstall();
        try {
            switch (mode) {
                case "tcp" -> node = new TCPNode(host, port, peers);
                case "udp" -> node = new UDPNode(host, port, peers);
                case "rmi" -> node = new RMINode(host, port, peers);
                default -> throw new IllegalArgumentException("unknown mode '" + mode + "'");
            }

            node.listen();

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
                    bw.write(ansi().fgRgb(184, 0, 0).a(e.getMessage()).reset() + "\n");
                    bw.flush();
                    continue;
                }

                if (mt instanceof MessageType.Get) {
                    if (store.hasLocally(mt.getKey())) {
                        bw.write(store.get(mt.getKey()).toString() + "\n");
                    } else if (store.hasRemotely(mt.getKey())) {
                        String peer = store.getPeerForKey(mt.getKey());
                        MessageType resp = messenger.getValueForKey(mt, peer);
                        bw.write(
                            Objects.requireNonNullElseGet(
                                resp.getValue(),
                                () -> ansi().fgRgb(184, 0, 0).a("Could not get data from peer").reset()
                            ) + "\n"
                        );
                    } else {
                        bw.write(ansi().fgRgb(184, 0, 0).a(MSG_KEY_NOT_LOCAL).reset() + "\n");
                    }
                } else if (mt instanceof MessageType.Put) {
                    if (store.hasLocally(mt.getKey())) {
                        store.put(mt.getKey(), mt.getValue());
                        bw.write(ansi().fgRgb(166, 166, 166).a(MSG_OK).reset() + "\n");
                    } else if (store.hasRemotely(mt.getKey())) {
                        bw.write(ansi().fgRgb(184, 0, 0).a(MSG_KEY_NOT_LOCAL).reset() + "\n");
                    } else {
                        node.getVotedOn().add(mt.getKey());
                        MessageType msg = new MessageType.Owner(mt.getKey());
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
                            store.put(mt.getKey(), mt.getValue());
                            msg = new MessageType.Commit(mt.getKey());
                            for (String peer : node.getPeers()) {
                                messenger.commitKey(msg, peer);
                            }
                            bw.write(ansi().fgRgb(166, 166, 166).a(MSG_OK).reset() + "\n");
                        } else {
                            bw.write(ansi().fgRgb(184, 0, 0).a("Cannot write data").reset() + "\n");
                        }
                        node.resetAcks();
                        node.resetNaks();
                        node.getVotedOn().remove(mt.getKey());
                    }
                } else if (mt instanceof MessageType.Del) {
                    if (store.hasLocally(mt.getKey())) {
                        store.delete(mt.getKey());
                        for (String peer : node.getPeers()) {
                            messenger.deleteKey(mt, peer);
                        }
                        bw.write(ansi().fgRgb(166, 166, 166).a(MSG_OK).reset() + "\n");
                    } else {
                        bw.write(ansi().fgRgb(184, 0, 0).a(MSG_KEY_NOT_LOCAL).reset() + "\n");
                    }
                } else if (mt instanceof MessageType.Store) {
                    Map<String, Object> localStore = store.getAll();
                    MessageType resp;
                    for (String peer : node.getPeers()) {
                        resp = messenger.getAllData(peer);
                        localStore.putAll((Map<String, Object>) resp.getValue());
                    }
                    Gson gson = new GsonBuilder().setPrettyPrinting().create();
                    String data = gson.toJson(localStore);
                    if (data.length() > DATA_LIMIT) {
                        bw.write("TRIMMED:" + data.substring(0, DATA_LIMIT) + "\n");
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
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            AnsiConsole.systemUninstall();
            if (node != null) {
                try {
                    br.close();
                    bw.close();
                    node.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        System.exit(0);
    }
}
