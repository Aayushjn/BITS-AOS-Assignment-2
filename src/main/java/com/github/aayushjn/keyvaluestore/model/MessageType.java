package com.github.aayushjn.keyvaluestore.model;


import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import java.io.Serial;
import java.io.Serializable;
import java.util.Map;

public sealed class MessageType implements Serializable {
    /**
     * GET <key>
     */
    public static final class Get extends MessageType implements Serializable {
        @Serial private static final long serialVersionUID = 20231110183500L;

        public Get(String key) {
            this.key = key;
        }

        @Override
        public String toString() {
            return "GET " + key;
        }
    }

    public static final class Put extends MessageType implements Serializable {
        @Serial private static final long serialVersionUID = 20231110183600L;

        public Put(String key, Object value) {
            this.key = key;
            this.value = value;
        }

        @Override
        public String toString() {
            return "PUT " + key + ' ' + value;
        }
    }

    public static final class Del extends MessageType implements Serializable {
        @Serial private static final long serialVersionUID = 20231110183700L;

        public Del(String key) {
            this.key = key;
        }

        @Override
        public String toString() {
            return "DEL " + key;
        }
    }

    public static final class Store extends MessageType implements Serializable {
        @Serial private static final long serialVersionUID = 20231110183800L;

        @Override
        public String toString() {
            return "STORE";
        }
    }

    public static final class Exit extends MessageType implements Serializable {
        @Serial private static final long serialVersionUID = 20231110183900L;

        public Exit() {}

        public Exit(String peer) {
            this.peer = peer;
        }

        @Override
        public String toString() {
            if ("".equals(peer)) {
                return "EXIT";
            }
            return "EXIT " + peer;
        }
    }

    public static final class Data extends MessageType implements Serializable {
        @Serial private static final long serialVersionUID = 20231110184000L;

        public Data(String key, Object value) {
            this.key = key;
            this.value = value;
        }

        @Override
        public String toString() {
            return "DATA " + key + ' ' + value;
        }
    }

    public static final class DataAll extends MessageType implements Serializable {
        @Serial private static final long serialVersionUID = 20231110184100L;

        public DataAll(Object value) {
            this.value = value;
        }

        @Override
        public String toString() {
            Gson gson = new Gson();
            String actualValue = gson.toJson(value);
            return "DATA_ALL " + actualValue;
        }
    }

    public static final class Owner extends MessageType implements Serializable {
        @Serial private static final long serialVersionUID = 20231110184200L;

        public Owner(String key) {
            this.key = key;
        }

        @Override
        public String toString() {
            return "OWNER " + key;
        }
    }

    public static final class Ack extends MessageType implements Serializable {
        @Serial private static final long serialVersionUID = 20231110184300L;

        public Ack(String key) {
            this.key = key;
        }

        @Override
        public String toString() {
            return "ACK " + key;
        }
    }

    public static final class Nak extends MessageType implements Serializable {
        @Serial private static final long serialVersionUID = 20231110184400L;

        public Nak(String key) {
            this.key = key;
        }

        @Override
        public String toString() {
            return "NAK " + key;
        }
    }

    public static final class Commit extends MessageType implements Serializable {
        @Serial private static final long serialVersionUID = 20231110184500L;

        public Commit(String key) {
            this.key = key;
        }

        public Commit(String key, String peer) {
            this.key = key;
            this.peer = peer;
        }

        @Override
        public String toString() {
            return "COMMIT " + key + "|||" + peer;
        }
    }

    @Serial private static final long serialVersionUID = 20231110182300L;

    private MessageType() {}

    protected String key = "";
    protected Object value = null;
    protected String peer = "";

    public String getKey() {
        return key;
    }

    public Object getValue() {
        return value;
    }

    public String getPeer() {
        return peer;
    }

    public void setPeer(String peer) {
        this.peer = peer;
    }

    public static MessageType parseString(String s) throws IllegalArgumentException {
        MessageType mt = null;
        try {
            if (s.regionMatches(true, 0, "GET", 0, 3)) {
                mt = new MessageType.Get(s.substring(4));
            } else if (s.regionMatches(true, 0, "PUT", 0, 3)) {
                String[] split = s.substring(4).split(" ", 2);
                mt = new MessageType.Put(split[0], split[1]);
            } else if (s.regionMatches(true, 0, "DEL", 0, 3)) {
                mt = new MessageType.Del(s.substring(4));
            } else if (s.regionMatches(true, 0, "STORE", 0, 5)) {
                mt = new MessageType.Store();
            } else if (s.regionMatches(true, 0, "EXIT", 0, 4)) {
                mt = new MessageType.Exit();
                if (s.length() > 4) {
                    mt.peer = s.substring(5);
                }
            } else if (s.regionMatches(true, 0, "DATA_ALL", 0, 8)) {
                Gson gson = new Gson();
                TypeToken<Map<String, Object>> typeToken = new TypeToken<>() {};
                mt = new MessageType.DataAll(gson.fromJson(s.substring(9), typeToken));
            } else if (s.regionMatches(true, 0, "DATA", 0, 4)) {
                String[] split = s.substring(5).split(" ", 2);
                mt = new MessageType.Data(split[0], split[1]);
            } else if (s.regionMatches(true, 0, "OWNER", 0, 5)) {
                mt = new MessageType.Owner(s.substring(6));
            } else if (s.regionMatches(true, 0, "ACK", 0, 3)) {
                mt = new MessageType.Ack(s.substring(4));
            } else if (s.regionMatches(true, 0, "NAK", 0, 3)) {
                mt = new MessageType.Nak(s.substring(4));
            } else if (s.regionMatches(true, 0, "COMMIT", 0, 6)) {
                int sepIndex = s.indexOf("|||");
                mt = new MessageType.Commit(s.substring(7, sepIndex), s.substring(sepIndex + 3));
            }
        } catch (IndexOutOfBoundsException e) {
            throw new IllegalArgumentException("key/value must be specified");
        }

        if (mt == null) throw new IllegalArgumentException("unknown message type: " + s);
        return mt;
    }
}
