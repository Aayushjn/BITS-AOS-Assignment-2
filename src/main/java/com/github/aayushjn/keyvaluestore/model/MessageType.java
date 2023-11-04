package com.github.aayushjn.keyvaluestore.model;


import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import java.util.Map;
import java.util.NoSuchElementException;

public enum MessageType {
    // GET <key> -> fetch locally or remotely
    GET,
    // PUT <key> <value> -> write locally
    PUT,
    // DELETE <key> -> delete locally
    DELETE,
    // STORE -> fetch all locally and remotely
    STORE,
    // EXIT -> exit from program
    EXIT,
    // DATA <key> <value> -> value for a given key
    DATA,
    // DATA_ALL <json_data> -> value for a given key
    DATA_ALL,
    // OWNER <key> -> agreement initialization message
    OWNER,
    // ACK <key> -> agree with OWNER message
    ACK,
    // NAK <key> -> disagree with OWNER message
    NAK,
    // COMMIT <key> -> agreement completion message
    COMMIT;

    String key = "";
    Object value = null;

    @Override
    public String toString() throws NoSuchElementException {
        StringBuilder sb;
        switch (this) {
            case GET -> sb = new StringBuilder(4 + key.length()).append("GET ").append(key);
            case PUT -> {
                String actualValue = value.toString();
                sb = new StringBuilder(5 + key.length() + actualValue.length())
                        .append("PUT ")
                        .append(key)
                        .append(' ')
                        .append(actualValue);
            }
            case DELETE -> sb = new StringBuilder(7 + key.length()).append("DELETE ").append(key);
            case STORE -> {
                return "STORE";
            }
            case EXIT -> {
                return "EXIT";
            }
            case DATA -> {
                String actualValue = value.toString();
                sb = new StringBuilder(6 + key.length() + actualValue.length())
                        .append("DATA ")
                        .append(key)
                        .append(' ')
                        .append(actualValue);
            }
            case DATA_ALL -> {
                Gson gson = new Gson();
                String actualValue = gson.toJson(value);
                sb = new StringBuilder(10 + actualValue.length())
                        .append("DATA_ALL ")
                        .append(actualValue);
            }
            case OWNER -> sb = new StringBuilder(6 + key.length()).append("OWNER ").append(key);
            case ACK -> sb = new StringBuilder(4 + key.length()).append("ACK ").append(key);
            case NAK -> sb = new StringBuilder(4 + key.length()).append("NAK ").append(key);
            case COMMIT -> sb = new StringBuilder(7 + key.length()).append("COMMIT ").append(key);
            default -> throw new NoSuchElementException("unknown message type");
        }
        return sb.toString();
    }

    public static MessageType parseString(String s) throws IllegalArgumentException {
        MessageType mt = null;
        try {
            if (s.regionMatches(true, 0, "GET", 0, 3)) {
                mt = MessageType.GET;
                mt.key = s.substring(4);
            } else if (s.regionMatches(true, 0, "PUT", 0, 3)) {
                mt = MessageType.PUT;
                String[] split = s.substring(4).split(" ", 2);
                mt.key = split[0];
                mt.value = split[1];
            } else if (s.regionMatches(true, 0, "DELETE", 0, 6)) {
                mt = MessageType.DELETE;
                mt.key = s.substring(7);
            } else if (s.regionMatches(true, 0, "STORE", 0, 5)) {
                mt = MessageType.STORE;
            } else if (s.regionMatches(true, 0, "EXIT", 0, 4)) {
                mt = MessageType.EXIT;
            } else if (s.regionMatches(true, 0, "DATA", 0, 4)) {
                mt = MessageType.DATA;
                String[] split = s.substring(5).split(" ", 2);
                mt.key = split[0];
                mt.value = split[1];
            } else if (s.regionMatches(true, 0, "DATA_ALL", 0, 8)) {
                Gson gson = new Gson();
                mt = MessageType.DATA_ALL;
                TypeToken<Map<String, Object>> typeToken = new TypeToken<>() {};
                mt.value = gson.fromJson(s.substring(9), typeToken);
            } else if (s.regionMatches(true, 0, "OWNER", 0, 5)) {
                mt = MessageType.OWNER;
                mt.key = s.substring(6);
            } else if (s.regionMatches(true, 0, "ACK", 0, 3)) {
                mt = MessageType.ACK;
                mt.key = s.substring(4);
            } else if (s.regionMatches(true, 0, "NAK", 0, 3)) {
                mt = MessageType.NAK;
                mt.key = s.substring(4);
            } else if (s.regionMatches(true, 0, "COMMIT", 0, 6)) {
                mt = MessageType.COMMIT;
                mt.key = s.substring(7);
            }
        } catch (IndexOutOfBoundsException e) {
            throw new IllegalArgumentException("key/value must be specified");
        }

        if (mt == null) throw new IllegalArgumentException("unknown message type: " + s);
        return mt;
    }
}
