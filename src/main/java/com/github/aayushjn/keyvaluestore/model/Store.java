package com.github.aayushjn.keyvaluestore.model;

import java.util.*;

public class Store {
    private final Map<String, Object> localStore;
    private final Map<String, String> peerStore;

    public Store() {
        localStore = new Hashtable<>();
        peerStore = new Hashtable<>();
    }

    public void putPeerForKey(String key, String peerInfo) {
        peerStore.computeIfAbsent(key, k -> peerInfo);
    }

    public void removePeerForKey(String key) {
        peerStore.remove(key);
    }

    public void removePeer(String peer) {
        Iterator<Map.Entry<String, String>> iterator = peerStore.entrySet().iterator();
        Map.Entry<String, String> entry;
        while (iterator.hasNext()) {
            entry = iterator.next();
            if (peer.equals(entry.getValue())) {
                iterator.remove();
            }
        }
    }

    public String getPeerForKey(String key) {
        return peerStore.get(key);
    }

    public Object get(String key) {
        if (!localStore.containsKey(key)) {
            throw new IllegalArgumentException("Key " + key + " is owned by someone else");
        }
        return localStore.getOrDefault(key, null);
    }

    public Map<String, Object> getAll() {
        return localStore;
    }

    public void put(String key, Object value) {
        if (peerStore.containsKey(key)) {
            throw new IllegalArgumentException("Key " + key + " is owned by someone else");
        }
        localStore.put(key, value);
    }

    public void delete(String key) {
        if (!localStore.containsKey(key)) {
            throw new IllegalArgumentException("Key " + key + " is not present locally");
        }
        localStore.remove(key);
    }

    public boolean hasLocally(String key) {
        return localStore.containsKey(key);
    }

    public boolean hasRemotely(String key) {
        return peerStore.containsKey(key);
    }

    public boolean hasKey(String key) {
        return hasLocally(key) || hasRemotely(key);
    }

    public Map<String, List<String>> getKeysPerPeer() {
        final Map<String, List<String>> mapping = new HashMap<>();

        for (Map.Entry<String, String> entry : peerStore.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();

            mapping.compute(value, (k, v) -> {
                v = v != null ? new ArrayList<>(v) : new ArrayList<>();
                v.add(key);
                return v;
            });
        }

        return mapping;
    }
}
