package com.github.aayushjn.keyvaluestore.model;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.UnaryOperator;

public abstract class Agreeable<S> {
    protected final List<String> peers;
    protected final Set<S> votedOn;
    protected final AtomicReference<Integer> acks;
    protected final AtomicReference<Integer> naks;
    protected int majority;

    protected Agreeable(String... peers) {
        this.peers = new CopyOnWriteArrayList<>(peers);
        votedOn = new HashSet<>();
        acks = new AtomicReference<>(0);
        naks = new AtomicReference<>(0);
        int peerCount = peers.length;
        majority = peerCount > 0 ? (int) Math.floor(peerCount / 2.0) + 1 : 0;
    }

    public boolean hasMajority() {
        return acks.get() >= majority;
    }

    public void updateAcks(UnaryOperator<Integer> operator) {
        acks.getAndUpdate(operator);
    }

    public void resetAcks() {
        acks.set(0);
    }

    public void updateNaks(UnaryOperator<Integer> operator) {
        naks.getAndUpdate(operator);
    }

    public void resetNaks() {
        naks.set(0);
    }

    public Set<S> getVotedOn() {
        return votedOn;
    }

    public List<String> getPeers() {
        return peers;
    }

    public void recomputeMajority() {
        int peerCount = peers.size();
        majority = peerCount > 0 ? (int) Math.floor(peerCount / 2.0) + 1 : 0;
    }

    protected static final int AGREEMENT_DELAY = 1000;
}
