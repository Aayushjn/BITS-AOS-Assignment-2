package com.github.aayushjn.keyvaluestore.model;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntUnaryOperator;

/**
 * Abstract class that provides a simple 2-phase commit protocol for the distributed system
 * Unlike normal 2-PC protocol, this variant requires a simple majority, i.e., 51% majority for agreement
 * @param <S> type of data being voted on
 */
public abstract class Agreeable<S> {
    protected final List<String> peers;
    protected final Set<S> votedOn;
    protected final AtomicInteger acks;
    protected final AtomicInteger naks;
    protected int majority;

    protected Agreeable(String... peers) {
        // use a CopyOnWriteArrayList to ensure that list modifications do not block
        this.peers = new CopyOnWriteArrayList<>(peers);
        votedOn = new HashSet<>();
        acks = new AtomicInteger(0);
        naks = new AtomicInteger(0);
        int peerCount = peers.length;
        majority = peerCount > 0 ? (int) Math.floor(peerCount / 2.0) + 1 : 0;
    }

    public boolean hasMajority() {
        return acks.get() >= majority;
    }

    public void updateAcks(IntUnaryOperator operator) {
        acks.getAndUpdate(operator);
    }

    public void resetAcks() {
        acks.set(0);
    }

    public void updateNaks(IntUnaryOperator operator) {
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
}
