package com.github.aayushjn.keyvaluestore.model;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

public abstract class Agreeable<T, S> {
    protected final AtomicReference<T> awaitingData;
    protected final Set<S> votedOn;
    protected final AtomicReference<Integer> acks;
    protected final AtomicReference<Integer> naks;
    protected final int majority;

    protected Agreeable(int peerCount) {
        awaitingData = new AtomicReference<>(null);
        votedOn = new HashSet<>();
        acks = new AtomicReference<>(0);
        naks = new AtomicReference<>(0);
        majority = peerCount > 0 ? (int) Math.floor(peerCount / 2.0) + 1 : 0;
    }

    protected boolean hasMajority() {
        return acks.get() >= majority;
    }

    protected static final int AGREEMENT_DELAY = 1000;
}
