package org.xbib.elx.common;

import org.xbib.elx.api.IndexRetention;

public class DefaultIndexRetention implements IndexRetention {

    private int delta;

    private int minToKeep;

    @Override
    public IndexRetention setDelta(int delta) {
        this.delta = delta;
        return this;
    }

    @Override
    public int getDelta() {
        return delta;
    }

    @Override
    public IndexRetention setMinToKeep(int minToKeep) {
        this.minToKeep = minToKeep;
        return this;
    }

    @Override
    public int getMinToKeep() {
        return minToKeep;
    }
}
