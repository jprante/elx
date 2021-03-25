package org.xbib.elx.common;

import org.xbib.elx.api.IndexRetention;

public class DefaultIndexRetention implements IndexRetention {

    private int delta;

    private int minToKeep;

    public DefaultIndexRetention() {
        this.delta = 2;
        this.minToKeep = 2;
    }

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
