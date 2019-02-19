package org.xbib.elx.api;

public class IndexRetention {

    private int timestampDiff;

    private int minToKeep;

    public IndexRetention setDelta(int timestampDiff) {
        this.timestampDiff = timestampDiff;
        return this;
    }

    public int getDelta() {
        return timestampDiff;
    }

    public IndexRetention setMinToKeep(int minToKeep) {
        this.minToKeep = minToKeep;
        return this;
    }

    public int getMinToKeep() {
        return minToKeep;
    }

}
