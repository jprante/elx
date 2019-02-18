package org.xbib.elx.common.management;

public class IndexRetention {

    private int timestampDiff;

    private int minToKeep;

    public IndexRetention setTimestampDiff(int timestampDiff) {
        this.timestampDiff = timestampDiff;
        return this;
    }

    public int getTimestampDiff() {
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
