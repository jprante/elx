package org.xbib.elx.api;

public interface IndexRetention {

    IndexRetention setDelta(int delta);

    int getDelta();

    IndexRetention setMinToKeep(int minToKeep);

    int getMinToKeep();

}
