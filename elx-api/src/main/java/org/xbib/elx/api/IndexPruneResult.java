package org.xbib.elx.api;

import java.util.Collection;

public interface IndexPruneResult {

    enum State { SUCCESS, NONE };

    State getState();

    Collection<String> getCandidateIndices();

    Collection<String> getDeletedIndices();

    boolean isAcknowledged();
}
