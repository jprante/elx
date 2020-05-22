package org.xbib.elx.api;

import java.util.List;

public interface IndexPruneResult {

    enum State { NOTHING_TO_DO, SUCCESS, NONE, FAIL };

    State getState();

    List<String> getCandidateIndices();

    List<String> getDeletedIndices();

    boolean isAcknowledged();
}
