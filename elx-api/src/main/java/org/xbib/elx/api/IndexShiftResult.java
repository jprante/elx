package org.xbib.elx.api;

import java.util.Collection;

public interface IndexShiftResult {

    Collection<String> getMovedAliases();

    Collection<String> getNewAliases();
}
