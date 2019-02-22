package org.xbib.elx.api;

import java.util.List;

public interface IndexShiftResult {

    List<String> getMovedAliases();

    List<String> getNewAliases();
}
