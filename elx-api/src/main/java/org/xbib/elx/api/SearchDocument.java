package org.xbib.elx.api;

import java.util.Map;

public interface SearchDocument {

    String getIndex();

    String getId();

    float getScore();

    Map<String, Object> getFields();
}
