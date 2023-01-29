package org.xbib.elx.api;

import java.util.Map;

public interface SearchDocument {

    String getIndex();

    String getId();

    float getScore();

    String getContent();

    Map<String, Object> getFields();
}
