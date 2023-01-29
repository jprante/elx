package org.xbib.elx.common;

import org.elasticsearch.action.get.MultiGetItemResponse;
import org.xbib.elx.api.SearchDocument;
import java.util.Map;

public class MultiGetDocument implements SearchDocument {

    private final MultiGetItemResponse getResponse;

    public MultiGetDocument(MultiGetItemResponse getResponse) {
        this.getResponse = getResponse;
    }

    @Override
    public String getIndex() {
        return getResponse.getResponse().getIndex();
    }

    @Override
    public String getId() {
        return getResponse.getResponse().getId();
    }

    @Override
    public float getScore() {
        return -1f;
    }

    @Override
    public String getContent() {
        return getResponse.getResponse().getSourceAsBytesRef().utf8ToString();
    }

    @Override
    public Map<String, Object> getFields() {
        return getResponse.getResponse().getSourceAsMap();
    }
}
