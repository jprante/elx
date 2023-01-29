package org.xbib.elx.common;

import org.elasticsearch.action.get.GetResponse;
import org.xbib.elx.api.SearchDocument;
import java.util.Map;

public class GetDocument implements SearchDocument {

    private final GetResponse getResponse;

    public GetDocument(GetResponse getResponse) {
        this.getResponse = getResponse;
    }

    @Override
    public String getIndex() {
        return getResponse.getIndex();
    }

    @Override
    public String getId() {
        return getResponse.getId();
    }

    @Override
    public float getScore() {
        return -1f;
    }

    @Override
    public String getContent() {
        return getResponse.getSourceAsBytesRef().utf8ToString();
    }

    @Override
    public Map<String, Object> getFields() {
        return getResponse.getSourceAsMap();
    }
}
