package org.xbib.elx.http.action.admin.indices.exists.indices;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsAction;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.common.CheckedFunction;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.rest.RestStatus;
import org.xbib.elx.http.HttpAction;
import org.xbib.elx.http.HttpActionContext;
import org.xbib.netty.http.client.api.Request;
import org.xbib.netty.http.common.HttpResponse;

import java.io.IOException;

public class HttpIndicesExistsAction extends HttpAction<IndicesExistsRequest, IndicesExistsResponse> {

    @Override
    public IndicesExistsAction getActionInstance() {
        return IndicesExistsAction.INSTANCE;
    }

    @Override
    protected Request.Builder createHttpRequest(String url, IndicesExistsRequest request) {
        String index = String.join(",", request.indices());
        return newHeadRequest(url, index);
    }

    /**
     * Override for non-body logic.
     * @param httpActionContext the HTTP action context
     * @return the ELasticsearch sttatus exception
     */
    @Override
    protected ElasticsearchStatusException parseToError(HttpActionContext<IndicesExistsRequest, IndicesExistsResponse> httpActionContext) {
        return new ElasticsearchStatusException("not found", RestStatus.NOT_FOUND);
    }

    @Override
    protected CheckedFunction<XContentParser, IndicesExistsResponse, IOException> entityParser(HttpResponse httpResponse) {
        return httpResponse.getStatus().getCode() == 200 ? this::found : this::notfound;
    }

    private IndicesExistsResponse found(XContentParser parser) {
        return new IndicesExistsResponse(true);
    }

    private IndicesExistsResponse notfound(XContentParser parser) {
        return new IndicesExistsResponse(false);
    }

}