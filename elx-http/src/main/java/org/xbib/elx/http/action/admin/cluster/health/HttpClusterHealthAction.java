package org.xbib.elx.http.action.admin.cluster.health;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthAction;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.common.CheckedFunction;
import org.elasticsearch.common.xcontent.XContentParser;
import org.xbib.elx.http.HttpAction;
import org.xbib.netty.http.client.api.Request;
import org.xbib.netty.http.common.HttpResponse;

import java.io.IOException;

public class HttpClusterHealthAction extends HttpAction<ClusterHealthRequest, ClusterHealthResponse> {

    @Override
    public ClusterHealthAction getActionInstance() {
        return ClusterHealthAction.INSTANCE;
    }

    @Override
    protected Request.Builder createHttpRequest(String url, ClusterHealthRequest request) {
        return newGetRequest(url, "/_cluster/health");
    }

    @Override
    protected CheckedFunction<XContentParser, ClusterHealthResponse, IOException> entityParser(HttpResponse httpResponse) {
        return HttpClusterHealthResponse::fromXContent;
    }
}
