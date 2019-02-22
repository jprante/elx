package org.xbib.elx.http.action.admin.cluster.state;

import org.elasticsearch.action.admin.cluster.state.ClusterStateAction;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.common.CheckedFunction;
import org.elasticsearch.common.xcontent.XContentParser;
import org.xbib.elx.http.HttpAction;
import org.xbib.netty.http.client.RequestBuilder;

import java.io.IOException;

public class HttpClusterStateAction extends HttpAction<ClusterStateRequest, ClusterStateResponse> {

    @Override
    public ClusterStateAction getActionInstance() {
        return ClusterStateAction.INSTANCE;
    }

    @Override
    protected RequestBuilder createHttpRequest(String url, ClusterStateRequest request) {
        return newPutRequest(url, "/_cluster/state");
    }

    @Override
    protected CheckedFunction<XContentParser, ClusterStateResponse, IOException> entityParser() {
        throw new UnsupportedOperationException();
    }
}
