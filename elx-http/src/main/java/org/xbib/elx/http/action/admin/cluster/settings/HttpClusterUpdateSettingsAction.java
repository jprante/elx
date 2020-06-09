package org.xbib.elx.http.action.admin.cluster.settings;

import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsAction;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsResponse;
import org.elasticsearch.common.CheckedFunction;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.xbib.elx.http.HttpAction;
import org.xbib.netty.http.client.api.Request;
import org.xbib.netty.http.common.HttpResponse;

import java.io.IOException;
import java.io.UncheckedIOException;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

public class HttpClusterUpdateSettingsAction extends HttpAction<ClusterUpdateSettingsRequest, ClusterUpdateSettingsResponse> {

    @Override
    public ClusterUpdateSettingsAction getActionInstance() {
        return ClusterUpdateSettingsAction.INSTANCE;
    }

    @Override
    protected Request.Builder createHttpRequest(String url, ClusterUpdateSettingsRequest request) {
        try {
            XContentBuilder builder = jsonBuilder();
            builder.startObject().startObject("persistent");
            request.persistentSettings().toXContent(builder, ToXContent.EMPTY_PARAMS);
            builder.endObject();
            builder.startObject("transient");
            request.transientSettings().toXContent(builder, ToXContent.EMPTY_PARAMS);
            builder.endObject().endObject();
            return newPutRequest(url, "/_cluster/settings", BytesReference.bytes(builder));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    protected CheckedFunction<XContentParser, ClusterUpdateSettingsResponse, IOException> entityParser(HttpResponse httpResponse) {
        return ClusterUpdateSettingsResponse::fromXContent;
    }
}
