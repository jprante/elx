package org.elasticsearch.action.admin.indices.settings.put;

import org.elasticsearch.common.CheckedFunction;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.xbib.elasticsearch.client.http.HttpAction;
import org.xbib.netty.http.client.RequestBuilder;

import java.io.IOException;
import java.io.UncheckedIOException;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 *
 */
public class HttpUpdateSettingsAction extends HttpAction<UpdateSettingsRequest, UpdateSettingsResponse> {

    @Override
    public UpdateSettingsAction getActionInstance() {
        return UpdateSettingsAction.INSTANCE;
    }

    @Override
    protected RequestBuilder createHttpRequest(String url, UpdateSettingsRequest request) {
        try {
            XContentBuilder builder = jsonBuilder();
            builder.startObject();
            request.settings().toXContent(builder, ToXContent.EMPTY_PARAMS);
            builder.endObject();
            String index = request.indices() != null ? "/" + String.join(",", request.indices()) : "";
            return newPutRequest(url, index + "/_settings", BytesReference.bytes(builder));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    protected CheckedFunction<XContentParser, UpdateSettingsResponse, IOException> entityParser() {
        return parser -> new UpdateSettingsResponse();
    }
}
