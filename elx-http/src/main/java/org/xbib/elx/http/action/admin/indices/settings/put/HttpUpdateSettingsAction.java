package org.xbib.elx.http.action.admin.indices.settings.put;

import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsAction;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.common.CheckedFunction;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.xbib.elx.http.HttpAction;
import org.xbib.netty.http.client.api.Request;
import org.xbib.netty.http.common.HttpResponse;

import java.io.IOException;
import java.io.UncheckedIOException;

public class HttpUpdateSettingsAction extends HttpAction<UpdateSettingsRequest, AcknowledgedResponse> {

    @Override
    public UpdateSettingsAction getActionInstance() {
        return UpdateSettingsAction.INSTANCE;
    }

    @Override
    protected Request.Builder createHttpRequest(String url, UpdateSettingsRequest request) {
        try {
            XContentBuilder builder = XContentFactory.jsonBuilder();
            builder.startObject();
            request.settings().toXContent(builder, ToXContent.EMPTY_PARAMS);
            builder.endObject();
            String index = request.indices() != null ? String.join(",", request.indices()) + "/" : "";
            return newPutRequest(url, "/" + index + "_settings", BytesReference.bytes(builder));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    protected CheckedFunction<XContentParser, AcknowledgedResponse, IOException> entityParser(HttpResponse httpResponse) {
        return AcknowledgedResponse::fromXContent;
    }
}
