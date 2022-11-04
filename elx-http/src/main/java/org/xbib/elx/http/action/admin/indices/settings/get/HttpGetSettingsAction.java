package org.xbib.elx.http.action.admin.indices.settings.get;

import org.elasticsearch.action.admin.indices.settings.get.GetSettingsAction;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsRequest;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.elasticsearch.common.CheckedFunction;
import org.elasticsearch.common.xcontent.XContentParser;
import org.xbib.elx.http.HttpAction;
import org.xbib.net.http.client.HttpResponse;
import org.xbib.net.http.client.netty.HttpRequestBuilder;

import java.io.IOException;

public class HttpGetSettingsAction extends HttpAction<GetSettingsRequest, GetSettingsResponse> {

    @Override
    public GetSettingsAction getActionInstance() {
        return GetSettingsAction.INSTANCE;
    }

    @Override
    protected HttpRequestBuilder createHttpRequest(String url, GetSettingsRequest request) {
        // beware, request.indices() is always an empty array
        String index = request.indices() != null ? String.join(",", request.indices()) + "/" : "";
        return newGetRequest(url, index + "_settings");
    }

    @Override
    protected CheckedFunction<XContentParser, GetSettingsResponse, IOException> entityParser(HttpResponse httpResponse) {
        return GetSettingsResponse::fromXContent;
    }
}
