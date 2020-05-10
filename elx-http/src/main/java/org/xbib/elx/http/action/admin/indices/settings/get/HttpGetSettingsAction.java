package org.xbib.elx.http.action.admin.indices.settings.get;

import org.elasticsearch.action.admin.indices.settings.get.GetSettingsAction;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsRequest;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.elasticsearch.common.CheckedFunction;
import org.elasticsearch.common.xcontent.XContentParser;
import org.xbib.elx.http.HttpAction;
import org.xbib.netty.http.client.api.Request;
import org.xbib.netty.http.common.HttpResponse;

import java.io.IOException;

public class HttpGetSettingsAction extends HttpAction<GetSettingsRequest, GetSettingsResponse> {

    @Override
    public GetSettingsAction getActionInstance() {
        return GetSettingsAction.INSTANCE;
    }

    @Override
    protected Request.Builder createHttpRequest(String url, GetSettingsRequest request) {
        // beware, request.indices() is always an empty array
        String index = request.indices() != null ? String.join(",", request.indices()) + "/" : "";
        return newGetRequest(url, index + "_settings");
    }

    @Override
    protected CheckedFunction<XContentParser, GetSettingsResponse, IOException> entityParser(HttpResponse httpResponse) {
        return GetSettingsResponse::fromXContent;
    }
}
