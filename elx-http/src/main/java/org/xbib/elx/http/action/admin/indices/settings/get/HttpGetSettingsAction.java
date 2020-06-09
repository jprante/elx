package org.xbib.elx.http.action.admin.indices.settings.get;

import org.elasticsearch.action.admin.indices.settings.get.GetSettingsAction;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsRequest;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.elasticsearch.common.CheckedFunction;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParserUtils;
import org.xbib.elx.http.HttpAction;
import org.xbib.netty.http.client.api.Request;
import org.xbib.netty.http.common.HttpResponse;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

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
        return this::fromXContent;
    }

    private GetSettingsResponse fromXContent(XContentParser parser) throws IOException {
        Map<String, Settings> indexToSettings = new HashMap<>();
        Map<String, Settings> indexToDefaultSettings = new HashMap<>();
        if (parser.currentToken() == null) {
            parser.nextToken();
        }
        XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser::getTokenLocation);
        parser.nextToken();
        while (!parser.isClosed()) {
            if (parser.currentToken() == XContentParser.Token.START_OBJECT) {
                parseIndexEntry(parser, indexToSettings, indexToDefaultSettings);
            } else if (parser.currentToken() == XContentParser.Token.START_ARRAY) {
                parser.skipChildren();
            } else {
                parser.nextToken();
            }
        }
        ImmutableOpenMap<String, Settings> settingsMap = ImmutableOpenMap.<String, Settings>builder().putAll(indexToSettings).build();
        return new GetSettingsResponse(settingsMap);
    }

    private static void parseIndexEntry(XContentParser parser, Map<String, Settings> indexToSettings,
                                        Map<String, Settings> indexToDefaultSettings) throws IOException {
        String indexName = parser.currentName();
        parser.nextToken();
        while (!parser.isClosed() && parser.currentToken() != XContentParser.Token.END_OBJECT) {
            parseSettingsField(parser, indexName, indexToSettings, indexToDefaultSettings);
        }
    }

    private static void parseSettingsField(XContentParser parser, String currentIndexName, Map<String, Settings> indexToSettings,
                                           Map<String, Settings> indexToDefaultSettings) throws IOException {
        if (parser.currentToken() == XContentParser.Token.START_OBJECT) {
            switch (parser.currentName()) {
                case "settings":
                    indexToSettings.put(currentIndexName, Settings.fromXContent(parser));
                    break;
                case "defaults":
                    indexToDefaultSettings.put(currentIndexName, Settings.fromXContent(parser));
                    break;
                default:
                    parser.skipChildren();
            }
        } else if (parser.currentToken() == XContentParser.Token.START_ARRAY) {
            parser.skipChildren();
        }
        parser.nextToken();
    }
}
