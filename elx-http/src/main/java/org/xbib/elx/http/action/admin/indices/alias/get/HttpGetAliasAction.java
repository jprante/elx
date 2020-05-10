package org.xbib.elx.http.action.admin.indices.alias.get;

import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesAction;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesResponse;
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.common.CheckedFunction;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.xcontent.XContentParser;
import org.xbib.elx.http.HttpAction;
import org.xbib.netty.http.client.api.Request;
import org.xbib.netty.http.common.HttpResponse;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;

public class HttpGetAliasAction extends HttpAction<GetAliasesRequest, GetAliasesResponse> {

    @Override
    protected Request.Builder createHttpRequest(String url, GetAliasesRequest request) {
        // beware of this inconsistency, request.indices() always return empty array
        String index = request.indices() != null ? String.join(",", request.indices()) + "/" : "";
        String aliases = request.aliases() != null ? String.join(",", request.aliases()) + "/" : "";
        // do not add "/" in front of index
        return newGetRequest(url, index + "_alias/" + aliases);
    }

    @Override
    public ActionType<GetAliasesResponse> getActionInstance() {
        return GetAliasesAction.INSTANCE;
    }

    @Override
    protected CheckedFunction<XContentParser, GetAliasesResponse, IOException> entityParser(HttpResponse httpResponse) {
        return this::fromXContent;
    }

    private GetAliasesResponse fromXContent(XContentParser parser) throws IOException {
        if (parser.currentToken() == null) {
            parser.nextToken();
        }
        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser::getTokenLocation);
        ImmutableOpenMap.Builder<String, List<AliasMetaData>> aliasesBuilder = ImmutableOpenMap.builder();
        while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
            if (parser.currentToken() == XContentParser.Token.FIELD_NAME) {
                String indexName = parser.currentName();
                if (parser.nextToken() == XContentParser.Token.START_OBJECT) {
                    List<AliasMetaData> parseInside = parseAliases(parser);
                    aliasesBuilder.put(indexName, parseInside);
                }
            }
        }
        return new GetAliasesResponse(aliasesBuilder.build());
    }

    private static List<AliasMetaData> parseAliases(XContentParser parser) throws IOException {
        List<AliasMetaData> aliases = new ArrayList<>();
        XContentParser.Token token;
        String currentFieldName = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                if ("aliases".equals(currentFieldName)) {
                    while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
                        AliasMetaData fromXContent = AliasMetaData.Builder.fromXContent(parser);
                        aliases.add(fromXContent);
                    }
                } else {
                    parser.skipChildren();
                }
            } else if (token == XContentParser.Token.START_ARRAY) {
                parser.skipChildren();
            }
        }
        return aliases;
    }
}