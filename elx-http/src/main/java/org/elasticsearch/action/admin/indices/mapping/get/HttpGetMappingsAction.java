package org.elasticsearch.action.admin.indices.mapping.get;

import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.CheckedFunction;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.xcontent.XContentParser;
import org.xbib.elx.http.HttpAction;
import org.xbib.netty.http.client.api.Request;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;

public class HttpGetMappingsAction extends HttpAction<GetMappingsRequest, GetMappingsResponse> {

    private static final ParseField MAPPINGS = new ParseField("mappings");

    @Override
    public GetMappingsAction getActionInstance() {
        return GetMappingsAction.INSTANCE;
    }

    @Override
    protected Request.Builder createHttpRequest(String url, GetMappingsRequest request) {
        String index = request.indices() != null ? "/" + String.join(",", request.indices()) : "";
        return newGetRequest(url, index + "/_mapping");
    }

    @Override
    protected CheckedFunction<XContentParser, GetMappingsResponse, IOException> entityParser() {
        return this::fromXContent;
    }

    @Override
    protected GetMappingsResponse emptyResponse() {
        return new GetMappingsResponse();
    }

    @SuppressWarnings("unchecked")
    private GetMappingsResponse fromXContent(XContentParser parser) throws IOException {
        XContentParser.Token token = parser.nextToken();
        ensureExpectedToken(XContentParser.Token.START_OBJECT, token, parser::getTokenLocation);
        Map<String, Object> parts = parser.map();
        ImmutableOpenMap.Builder<String, ImmutableOpenMap<String, MappingMetaData>> builder = new ImmutableOpenMap.Builder<>();
        for (Map.Entry<String, Object> entry : parts.entrySet()) {
            String indexName = entry.getKey();
            Map<String, Object> mapping = (Map<String, Object>) ((Map) entry.getValue()).get(MAPPINGS.getPreferredName());
            ImmutableOpenMap.Builder<String, MappingMetaData> typeBuilder = new ImmutableOpenMap.Builder<>();
            for (Map.Entry<String, Object> typeEntry : mapping.entrySet()) {
                String typeName = typeEntry.getKey();
                Map<String, Object> fieldMappings = (Map<String, Object>) typeEntry.getValue();
                MappingMetaData mmd = new MappingMetaData(typeName, fieldMappings);
                typeBuilder.put(typeName, mmd);
            }
            builder.put(indexName, typeBuilder.build());
        }
        return new GetMappingsResponse(builder.build());
    }
}