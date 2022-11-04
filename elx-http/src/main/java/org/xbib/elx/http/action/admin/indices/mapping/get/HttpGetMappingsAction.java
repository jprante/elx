package org.xbib.elx.http.action.admin.indices.mapping.get;

import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsAction;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsRequest;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.common.CheckedFunction;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.xcontent.XContentParser;
import org.xbib.elx.http.HttpAction;
import org.xbib.net.http.client.HttpResponse;
import org.xbib.net.http.client.netty.HttpRequestBuilder;

import java.io.IOException;
import java.util.Map;

public class HttpGetMappingsAction extends HttpAction<GetMappingsRequest, GetMappingsResponse> {

    private static final ParseField MAPPINGS = new ParseField("mappings");

    @Override
    public GetMappingsAction getActionInstance() {
        return GetMappingsAction.INSTANCE;
    }

    @Override
    protected HttpRequestBuilder createHttpRequest(String url, GetMappingsRequest request) {
        String index = request.indices() != null ? "/" + String.join(",", request.indices()) : "";
        return newGetRequest(url, index + "/_mapping");
    }

    @Override
    protected CheckedFunction<XContentParser, GetMappingsResponse, IOException> entityParser(HttpResponse httpResponse) {
        return this::fromXContent;
    }

    // fixed version from GetMappingsRequest - use only one mapping per index with type "_doc"
    @SuppressWarnings("unchecked")
    private GetMappingsResponse fromXContent(XContentParser parser) throws IOException {
        if (parser.currentToken() == null) {
            parser.nextToken();
        }
        Map<String, Object> map = parser.map();
        ImmutableOpenMap.Builder<String, ImmutableOpenMap<String, MappingMetadata>> builder = new ImmutableOpenMap.Builder<>();
        for (Map.Entry<String, Object> entry : map.entrySet()) {
            final String indexName = entry.getKey();
            final Map<String, Object> mapping = (Map<String, Object>) ((Map<String, Object>) entry.getValue()).get(MAPPINGS.getPreferredName());
            ImmutableOpenMap.Builder<String, MappingMetadata> typeBuilder = new ImmutableOpenMap.Builder<>();
            MappingMetadata mmd = new MappingMetadata("_doc", mapping);
            typeBuilder.put("_doc", mmd);
            builder.put(indexName, typeBuilder.build());
        }
        return new GetMappingsResponse(builder.build());
    }
}
