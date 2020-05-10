package org.xbib.elx.http.action.admin.indices.create;

import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.create.CreateIndexAction;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.common.CheckedFunction;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.xbib.elx.http.HttpAction;
import org.xbib.netty.http.client.api.Request;
import org.xbib.netty.http.common.HttpResponse;

import java.io.IOException;
import java.util.Map;

public class HttpCreateIndexAction extends HttpAction<CreateIndexRequest, CreateIndexResponse> {

    public static final ParseField MAPPINGS = new ParseField("mappings");
    public static final ParseField SETTINGS = new ParseField("settings");
    public static final ParseField ALIASES = new ParseField("aliases");

    @Override
    public CreateIndexAction getActionInstance() {
        return CreateIndexAction.INSTANCE;
    }

    @Override
    protected Request.Builder createHttpRequest(String url, CreateIndexRequest createIndexRequest) throws IOException {
        XContentBuilder builder = toXContent(createIndexRequest, XContentFactory.jsonBuilder());
        return newPutRequest(url, "/" + createIndexRequest.index() + "?include_type_name=true",
                BytesReference.bytes(builder));
    }

    @Override
    protected CheckedFunction<XContentParser, CreateIndexResponse, IOException> entityParser(HttpResponse httpResponse) {
        return CreateIndexResponse::fromXContent;
    }

    // fixed version from CreateIndexRequest - use only one mapping
    private XContentBuilder toXContent(CreateIndexRequest createIndexRequest,
                                       XContentBuilder builder) throws IOException {
        builder.startObject();
        builder.startObject(SETTINGS.getPreferredName());
        createIndexRequest.settings().toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();
        // there is only an empty or a single entry for mappings
        if (createIndexRequest.mappings().isEmpty()) {
            // ES wants a mappings element with an empty map
            builder.startObject(MAPPINGS.getPreferredName());
            builder.endObject();
        } else {
            Map<String, ?> mappingAsMap = createIndexRequest.mappings();
            String mappingString = mappingAsMap.values().iterator().next().toString();
            builder.field(MAPPINGS.getPreferredName());
            builder.map(XContentHelper.convertToMap(new BytesArray(mappingString), false, XContentType.JSON).v2());
        }
        builder.startObject(ALIASES.getPreferredName());
        for (Alias alias : createIndexRequest.aliases()) {
            alias.toXContent(builder, ToXContent.EMPTY_PARAMS);
        }
        builder.endObject();
        builder.endObject();
        return builder;
    }
}
