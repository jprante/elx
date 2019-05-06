package org.xbib.elx.http.action.admin.indices.alias;

import org.apache.logging.log4j.Level;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesAction;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesResponse;
import org.elasticsearch.common.CheckedFunction;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.xbib.elx.http.HttpAction;
import org.xbib.netty.http.client.RequestBuilder;

import java.io.IOException;

public class HttpIndicesAliasesAction extends HttpAction<IndicesAliasesRequest, IndicesAliasesResponse> {

    @Override
    public IndicesAliasesAction getActionInstance() {
        return IndicesAliasesAction.INSTANCE;
    }

    @Override
    protected RequestBuilder createHttpRequest(String url, IndicesAliasesRequest request) {
        try {
            XContentBuilder builder = JsonXContent.contentBuilder();
            request.toXContent(builder, ToXContent.EMPTY_PARAMS);
            String body =  Strings.toString(builder);
            logger.log(Level.DEBUG, "body = " + body);
            return newPostRequest(url, "/_aliases", body);
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
            return null;
        }
    }

    @Override
    protected CheckedFunction<XContentParser, IndicesAliasesResponse, IOException> entityParser() {
        return IndicesAliasesResponse::fromXContent;
    }

    @Override
    protected IndicesAliasesResponse emptyResponse() {
        return new IndicesAliasesResponse();
    }
}
