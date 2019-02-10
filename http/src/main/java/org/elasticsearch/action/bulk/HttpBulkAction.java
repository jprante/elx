package org.elasticsearch.action.bulk;

import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.CheckedFunction;
import org.elasticsearch.common.xcontent.XContentParser;
import org.xbib.elasticsearch.client.http.HttpAction;
import org.xbib.netty.http.client.RequestBuilder;

import java.io.IOException;

/**
 *
 */
public class HttpBulkAction extends HttpAction<BulkRequest, BulkResponse> {

    @Override
    public BulkAction getActionInstance() {
        return BulkAction.INSTANCE;
    }

    @Override
    protected RequestBuilder createHttpRequest(String url, BulkRequest request) {
        StringBuilder bulkContent = new StringBuilder();
        for (DocWriteRequest<?> actionRequest : request.requests()) {
            if (actionRequest instanceof IndexRequest) {
                IndexRequest indexRequest = (IndexRequest) actionRequest;
                bulkContent.append("{\"").append(indexRequest.opType()).append("\":{");
                bulkContent.append("\"_index\":\"").append(indexRequest.index()).append("\"");
                bulkContent.append(",\"_type\":\"").append(indexRequest.type()).append("\"");
                if (indexRequest.id() != null) {
                    bulkContent.append(",\"_id\":\"").append(indexRequest.id()).append("\"");
                }
                if (indexRequest.routing() != null) {
                    bulkContent.append(",\"_routing\":\"").append(indexRequest.routing()).append("\"");
                }
                if (indexRequest.parent() != null) {
                    bulkContent.append(",\"_parent\":\"").append(indexRequest.parent()).append("\"");
                }
                if (indexRequest.version() > 0) {
                    bulkContent.append(",\"_version\":\"").append(indexRequest.version()).append("\"");
                    if (indexRequest.versionType() != null) {
                        bulkContent.append(",\"_version_type\":\"").append(indexRequest.versionType().name()).append("\"");
                    }
                }
                bulkContent.append("}}\n");
                bulkContent.append(indexRequest.source().utf8ToString());
                bulkContent.append("\n");
            } else if (actionRequest instanceof DeleteRequest) {
                DeleteRequest deleteRequest = (DeleteRequest) actionRequest;
                bulkContent.append("{\"delete\":{");
                bulkContent.append("\"_index\":\"").append(deleteRequest.index()).append("\"");
                bulkContent.append(",\"_type\":\"").append(deleteRequest.type()).append("\"");
                bulkContent.append(",\"_id\":\"").append(deleteRequest.id()).append("\"");
                if (deleteRequest.routing() != null) {
                    bulkContent.append(",\"_routing\":\"").append(deleteRequest.routing()).append("\""); // _routing
                }
                bulkContent.append("}}\n");
            }
        }
        return newPostRequest(url, "/_bulk", bulkContent.toString());
    }

    @Override
    protected CheckedFunction<XContentParser, BulkResponse, IOException> entityParser() {
        return BulkResponse::fromXContent;
    }
}
