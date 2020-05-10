package org.xbib.elx.http.action.bulk;

import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.BulkAction;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.common.CheckedFunction;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.xbib.elx.http.HttpAction;
import org.xbib.netty.http.client.api.Request;
import org.xbib.netty.http.common.HttpResponse;

import java.io.IOException;

public class HttpBulkAction extends HttpAction<BulkRequest, BulkResponse> {

    @Override
    public BulkAction getActionInstance() {
        return BulkAction.INSTANCE;
    }

    @Override
    protected Request.Builder createHttpRequest(String url, BulkRequest request) throws IOException {
        StringBuilder bulkContent = new StringBuilder();
        for (DocWriteRequest<?> actionRequest : request.requests()) {
            if (actionRequest instanceof IndexRequest) {
                IndexRequest indexRequest = (IndexRequest) actionRequest;
                bulkContent.append("{\"").append(indexRequest.opType().getLowercase()).append("\":{");
                bulkContent.append("\"_index\":\"").append(indexRequest.index()).append("\"");
                bulkContent.append(",\"_type\":\"").append("_doc").append("\"");
                if (indexRequest.id() != null) {
                    bulkContent.append(",\"_id\":\"").append(indexRequest.id()).append("\"");
                }
                if (indexRequest.routing() != null) {
                    bulkContent.append(",\"_routing\":\"").append(indexRequest.routing()).append("\"");
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
            } else if (actionRequest instanceof UpdateRequest) {
                UpdateRequest updateRequest = (UpdateRequest) actionRequest;
                bulkContent.append("{\"update\":{");
                bulkContent.append("\"_index\":\"").append(updateRequest.index()).append("\"");
                bulkContent.append(",\"_type\":\"").append("_doc").append("\"");
                bulkContent.append(",\"_id\":\"").append(updateRequest.id()).append("\"");
                if (updateRequest.routing() != null) {
                    bulkContent.append(",\"_routing\":\"").append(updateRequest.routing()).append("\"");
                }
                if (updateRequest.version() > 0) {
                    bulkContent.append(",\"_version\":\"").append(updateRequest.version()).append("\"");
                    if (updateRequest.versionType() != null) {
                        bulkContent.append(",\"_version_type\":\"").append(updateRequest.versionType().name()).append("\"");
                    }
                }
                bulkContent.append("}}\n");
                // only 'doc' supported
                if (updateRequest.doc() != null) {
                    bulkContent.append("{\"doc\":");
                    bulkContent.append(XContentHelper.convertToJson(updateRequest.doc().source(), false, XContentType.JSON));
                    bulkContent.append("}\n");
                }
            } else if (actionRequest instanceof DeleteRequest) {
                DeleteRequest deleteRequest = (DeleteRequest) actionRequest;
                bulkContent.append("{\"delete\":{");
                bulkContent.append("\"_index\":\"").append(deleteRequest.index()).append("\"");
                bulkContent.append(",\"_type\":\"").append("_doc").append("\"");
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
    protected CheckedFunction<XContentParser, BulkResponse, IOException> entityParser(HttpResponse httpResponse) {
        return BulkResponse::fromXContent;
    }
}
