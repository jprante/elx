package org.xbib.elx.http;

import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.xbib.elx.common.AbstractBulkClient;
import java.io.IOException;

/**
 * Elasticsearch HTTP bulk client.
 */
public class HttpBulkClient extends AbstractBulkClient implements ElasticsearchClient {

    private final HttpClientHelper helper;

    public HttpBulkClient() {
        this.helper = new HttpClientHelper();
    }

    @Override
    public void init(Settings settings) throws IOException {
        super.init(settings);
        helper.init(settings);
    }

    @Override
    protected ElasticsearchClient createClient(Settings settings) throws IOException {
        return this;
    }

    @Override
    protected void closeClient(Settings settings) throws IOException {
        helper.closeClient(settings);
    }

    @Override
    public <Request extends ActionRequest, Response extends ActionResponse, RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder>> ActionFuture<Response> execute(Action<Request, Response, RequestBuilder> action, Request request) {
        return helper.execute(action, request);
    }

    @Override
    public <Request extends ActionRequest, Response extends ActionResponse, RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder>> void execute(Action<Request, Response, RequestBuilder> action, Request request, ActionListener<Response> listener) {
        helper.execute(action, request, listener);
    }

    @Override
    public <Request extends ActionRequest, Response extends ActionResponse,
            RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder>> RequestBuilder
    prepareExecute(Action<Request, Response, RequestBuilder> action) {
        return action.newRequestBuilder(this);
    }

    @Override
    public ThreadPool threadPool() {
        return helper.threadPool();
    }
}
