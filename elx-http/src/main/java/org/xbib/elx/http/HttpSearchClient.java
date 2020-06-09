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
import org.xbib.elx.common.AbstractSearchClient;
import java.io.IOException;

/**
 * Elasticsearch HTTP search client.
 */
public class HttpSearchClient extends AbstractSearchClient implements ElasticsearchClient {

    private final HttpClientHelper helper;

    public HttpSearchClient() {
        this.helper = new HttpClientHelper();
    }

    @Override
    public void init(Settings settings) throws IOException {
        super.init(settings);
        helper.init(settings);
    }

    @Override
    protected ElasticsearchClient createClient(Settings settings) {
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
    public <Request extends ActionRequest, Response extends ActionResponse, RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder>> RequestBuilder prepareExecute(Action<Request, Response, RequestBuilder> action) {
        return action.newRequestBuilder(this);
    }

    @Override
    public ThreadPool threadPool() {
        return helper.threadPool();
    }
}
