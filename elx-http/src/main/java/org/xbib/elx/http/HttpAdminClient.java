package org.xbib.elx.http;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.xbib.elx.common.AbstractAdminClient;
import java.io.IOException;

/**
 * Elasticsearch HTTP admin client.
 */
public class HttpAdminClient extends AbstractAdminClient implements ElasticsearchClient {

    private final HttpClientHelper helper;

    public HttpAdminClient() {
        super();
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
    public <Request extends ActionRequest, Response extends ActionResponse> ActionFuture<Response> execute(ActionType<Response> action, Request request) {
        return helper.execute(action, request);
    }

    @Override
    public <Request extends ActionRequest, Response extends ActionResponse> void execute(ActionType<Response> action, Request request, ActionListener<Response> listener) {
        helper.execute(action, request, listener);
    }

    @Override
    public ThreadPool threadPool() {
        return helper.threadPool();
    }
}
