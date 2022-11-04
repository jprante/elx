package org.xbib.elx.http;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
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

    public HttpSearchClient(ClassLoader classLoader) {
        super();
        this.helper = new HttpClientHelper(classLoader);
    }

    @Override
    public boolean init(Settings settings, String info) throws IOException {
        if (super.init(settings, "Netty: " + io.netty.util.Version.identify())) {
            helper.init(settings);
            return true;
        }
        return false;
    }

    @Override
    protected ElasticsearchClient createClient(Settings settings) {
        return this;
    }

    @Override
    protected void closeClient(Settings settings) {
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
