package org.xbib.elx.http;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.xbib.net.http.client.HttpResponse;
import org.xbib.net.http.client.netty.Interaction;

/**
 * HTTP action context.
 *
 * @param <R> request type
 * @param <T> response type
 */
public class HttpActionContext<R extends ActionRequest, T extends ActionResponse> {

    private final HttpClientHelper extendedHttpClient;

    private final R request;

    private final String url;

    private Interaction httpClientTransport;

    private HttpResponse httpResponse;

    HttpActionContext(HttpClientHelper extendedHttpClient, R request, String url) {
        this.extendedHttpClient = extendedHttpClient;
        this.request = request;
        this.url = url;
    }

    public HttpClientHelper getExtendedHttpClient() {
        return extendedHttpClient;
    }

    public R getRequest() {
        return request;
    }

    public String getUrl() {
        return url;
    }

    public void setHttpClientTransport(Interaction httpClientTransport) {
        this.httpClientTransport = httpClientTransport;
    }

    public Interaction getHttpClientTransport() {
        return httpClientTransport;
    }

    public HttpActionContext<R, T> setHttpResponse(HttpResponse httpResponse) {
        this.httpResponse = httpResponse;
        return this;
    }

    public HttpResponse getHttpResponse() {
        return httpResponse;
    }
}
