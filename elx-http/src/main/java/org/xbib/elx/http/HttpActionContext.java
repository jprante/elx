package org.xbib.elx.http;

import io.netty.handler.codec.http.FullHttpResponse;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.xbib.netty.http.client.transport.Transport;

/**
 * HTTP action context.
 *
 * @param <R> request type
 * @param <T> response type
 */
public class HttpActionContext<R extends ActionRequest, T extends ActionResponse> {

    private final ExtendedHttpClient extendedHttpClient;

    private final R request;

    private final String url;

    private Transport httpClientTransport;

    private FullHttpResponse httpResponse;

    HttpActionContext(ExtendedHttpClient extendedHttpClient, R request, String url) {
        this.extendedHttpClient = extendedHttpClient;
        this.request = request;
        this.url = url;
    }

    public ExtendedHttpClient getExtendedHttpClient() {
        return extendedHttpClient;
    }

    public R getRequest() {
        return request;
    }

    public String getUrl() {
        return url;
    }

    public void setHttpClientTransport(Transport httpClientTransport) {
        this.httpClientTransport = httpClientTransport;
    }

    public Transport getHttpClientTransport() {
        return httpClientTransport;
    }

    public HttpActionContext<R, T> setHttpResponse(FullHttpResponse fullHttpResponse) {
        this.httpResponse = fullHttpResponse;
        return this;
    }

    public FullHttpResponse getHttpResponse() {
        return httpResponse;
    }
}
