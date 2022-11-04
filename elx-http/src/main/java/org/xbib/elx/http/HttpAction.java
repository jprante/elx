package org.xbib.elx.http;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.common.CheckedFunction;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestStatus;
import org.xbib.net.URL;
import org.xbib.net.http.HttpHeaderNames;
import org.xbib.net.http.HttpMethod;
import org.xbib.net.http.client.netty.HttpRequest;
import org.xbib.net.http.client.netty.HttpRequestBuilder;
import org.xbib.net.http.client.HttpResponse;
import org.xbib.net.http.client.netty.Interaction;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * Base class for HTTP actions.
 *
 * @param <R> the request type
 * @param <T> the response type
 */
public abstract class HttpAction<R extends ActionRequest, T extends ActionResponse> {

    protected final Logger logger = LogManager.getLogger(getClass().getName());

    private static final String APPLICATION_JSON = "application/json";

    protected Settings settings;

    protected void setSettings(Settings settings) {
        this.settings = settings;
    }

    public abstract ActionType<T> getActionInstance();

    final void execute(HttpActionContext<R, T> httpActionContext, ActionListener<T> listener) throws IOException {
        try {
            ActionRequestValidationException validationException = httpActionContext.getRequest().validate();
            if (validationException != null) {
                listener.onFailure(validationException);
                return;
            }
            HttpRequestBuilder httpRequestBuilder =
                    createHttpRequest(httpActionContext.getUrl(), httpActionContext.getRequest());
            HttpRequest httpRequest = httpRequestBuilder.build();
            httpRequest.setResponseListener(fullHttpResponse -> {
                if (logger.isTraceEnabled()) {
                    logger.log(Level.TRACE, "got response: " + fullHttpResponse.getStatus().codeAsText() +
                            " headers = " + fullHttpResponse.getHeaders() +
                            " content = " + fullHttpResponse.getBodyAsChars(StandardCharsets.UTF_8));
                }
                httpActionContext.setHttpResponse(fullHttpResponse);
                if (fullHttpResponse.getStatus().code() == 200) {
                    listener.onResponse(parseToResponse(httpActionContext));
                } else {
                    ElasticsearchStatusException statusException = parseToError(httpActionContext);
                    if (statusException.status().equals(RestStatus.NOT_FOUND)) {
                        listener.onResponse(parseToResponse(httpActionContext));
                    } else {
                        listener.onFailure(statusException);
                    }
                }
            });
            if (logger.isTraceEnabled()) {
                logger.log(Level.TRACE, "executing HTTP request " + httpRequest);
            }
            Interaction transport = httpActionContext.getExtendedHttpClient().internalClient().execute(httpRequest);
            httpActionContext.setHttpClientTransport(transport);
            if (transport.isFailed()) {
                listener.onFailure(new Exception(transport.getFailure()));
            }
        } catch (Throwable e) {
            listener.onFailure(new RuntimeException(e));
            throw new IOException(e);
        }
    }

    protected HttpRequestBuilder newGetRequest(String url, String path) {
        return newRequest(HttpMethod.GET, url, path);
    }

    protected HttpRequestBuilder newGetRequest(String url, String path, BytesReference content) {
        return newRequest(HttpMethod.GET, url, path, content);
    }

    protected HttpRequestBuilder newHeadRequest(String url, String path) {
        return newRequest(HttpMethod.HEAD, url, path);
    }

    protected HttpRequestBuilder newPostRequest(String url, String path) {
        return newRequest(HttpMethod.POST, url, path);
    }

    protected HttpRequestBuilder newPostRequest(String url, String path, BytesReference content) {
        return newRequest(HttpMethod.POST, url, path, content);
    }

    protected HttpRequestBuilder newPostRequest(String url, String path, String content) {
        return newRequest(HttpMethod.POST, url, path, content);
    }

    protected HttpRequestBuilder newPutRequest(String url, String path) {
        return newRequest(HttpMethod.PUT, url, path);
    }

    protected HttpRequestBuilder newPutRequest(String url, String path, String content) {
        return newRequest(HttpMethod.PUT, url, path, content);
    }

    protected HttpRequestBuilder newPutRequest(String url, String path, BytesReference content) {
        return newRequest(HttpMethod.PUT, url, path, content);
    }

    protected HttpRequestBuilder newDeleteRequest(String url, String path) {
        return newRequest(HttpMethod.DELETE, url, path);
    }

    protected HttpRequestBuilder newDeleteRequest(String url, String path, BytesReference content) {
        return newRequest(HttpMethod.DELETE, url, path, content);
    }

    protected HttpRequestBuilder newRequest(HttpMethod method, String baseUrl, String path) {
        return HttpRequest.builder(method).setURL(URL.from(baseUrl).resolve(path));
    }

    protected HttpRequestBuilder newRequest(HttpMethod method, String baseUrl, String path, BytesReference content) {
        return HttpRequest.builder(method).setURL(URL.from(baseUrl).resolve(path)).content(content.toBytesRef().bytes, APPLICATION_JSON);
    }

    protected HttpRequestBuilder newRequest(HttpMethod method, String baseUrl, String path, String content) {
        return HttpRequest.builder(method).setURL(URL.from(baseUrl).resolve(path)).content(content, APPLICATION_JSON);
    }

    protected T parseToResponse(HttpActionContext<R, T> httpActionContext) {
        String mediaType = httpActionContext.getHttpResponse().getHeaders().get(HttpHeaderNames.CONTENT_TYPE);
        XContentType xContentType = XContentType.fromMediaTypeOrFormat(mediaType);
        if (xContentType == null) {
            throw new IllegalStateException("unsupported content-type: " + mediaType);
        }
        try (XContentParser parser = xContentType.xContent()
                .createParser(httpActionContext.getExtendedHttpClient().getRegistry(),
                DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                httpActionContext.getHttpResponse().getBodyAsChars(StandardCharsets.UTF_8).toString())) {
            return entityParser(httpActionContext.getHttpResponse()).apply(parser);
        } catch (Throwable e) {
            // catch all kinds of errors in the entity parsing process
            logger.error(e.getMessage(), e);
            logger.error("status = " + httpActionContext.getHttpResponse().getStatus().code());
            logger.error("body = " + httpActionContext.getHttpResponse().getBodyAsChars(StandardCharsets.UTF_8));
            return null;
        }
    }

    protected ElasticsearchStatusException parseToError(HttpActionContext<R, T> httpActionContext) {
        // we assume a non-empty, valid JSON response body. If there is none, this method must be overriden.
        try (XContentParser parser = XContentFactory.xContent(XContentType.JSON)
                .createParser(httpActionContext.getExtendedHttpClient().getRegistry(),
                        DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                        httpActionContext.getHttpResponse().getBodyAsChars(StandardCharsets.UTF_8).toString())) {
            return errorParser().apply(parser);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            return new ElasticsearchStatusException(e.getMessage(), RestStatus.INTERNAL_SERVER_ERROR, e);
        }
    }

    protected CheckedFunction<XContentParser, ElasticsearchStatusException, IOException> errorParser() {
        return BytesRestResponse::errorFromXContent;
    }

    protected abstract HttpRequestBuilder createHttpRequest(String baseUrl, R request) throws IOException;

    protected abstract CheckedFunction<XContentParser, T, IOException> entityParser(HttpResponse httpResponse);
}
