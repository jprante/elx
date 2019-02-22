package org.xbib.elx.http;

import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpMethod;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.GenericAction;
import org.elasticsearch.common.CheckedFunction;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.xbib.netty.http.client.Request;
import org.xbib.netty.http.client.RequestBuilder;
import org.xbib.netty.http.client.transport.Transport;

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

    protected static final String APPLICATION_JSON = "application/json";

    protected Settings settings;

    protected void setSettings(Settings settings) {
        this.settings = settings;
    }

    public abstract GenericAction<R, T> getActionInstance();

    /*public final ActionFuture<T> execute(HttpActionContext<R, T> httpActionContext) {
        PlainActionFuture<T> future = PlainActionFuture.newFuture();
        //HttpActionFuture<T, T> future = new HttpActionFuture<>();
        execute(httpActionContext, future);
        return future;
    }*/

    public final void execute(HttpActionContext<R, T> httpActionContext, ActionListener<T> listener) throws IOException {
        try {
            ActionRequestValidationException validationException = httpActionContext.getRequest().validate();
            if (validationException != null) {
                listener.onFailure(validationException);
                return;
            }
            RequestBuilder httpRequestBuilder =
                    createHttpRequest(httpActionContext.getUrl(), httpActionContext.getRequest());
            //httpRequestBuilder.addHeader("content-type", "application/json");
            Request httpRequest = httpRequestBuilder.build();
//            logger.info("action = {} request = {}", this.getClass().getName(), httpRequest.toString());
            httpRequest.setResponseListener(fullHttpResponse -> {
                logger.info("returned response " + fullHttpResponse.status().code() +
                        " headers = " + fullHttpResponse.headers().entries() +
                        " content = " + fullHttpResponse.content().toString(StandardCharsets.UTF_8));
                listener.onResponse(parseToResponse(httpActionContext.setHttpResponse(fullHttpResponse)));
            });
            Transport transport = httpActionContext.getExtendedHttpClient().internalClient().execute(httpRequest);
            logger.info("transport = " + transport);
            httpActionContext.setHttpClientTransport(transport);
            if (transport.isFailed()) {
                listener.onFailure(new Exception(transport.getFailure()));
            }
            logger.info("done, listener is " + listener);
        } catch (Throwable e) {
            listener.onFailure(new RuntimeException(e));
            throw new IOException(e);
        }
    }

    protected RequestBuilder newGetRequest(String url, String path) {
        return Request.builder(HttpMethod.GET).url(url).uri(path);
    }

    protected RequestBuilder newGetRequest(String url, String path, BytesReference content) {
        return newRequest(HttpMethod.GET, url, path, content);
    }

    protected RequestBuilder newHeadRequest(String url, String path) {
        return newRequest(HttpMethod.HEAD, url, path);
    }

    protected RequestBuilder newPostRequest(String url, String path) {
        return newRequest(HttpMethod.POST, url, path);
    }

    protected RequestBuilder newPostRequest(String url, String path, BytesReference content) {
        return newRequest(HttpMethod.POST, url, path, content);
    }

    protected RequestBuilder newPostRequest(String url, String path, String content) {
        return newRequest(HttpMethod.POST, url, path, content);
    }

    protected RequestBuilder newPutRequest(String url, String path) {
        return newRequest(HttpMethod.PUT, url, path);
    }

    protected RequestBuilder newPutRequest(String url, String path, String content) {
        return newRequest(HttpMethod.PUT, url, path, content);
    }

    protected RequestBuilder newPutRequest(String url, String path, BytesReference content) {
        return newRequest(HttpMethod.PUT, url, path, content);
    }

    protected RequestBuilder newDeleteRequest(String url, String path, BytesReference content) {
        return newRequest(HttpMethod.DELETE, url, path, content);
    }

    protected RequestBuilder newRequest(HttpMethod method, String baseUrl, String path) {
        return Request.builder(method).url(baseUrl).uri(path);
    }

    protected RequestBuilder newRequest(HttpMethod method, String baseUrl, String path, BytesReference content) {
        return Request.builder(method).url(baseUrl).uri(path).content(content.toBytesRef().bytes, APPLICATION_JSON);
    }

    protected RequestBuilder newRequest(HttpMethod method, String baseUrl, String path, String content) {
        return Request.builder(method).url(baseUrl).uri(path).content(content, APPLICATION_JSON);
    }

    protected RequestBuilder newRequest(HttpMethod method, String baseUrl, String path, ByteBuf byteBuf) {
        return Request.builder(method).url(baseUrl).uri(path).content(byteBuf, APPLICATION_JSON);
    }

    protected T parseToResponse(HttpActionContext<R, T> httpActionContext) {
        String mediaType = httpActionContext.getHttpResponse().headers().get(HttpHeaderNames.CONTENT_TYPE);
        XContentType xContentType = XContentType.fromMediaTypeOrFormat(mediaType);
        if (xContentType == null) {
            throw new IllegalStateException("unsupported content-type: " + mediaType);
        }
        try (XContentParser parser = xContentType.xContent()
                .createParser(httpActionContext.getExtendedHttpClient().getRegistry(),
                DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                httpActionContext.getHttpResponse().content().array())) {
            return entityParser().apply(parser);
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
            return null;
        }
    }

    protected abstract RequestBuilder createHttpRequest(String baseUrl, R request) throws IOException;

    protected abstract CheckedFunction<XContentParser, T, IOException> entityParser();

}
