package org.xbib.elx.http;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.BaseFuture;
import org.elasticsearch.common.util.concurrent.UncategorizedExecutionException;
import org.xbib.netty.http.client.api.Transport;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class HttpActionFuture<T, L> extends BaseFuture<T> implements ActionFuture<T>, ActionListener<L> {

    private Transport httpClientTransport;
    
    HttpActionFuture<T, L> setHttpClientTransport(Transport httpClientTransport) {
        this.httpClientTransport = httpClientTransport;
        return this;
    }
    
    @Override
    public T actionGet() {
        try {
            httpClientTransport.get();
            return get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("future got interrupted", e);
        } catch (ExecutionException e) {
            throw rethrowExecutionException(e);
        }
    }

    @Override
    public T actionGet(String timeout) {
        return actionGet(TimeValue.parseTimeValue(timeout, null, getClass().getSimpleName() + ".actionGet.timeout"));
    }

    @Override
    public T actionGet(long timeoutMillis) {
        return actionGet(timeoutMillis, TimeUnit.MILLISECONDS);
    }

    @Override
    public T actionGet(TimeValue timeout) {
        return actionGet(timeout.millis(), TimeUnit.MILLISECONDS);
    }

    @Override
    public T actionGet(long timeout, TimeUnit unit) {
        try {
            return get(timeout, unit);
        } catch (TimeoutException e) {
            throw new ElasticsearchTimeoutException(e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Future got interrupted", e);
        } catch (ExecutionException e) {
            throw rethrowExecutionException(e);
        }
    }

    private static RuntimeException rethrowExecutionException(ExecutionException e) {
        if (e.getCause() instanceof ElasticsearchException) {
            ElasticsearchException esEx = (ElasticsearchException) e.getCause();
            Throwable root = esEx.unwrapCause();
            if (root instanceof ElasticsearchException) {
                return (ElasticsearchException) root;
            } else if (root instanceof RuntimeException) {
                return (RuntimeException) root;
            }
            return new UncategorizedExecutionException("Failed execution", root);
        } else if (e.getCause() instanceof RuntimeException) {
            return (RuntimeException) e.getCause();
        } else {
            return new UncategorizedExecutionException("Failed execution", e);
        }
    }

    @Override
    public void onResponse(L result) {
        set(convert(result));
    }

    @Override
    public void onFailure(Exception e) {
        setException(e);
    }

    @SuppressWarnings("unchecked")
    private T convert(L listenerResponse) {
        return (T) listenerResponse;
    }
}
