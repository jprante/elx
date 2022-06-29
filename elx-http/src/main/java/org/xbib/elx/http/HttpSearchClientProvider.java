package org.xbib.elx.http;

import org.xbib.elx.api.SearchClientProvider;

public class HttpSearchClientProvider implements SearchClientProvider<HttpSearchClient> {

    @Override
    public HttpSearchClient getClient(ClassLoader classLoader) {
        return new HttpSearchClient(classLoader);
    }
}
