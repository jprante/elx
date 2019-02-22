package org.xbib.elx.http;

import org.xbib.elx.api.ExtendedClientProvider;

import java.util.Collections;

public class ExtendedHttpClientProvider implements ExtendedClientProvider<ExtendedHttpClient> {
    @Override
    public ExtendedHttpClient getExtendedClient() {
        return new ExtendedHttpClient(Collections.emptyList(), Thread.currentThread().getContextClassLoader());
    }
}
