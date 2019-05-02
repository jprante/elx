package org.xbib.elx.http;

import org.xbib.elx.api.ExtendedClientProvider;

public class ExtendedHttpClientProvider implements ExtendedClientProvider<ExtendedHttpClient> {
    @Override
    public ExtendedHttpClient getExtendedClient() {
        return new ExtendedHttpClient();
    }
}
