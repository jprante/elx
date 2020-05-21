package org.xbib.elx.api;

@FunctionalInterface
public interface SearchClientProvider<C extends SearchClient> {

    C getClient();
}
