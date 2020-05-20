package org.xbib.elx.api;

@FunctionalInterface
public interface SeachClientProvider<C extends SearchClient> {

    C getClient();
}
