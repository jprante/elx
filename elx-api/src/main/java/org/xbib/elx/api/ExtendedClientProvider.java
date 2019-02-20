package org.xbib.elx.api;

@FunctionalInterface
public interface ExtendedClientProvider<C extends ExtendedClient> {

    C getExtendedClient();
}
