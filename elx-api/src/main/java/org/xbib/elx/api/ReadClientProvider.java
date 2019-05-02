package org.xbib.elx.api;

@FunctionalInterface
public interface ReadClientProvider<C extends ReadClient> {

    C getReadClient();
}
