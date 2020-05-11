package org.xbib.elx.api;

public interface ReadClientProvider<C extends ReadClient> {

    C getReadClient();
}
