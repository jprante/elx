package org.xbib.elx.api;

@FunctionalInterface
public interface AdminClientProvider<C extends AdminClient> {

    C getClient();
}
