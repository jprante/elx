package org.xbib.elx.common.io;

import java.net.URLStreamHandler;
import java.net.URLStreamHandlerFactory;

public class ClasspathURLStreamHandlerFactory implements URLStreamHandlerFactory {

    @Override
    public URLStreamHandler createURLStreamHandler(String protocol) {
        return "classpath".equals(protocol) ? new ClasspathURLStreamHandler() : null;
    }
}
