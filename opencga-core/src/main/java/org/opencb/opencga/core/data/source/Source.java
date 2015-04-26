package org.opencb.opencga.core.data.source;

import java.io.InputStream;

public interface Source {
    public InputStream getInputStream(String path);
}
