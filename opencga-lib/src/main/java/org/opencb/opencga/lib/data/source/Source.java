package org.opencb.opencga.lib.data.source;

import java.io.InputStream;

public interface Source {
    public InputStream getInputStream(String path);
}
