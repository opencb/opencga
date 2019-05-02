package org.opencb.opencga.storage.core.io.managers;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.nio.file.Path;

/**
 * Created on 01/05/19.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public interface IOManager {

    boolean supports(URI uri);

    InputStream newInputStream(URI uri) throws IOException;

    OutputStream newOutputStream(URI uri) throws IOException;

    boolean exists(URI uri) throws IOException;

    void copyFromLocal(Path localSourceFile, URI targetFile) throws IOException;

    void copyToLocal(URI sourceFile, Path localTargetFile) throws IOException;

    boolean delete(URI uri) throws IOException;

    long size(URI uri) throws IOException;

    String md5(URI uri) throws IOException;

}
