package org.opencb.opencga.storage.core.io;

import org.opencb.opencga.storage.core.io.managers.LocalIOManager;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Created on 03/05/19.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class TestIOManager extends LocalIOManager {

    private URI toLocalUri(URI uri) {
        return Paths.get("target/test-data/", uri.getPath()).toUri();
    }

    @Override
    public boolean supports(URI uri) {
        return uri.getScheme().equals("test");
    }

    @Override
    public InputStream newInputStreamRaw(URI uri) throws IOException {
        return super.newInputStreamRaw(toLocalUri(uri));
    }

    @Override
    public OutputStream newOutputStreamRaw(URI uri) throws IOException {
        return super.newOutputStreamRaw(toLocalUri(uri));
    }

    @Override
    public boolean isDirectory(URI uri) throws IOException {
        return super.isDirectory(toLocalUri(uri));
    }

    @Override
    public boolean canWrite(URI uri) throws IOException {
        return super.canWrite(toLocalUri(uri));
    }

    @Override
    public void move(URI source, URI target) throws IOException {
        super.move(toLocalUri(source), target);
    }

    @Override
    public void createDirectory(URI directory) throws IOException {
        super.createDirectory(toLocalUri(directory));
    }

    @Override
    public boolean exists(URI uri) throws IOException {
        return super.exists(toLocalUri(uri));
    }

    @Override
    public void copyFromLocal(Path localSourceFile, URI targetFile) throws IOException {
        super.copyFromLocal(localSourceFile, toLocalUri(targetFile));
    }

    @Override
    public void copyToLocal(URI sourceFile, Path localTargetFile) throws IOException {
        super.copyToLocal(toLocalUri(sourceFile), localTargetFile);
    }

    @Override
    public boolean delete(URI uri) throws IOException {
        return super.delete(toLocalUri(uri));
    }

    @Override
    public long size(URI uri) throws IOException {
        return super.size(toLocalUri(uri));
    }

    @Override
    public String md5(URI uri) throws IOException {
        return super.md5(toLocalUri(uri));
    }

    @Override
    public int hashCode() {
        return TestIOManager.class.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof TestIOManager;
    }
}
