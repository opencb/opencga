package org.opencb.opencga.catalog.io;

import org.opencb.opencga.catalog.exceptions.CatalogIOException;
import org.opencb.opencga.core.models.file.FileContent;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.CopyOption;
import java.nio.file.FileVisitor;
import java.nio.file.Path;
import java.util.Date;
import java.util.List;
import java.util.stream.Stream;

public abstract class IOManager {

    protected abstract void checkUriExists(URI uri) throws CatalogIOException;

    protected abstract void checkUriScheme(URI uri) throws CatalogIOException;

    public abstract void checkDirectoryUri(URI uri, boolean writable) throws CatalogIOException;

    public abstract void checkWritableUri(URI uri) throws CatalogIOException;

    public abstract boolean exists(URI uri);

    public abstract URI createDirectory(URI uri, boolean parents) throws CatalogIOException;

    public URI createDirectory(URI uri) throws CatalogIOException {
        return createDirectory(uri, false);
    }

    public abstract void deleteDirectory(URI uri) throws CatalogIOException;

    public abstract void deleteFile(URI fileUri) throws CatalogIOException;

    public abstract void rename(URI oldName, URI newName) throws CatalogIOException;

    public abstract boolean isDirectory(URI uri);

    public abstract long copy(InputStream in, URI target, CopyOption... options) throws CatalogIOException;

    public abstract void copy(URI source, URI target, CopyOption... options) throws IOException, CatalogIOException;

    public abstract void move(URI source, URI target, CopyOption... options) throws CatalogIOException;

    public abstract void walkFileTree(URI start, FileVisitor<? super URI> visitor) throws CatalogIOException;

    public abstract DataInputStream getFileObject(URI fileUri, int start, int limit) throws CatalogIOException;

    public abstract FileContent tail(Path file, int lines) throws CatalogIOException;

    public abstract FileContent head(Path file, int lines) throws CatalogIOException;

    public abstract FileContent content(Path file, long offset, int numLines) throws CatalogIOException;

    /**
     * Grep the content of a file.
     *
     * @param file File.
     * @param pattern String pattern.
     * @param lines Maximum number of lines to be returned. 0 means all the lines.
     * @param ignoreCase Case insensitive search.
     * @return the FileContent.
     * @throws CatalogIOException If the file is not a file or cannot be found.
     */
    public abstract FileContent grep(Path file, String pattern, int lines, boolean ignoreCase) throws CatalogIOException;

    public abstract String calculateChecksum(URI file) throws CatalogIOException;

    public abstract List<URI> listFiles(URI directory) throws CatalogIOException;

    public Stream<URI> listFilesStream(URI directory) throws CatalogIOException {
        return listFiles(directory).stream();
    }

    public abstract long getFileSize(URI file) throws CatalogIOException;

    public abstract Date getCreationDate(URI file) throws CatalogIOException;

    public abstract Date getModificationDate(URI file) throws CatalogIOException;

}
