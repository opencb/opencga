/*
 * Copyright 2015-2020 OpenCB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.opencb.opencga.catalog.io;

import org.opencb.opencga.catalog.exceptions.CatalogIOException;
import org.opencb.opencga.core.models.file.FileContent;

import java.io.DataInputStream;
import java.io.File;
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

    public abstract FileContent base64Image(Path file) throws CatalogIOException;

    public abstract FileContent head(Path file, long offset, int lines) throws CatalogIOException;

    public abstract FileContent tail(Path file, int lines) throws CatalogIOException;

    /**
     * Decompress zip or tar.gz file.
     * @param file     File to be decompressed.
     * @param destDir  Directory where the file will be decompressed.
     * @throws CatalogIOException if there is any issue decompressing the file.
     */
    public void decompress(Path file, Path destDir) throws CatalogIOException {
        if (!file.toFile().isFile()) {
            throw new CatalogIOException("File '" + file + "' is not a file");
        }
        if (!file.toString().endsWith(".zip") && !file.toString().endsWith(".tar.gz")) {
            throw new CatalogIOException("Unexpected extension. Can only decompress zip or tar.gz files");
        }
        if (!destDir.toFile().isDirectory()) {
            throw new CatalogIOException("Cannot find directory '" + destDir + "'");
        }
        if (!destDir.toFile().canWrite()) {
            throw new CatalogIOException("Cannot write in directory '" + destDir + "'");
        }
        if (file.toString().endsWith(".zip")) {
            unzip(file.toFile(), destDir.toFile());
        } else if (file.toString().endsWith(".tar.gz")) {
            decompressTarBall(file, destDir);
        } else {
            throw new IllegalStateException("Unexpected situation");
        }
    }

    protected abstract void unzip(File file, File destDir) throws CatalogIOException;

    protected abstract void decompressTarBall(Path file, Path destDir) throws CatalogIOException;

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
