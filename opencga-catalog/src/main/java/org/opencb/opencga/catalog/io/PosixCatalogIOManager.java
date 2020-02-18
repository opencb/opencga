/*
 * Copyright 2015-2017 OpenCB
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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import org.opencb.commons.exec.Command;
import org.opencb.opencga.catalog.exceptions.CatalogIOException;
import org.opencb.opencga.core.common.IOUtils;
import org.opencb.opencga.core.common.UriUtils;
import org.opencb.opencga.core.config.Configuration;
import org.opencb.opencga.core.models.file.FileContent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Stream;

public class PosixCatalogIOManager extends CatalogIOManager {

    protected static Logger logger = LoggerFactory.getLogger(PosixCatalogIOManager.class);
    protected static ObjectMapper jsonObjectMapper;
    protected static ObjectWriter jsonObjectWriter;

    private static final int MAXIMUM_BYTES = 1024 * 1024;

    public PosixCatalogIOManager(Configuration configuration) throws CatalogIOException {
        super(configuration);
    }

    @Override
    protected void setConfiguration(Configuration configuration) throws CatalogIOException {
        try {
            rootDir = UriUtils.createDirectoryUri(configuration.getWorkspace());
        } catch (URISyntaxException e) {
            throw new CatalogIOException("Malformed URI 'OPENCGA.CATALOG.MAIN.ROOTDIR'", e);
        }
        if (!rootDir.getScheme().equals("file")) {
            throw new CatalogIOException("wrong posix file system in catalog.properties: " + rootDir);
        }
    }

    /*
     * FS Utils
     *
     */

    @Override
    protected void checkUriExists(URI uri) throws CatalogIOException {
        if (uri == null || !Files.exists(Paths.get(uri))) {
            throw new CatalogIOException("Path '" + String.valueOf(uri) + "' is null or it does not exist");
        }
    }

    @Override
    protected void checkUriScheme(URI uri) throws CatalogIOException {
        if (uri == null) {    //If scheme is missing, use "file" as scheme
            throw new CatalogIOException("URI is null");
        } else if (uri.getScheme() != null && !uri.getScheme().equals("file")) {    //If scheme is missing, use "file" as scheme
            throw new CatalogIOException("Unknown URI.scheme for URI '" + String.valueOf(uri) + "'");
        }
    }

    @Override
    public void checkDirectoryUri(URI uri, boolean writable) throws CatalogIOException {
        if (uri == null) {
            throw new CatalogIOException("URI is null");
        } else {
            Path path = Paths.get(uri.getPath());
            if (!Files.exists(path)) {
                throw new CatalogIOException("Path '" + uri.toString() + "' does not exist");
            }
            if (!Files.isDirectory(path)) {
                throw new CatalogIOException("Path '" + uri.toString() + "' is not a directory");
            }
            if (writable && !Files.isWritable(path)) {
                throw new CatalogIOException("Path '" + uri.toString() + "' is not writable");
            }
        }
    }

    @Override
    public void checkWritableUri(URI uri) throws CatalogIOException {
        if (uri == null) {
            throw new CatalogIOException("URI is null");
        } else {
            Path path = Paths.get(uri.getPath());
            if (!Files.exists(path)) {
                throw new CatalogIOException("Path '" + uri.toString() + "' does not exist");
            }
            if (!Files.isWritable(path)) {
                throw new CatalogIOException("Path '" + uri.toString() + "' is not writable");
            }
        }
    }

    private void checkDirectoryPath(Path path, boolean writable) throws CatalogIOException {
        if (path == null || !Files.exists(path) || !Files.isDirectory(path)) {
            throw new CatalogIOException("Path '" + String.valueOf(path) + "' is null, it does not exist or it's not a directory");
        }

        if (writable && !Files.isWritable(path)) {
            throw new CatalogIOException("Path '" + path.toString() + "' is not writable");
        }
    }

    @Override
    public boolean exists(URI uri) {
        return Files.exists(Paths.get(uri));
    }

    @Override
    public URI createDirectory(URI uri, boolean parents) throws CatalogIOException {
        try {
            if (parents) {
                return Files.createDirectories(Paths.get(uri)).toUri();
            } else {
                return Files.createDirectory(Paths.get(uri)).toUri();
            }
        } catch (IOException e) {
            throw new CatalogIOException("Error creating directory " + uri + " with parents=" + parents, e);
        }
    }

    @Override
    public void deleteDirectory(URI uri) throws CatalogIOException {
        try {
            IOUtils.deleteDirectory(Paths.get(uri));
        } catch (IOException e) {
            throw new CatalogIOException("Could not delete directory " + uri, e);
        }
    }

    @Override
    public void deleteFile(URI fileUri) throws CatalogIOException {
        try {
            Files.delete(Paths.get(fileUri));
        } catch (IOException e) {
            throw new CatalogIOException("Could not delete file " + fileUri, e);
        }
    }

    @Override
    public void rename(URI oldName, URI newName) throws CatalogIOException {
        String parent;
        if (isDirectory(oldName)) { // if oldName is a file
            parent = "..";
        } else {
            parent = ".";
        }
        checkUriExists(oldName);
        checkDirectoryUri(oldName.resolve(parent), true);

        try {
            if (!Files.exists(Paths.get(newName))) {
                Files.move(Paths.get(oldName), Paths.get(newName));
            } else {
                throw new CatalogIOException("Unable to rename. File \"" + newName + "\" already exists");
            }
        } catch (IOException e) {
            throw new CatalogIOException("Unable to rename file", e);
        }
    }

    @Override
    public boolean isDirectory(URI uri) {
        return Paths.get(uri).toFile().isDirectory();
    }

    @Override
    public void copyFile(URI source, URI target) throws IOException, CatalogIOException {
        checkUriExists(source);
        if ("file".equals(source.getScheme()) && "file".equals(target.getScheme())) {
            Files.copy(Paths.get(source), Paths.get(target), StandardCopyOption.REPLACE_EXISTING);
        } else {
            throw new CatalogIOException("Expected posix file system URIs.");
        }
    }

    @Override
    public void moveFile(URI source, URI target) throws CatalogIOException {
        checkUriExists(source);
        if (source.getScheme().equals("file") && target.getScheme().equals("file")) {
            try {
                Files.move(Paths.get(source), Paths.get(target), StandardCopyOption.REPLACE_EXISTING);
            } catch (IOException e) {
                throw new CatalogIOException("Can't move from " + source.getScheme() + " to " + target.getScheme() + ": " + e.getMessage(),
                        e);
            }
        } else {
            throw new CatalogIOException("Can't move from " + source.getScheme() + " to " + target.getScheme());
        }
    }

    @Override
    public void createFile(URI fileUri, InputStream inputStream) throws CatalogIOException {
        try {
            Files.copy(inputStream, Paths.get(fileUri), StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException e) {
            throw new CatalogIOException("create file failed at copying file " + fileUri, e);
        }
    }

    @Override
    public DataInputStream getFileObject(URI fileUri, int start, int limit)
            throws CatalogIOException {

        Path objectPath = Paths.get(fileUri);

        if (Files.isRegularFile(objectPath)) {
            try {
                if (start == -1 && limit == -1) {
                    return new DataInputStream(Files.newInputStream(objectPath));
                } else {
                    return new DataInputStream(IOUtils.headOffset(objectPath, start, limit));
                }
            } catch (IOException e) {
                throw new CatalogIOException("Unable to read file", e);
            }
        } else {
            throw new CatalogIOException("Not a regular file: " + objectPath.toAbsolutePath().toString());
        }
    }

    @Override
    public FileContent tail(Path file, int bytes, int lines) throws CatalogIOException {
        if (Files.isRegularFile(file)) {
            FileContent fileContent = new FileContent(file.toAbsolutePath().toString(), true, -1, -1, -1, "");

            String cli = "tail";
            if (lines > 0) {
                cli += " -n " + lines + " " + file.toAbsolutePath().toString();
                fileContent.setLines(lines);
            } else {
                if (bytes == 0 || bytes > MAXIMUM_BYTES) {
                    bytes = MAXIMUM_BYTES;
                }
                cli += " -c " + bytes + " " + file.toAbsolutePath().toString();
                fileContent.setBytes(bytes);
            }
            logger.debug("command line = {}", cli);

            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            Command command = new Command(cli)
                    .setOutputOutputStream(outputStream)
                    .setPrintOutput(false);
            command.run();
            fileContent.setContent(outputStream.toString());

            return fileContent;
        } else {
            throw new CatalogIOException("Not a regular file: " + file.toAbsolutePath().toString());
        }
    }

    @Override
    public FileContent head(Path file, int bytes, int lines) throws CatalogIOException {
        return content(file, 0, bytes, lines);
    }

    @Override
    public FileContent content(Path file, long offset, int bytes, int numLines) throws CatalogIOException {
        if (Files.isRegularFile(file)) {
            try {
                if (numLines > 0) {
                    return getContentPerLines(file, offset, numLines);
                } else {
                    return getContentPerBytes(file, offset, bytes);
                }
            } catch (IOException e) {
                throw new CatalogIOException("Error while reading the content of the file '" + file.toAbsolutePath().toString() + "'", e);
            }
        } else {
            throw new CatalogIOException("Not a regular file: " + file.toAbsolutePath().toString());
        }
    }

    private FileContent getContentPerBytes(Path path, long offset, int bytes) throws IOException {
        if (bytes == 0) {
            bytes = MAXIMUM_BYTES;
        }

        byte[] byteBuffer = new byte[bytes];

        RandomAccessFile file = new RandomAccessFile(path.toFile(), "r");
        file.seek(offset);
        int read = file.read(byteBuffer);

        FileContent fileContent = new FileContent(path.toAbsolutePath().toString(), read == -1, file.getFilePointer(), byteBuffer.length,
                new String(byteBuffer));
        file.close();

        return fileContent;
    }

    private FileContent getContentPerLines(Path path, long offset, int numLines) throws IOException {
        StringBuilder sb = new StringBuilder();

        RandomAccessFile file = new RandomAccessFile(path.toFile(), "r");
        file.seek(offset);

        int readLines = 0;
        boolean eof = false;

        while (numLines > readLines && MAXIMUM_BYTES > file.getFilePointer() - offset && !eof) {
            String tmpContent = file.readLine();
            if (tmpContent != null) {
                sb.append(tmpContent + System.lineSeparator());
                readLines++;
            } else {
                eof = true;
            }
        }

        FileContent fileContent = new FileContent(path.toAbsolutePath().toString(), eof, file.getFilePointer(),
                (int) (file.getFilePointer() - offset + 1), readLines, sb.toString());
        file.close();

        return fileContent;
    }

    @Override
    public FileContent grep(Path file, String pattern, int lines, boolean ignoreCase) throws CatalogIOException {
        if (Files.isRegularFile(file)) {
            String cli = "grep ";
            if (ignoreCase) {
                cli += "--ignore-case ";
            }
            if (lines > 0) {
                cli += "--max-count=" + lines + " ";
            }
            cli += pattern + " " + file.toAbsolutePath();

            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            Command command = new Command(cli)
                    .setOutputOutputStream(outputStream)
                    .setPrintOutput(false);
            command.run();

            return new FileContent(file.toAbsolutePath().toString(), lines == 0, 0, -1, lines, outputStream.toString());
        } else {
            throw new CatalogIOException("Not a regular file: " + file.toAbsolutePath().toString());
        }
    }

    @Override
    public DataOutputStream createOutputStream(URI fileUri, boolean overwrite) throws CatalogIOException {
        Path path = Paths.get(fileUri);
        logger.info("URI: {}", fileUri);
        if (overwrite || !Files.exists(path)) {
            try {
                return new DataOutputStream(new FileOutputStream(path.toFile()));
            } catch (IOException e) {
                throw new CatalogIOException("Unable to create file", e);
            }
        } else {
            throw new CatalogIOException("File already exists");
        }
    }

    @Override
    public String calculateChecksum(URI file) throws CatalogIOException {
        String checksum;
        try {
            String[] command = {"md5sum", file.getPath()};
            logger.debug("command = {} {}", command[0], command[1]);
            Process p = Runtime.getRuntime().exec(command);
            BufferedReader br = new BufferedReader(new InputStreamReader(p.getInputStream()));
            checksum = br.readLine();

            if (p.waitFor() != 0) {
                //TODO: Handle error in checksum
                logger.info("checksum = " + checksum);
                br = new BufferedReader(new InputStreamReader(p.getErrorStream()));
                throw new CatalogIOException("md5sum failed with exit value : " + p.exitValue() + ". ERROR: " + br.readLine());
            }
        } catch (IOException | InterruptedException e) {
            //TODO: Handle error in checksum
            throw new CatalogIOException("Checksum error in file " + file, e);
        }

        return checksum.split(" ")[0];
    }

    @Override
    public List<URI> listFiles(URI directory) throws CatalogIOException {
        checkUriExists(directory);
        class ListFiles extends SimpleFileVisitor<Path> {
            private List<String> filePaths = new LinkedList<>();

            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                filePaths.add(file.toAbsolutePath().toString());
                return super.visitFile(file, attrs);
            }

            public List<String> getFilePaths() {
                return filePaths;
            }
        }
        ListFiles fileVisitor = new ListFiles();
        try {
            Files.walkFileTree(Paths.get(directory.getPath()), fileVisitor);
        } catch (IOException e) {
            throw new CatalogIOException("Unable to walkFileTree", e);
        }
        List<URI> fileUris = new LinkedList<>();

        for (String filePath : fileVisitor.getFilePaths()) {
            try {
                fileUris.add(new URI("file", filePath, null));
            } catch (URISyntaxException e) {
                throw CatalogIOException.uriSyntaxException(filePath, e);
            }
        }
        return fileUris;
    }

    @Override
    public Stream<URI> listFilesStream(URI directory) throws CatalogIOException {
        try {
            return Files.walk(Paths.get(directory.getPath()))
                    .map(Path::toUri)
                    .filter(uri -> !uri.equals(directory));
//                    .filter(uri -> !uri.getPath().endsWith("/"))
        } catch (IOException e) {
            throw new CatalogIOException("Unable to list files", e);
        }
    }

    @Override
    public long getFileSize(URI file) throws CatalogIOException {
        checkUriScheme(file);
        try {
            return Files.size(Paths.get(file));
        } catch (IOException e) {
            e.printStackTrace();
            throw new CatalogIOException("Can't get file size", e);
        }
    }

    @Override
    public Date getCreationDate(URI file) throws CatalogIOException {
        checkUriScheme(file);
        try {
            return Date.from(Files.readAttributes(Paths.get(file), BasicFileAttributes.class).creationTime().toInstant());
        } catch (IOException e) {
            e.printStackTrace();
            throw new CatalogIOException("Can't get file size", e);
        }
    }

    @Override
    public Date getModificationDate(URI file) throws CatalogIOException {
        checkUriScheme(file);
        try {
            return Date.from(Files.readAttributes(Paths.get(file), BasicFileAttributes.class).lastModifiedTime().toInstant());
        } catch (IOException e) {
            e.printStackTrace();
            throw new CatalogIOException("Can't get file size", e);
        }
    }

}
