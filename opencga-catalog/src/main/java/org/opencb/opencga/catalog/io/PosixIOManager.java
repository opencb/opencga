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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.exec.Command;
import org.opencb.commons.utils.FileUtils;
import org.opencb.opencga.catalog.exceptions.CatalogIOException;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.common.IOUtils;
import org.opencb.opencga.core.models.file.FileContent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Base64;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Stream;

public class PosixIOManager extends IOManager {

    protected static Logger logger = LoggerFactory.getLogger(PosixIOManager.class);
    protected static ObjectMapper jsonObjectMapper;

    private static final int MAXIMUM_BYTES = 1024 * 1024;

    @Override
    protected void checkUriExists(URI uri) throws CatalogIOException {
        if (uri == null || !Files.exists(Paths.get(uri))) {
            throw new CatalogIOException("Path '" + uri + "' is null or it does not exist");
        }
    }

    @Override
    protected void checkUriScheme(URI uri) throws CatalogIOException {
        if (uri == null) {    //If scheme is missing, use "file" as scheme
            throw new CatalogIOException("URI is null");
        } else if (uri.getScheme() != null && !uri.getScheme().equals("file")) {    //If scheme is missing, use "file" as scheme
            throw new CatalogIOException("Unknown URI.scheme for URI '" + uri + "'");
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
    public long copy(InputStream in, URI target, CopyOption... options) throws CatalogIOException {
        try {
            return Files.copy(in, Paths.get(target), options);
        } catch (IOException e) {
            throw new CatalogIOException("Can't copy input stream to " + target + ": " + e.getMessage(), e);
        }
    }

    @Override
    public void copy(URI source, URI target, CopyOption... options) throws IOException, CatalogIOException {
        checkUriExists(source);
        if ("file".equals(source.getScheme()) && "file".equals(target.getScheme())) {
            Files.copy(Paths.get(source), Paths.get(target), options);
        } else {
            throw new CatalogIOException("Expected posix file system URIs.");
        }
    }

    @Override
    public void move(URI source, URI target, CopyOption... options) throws CatalogIOException {
        checkUriExists(source);
        if (source.getScheme().equals("file") && target.getScheme().equals("file")) {
            try {
                Files.move(Paths.get(source), Paths.get(target), options);
            } catch (IOException e) {
                throw new CatalogIOException("Can't move from " + source.getScheme() + " to " + target.getScheme() + ": " + e.getMessage(),
                        e);
            }
        } else {
            throw new CatalogIOException("Can't move from " + source.getScheme() + " to " + target.getScheme());
        }
    }

    @Override
    public void walkFileTree(URI start, FileVisitor<? super URI> visitor) throws CatalogIOException {
        FileVisitor<? super Path> fileVisitor = new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
                return visitor.preVisitDirectory(dir.toUri(), attrs);
            }

            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                return visitor.visitFile(file.toUri(), attrs);
            }

            @Override
            public FileVisitResult visitFileFailed(Path file, IOException exc) throws IOException {
                return visitor.visitFileFailed(file.toUri(), exc);
            }

            @Override
            public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                return visitor.postVisitDirectory(dir.toUri(), exc);
            }
        };

        try {
            Files.walkFileTree(Paths.get(start), fileVisitor);
        } catch (IOException e) {
            throw new CatalogIOException(e.getMessage(), e);
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
    public FileContent tail(Path file, int lines) throws CatalogIOException {
        if (file.toFile().getName().endsWith(".gz")) {
            throw new CatalogIOException("Tail does not work with compressed files.");
        }
        if (lines > ParamConstants.MAXIMUM_LINES_CONTENT) {
            throw new CatalogIOException("Unable to tail more than " + ParamConstants.MAXIMUM_LINES_CONTENT + ". Attempting to fail " + lines + " lines");
        }

        int averageBytesPerLine = 250;

        try (RandomAccessFile randomAccessFile = new RandomAccessFile(file.toFile(), "r")) {
            long length = randomAccessFile.length();

            long offset = length - (averageBytesPerLine * (lines + 10));
            if (offset < 0) {
                offset = 0;
            }
            if (length - offset > MAXIMUM_BYTES) {
                offset = length - MAXIMUM_BYTES;
            }

            List<String> contentList = new LinkedList<>();
            randomAccessFile.seek(offset);
            if (offset != 0) {
                // If there is an offset, discard first line as it could be truncated
                randomAccessFile.readLine();
            }

            String line;
            while ((line = randomAccessFile.readLine()) != null) {
                contentList.add(line);
            }

            while (offset > 0 && contentList.size() < lines && length - MAXIMUM_BYTES < offset) {
                // We need to get more lines

                // Recalculate the average number of bytes per line from the file
                if (contentList.size() > 0) {
                    averageBytesPerLine = Math.round((length - offset) / ((float) contentList.size()));
                } else {
                    averageBytesPerLine = averageBytesPerLine * 2;
                }

                // We will always try to move the offset to 10 lines before
                int remainingLines = 10 + lines - contentList.size();

                long to = offset;
                offset = to - (averageBytesPerLine * remainingLines);
                if (offset < 0) {
                    offset = 0;
                }
                if (length - offset > MAXIMUM_BYTES) {
                    offset = length - MAXIMUM_BYTES;
                }

                randomAccessFile.seek(offset);
                List<String> additionalList = new LinkedList<>();

                if (offset != 0) {
                    // If there is an offset, discard first line as it could be truncated
                    randomAccessFile.readLine();
                }
                while (randomAccessFile.getFilePointer() < to) {
                    additionalList.add(randomAccessFile.readLine());
                }

                // Remove first line as it will probably be incomplete and it will be completely added from the additionalList
                contentList.addAll(0, additionalList);
            }

            if (contentList.size() > lines) {
                contentList = contentList.subList(contentList.size() - lines, contentList.size());
            }
            String fullContent = StringUtils.join(contentList, System.lineSeparator());

            return new FileContent(file.toAbsolutePath().toString(), true, length, fullContent.getBytes().length, contentList.size(),
                    fullContent);
        } catch (IOException e) {
            throw new CatalogIOException("Not a regular file: " + file.toAbsolutePath().toString() + ": " + e.getMessage(), e);
        }
    }

    @Override
    public FileContent base64Image(Path file) throws CatalogIOException {
        byte[] fileContent;
        try {
            fileContent = org.apache.commons.io.FileUtils.readFileToByteArray(file.toFile());
        } catch (IOException e) {
            throw new CatalogIOException("Cannot get byte array from file '" + file + "'.", e);
        }
        String encodedString = Base64.getEncoder().encodeToString(fileContent);
        return new FileContent(file.toAbsolutePath().toString(), true, file.toFile().length(), (int) file.toFile().length(), encodedString);
    }

    @Override
    public FileContent head(Path file, long offset, int lines) throws CatalogIOException {
        if (Files.isRegularFile(file)) {
            if (lines > ParamConstants.MAXIMUM_LINES_CONTENT) {
                throw new CatalogIOException("Unable to tail more than " + ParamConstants.MAXIMUM_LINES_CONTENT + ". Attempting to fail " + lines + " lines");
            }
            if (offset == 0) {
                int numLinesReturned = 0;
                try (BufferedReader bufferedReader = FileUtils.newBufferedReader(file)) {
                    StringBuilder sb = new StringBuilder();

                    boolean eof = false;

                    int bytes = 0;
                    while (numLinesReturned < lines && bytes < MAXIMUM_BYTES && !eof) {
                        String line = bufferedReader.readLine();

                        if (line != null) {
                            line = line + "\n";
                            bytes += line.getBytes().length;
                            sb.append(line);

                            numLinesReturned++;
                        } else {
                            eof = true;
                        }
                    }

                    return new FileContent(file.toAbsolutePath().toString(), eof, bytes, bytes, numLinesReturned, sb.toString());
                } catch (IOException e) {
                    throw new CatalogIOException(e.getMessage(), e);
                }
            } else {
                if (file.toFile().getName().endsWith(".gz")) {
                    throw new CatalogIOException("Content does not work with compressed files.");
                }
                try {
                    return getContentPerLines(file, offset, lines);
                } catch (IOException e) {
                    throw new CatalogIOException("Error while reading the content of the file '" + file.toAbsolutePath().toString() + "'",
                            e);
                }
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
            if (file.toFile().getName().endsWith(".gz")) {
                throw new CatalogIOException("Grep does not work with compressed files.");
            }

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
