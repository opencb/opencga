package org.opencb.opencga.storage.core.io.managers;

import com.microsoft.azure.storage.blob.*;
import com.microsoft.azure.storage.blob.models.*;
import io.reactivex.Flowable;
import io.reactivex.Single;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.log4j.Level;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.core.common.TimeUtils;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.math.BigInteger;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.InvalidKeyException;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Created on 01/05/19.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class AzureBlobStorageIOManager implements IOManager {

    private final String accountName;
    private final String accountKey;
    private ServiceURL serviceURL;
    protected static Logger logger = LoggerFactory.getLogger(AzureBlobStorageIOManager.class);

    public AzureBlobStorageIOManager() {
        this(System.getenv("AZURE_STORAGE_ACCOUNT"), System.getenv("AZURE_STORAGE_ACCESS_KEY"));
    }

    public AzureBlobStorageIOManager(ObjectMap options) {
        accountName = options.getString("accountName");
        accountKey = options.getString("accountKey");
    }

    public AzureBlobStorageIOManager(String azureStorageAccount, String azureStorageAccessKey) {
        accountName = azureStorageAccount;
        accountKey = azureStorageAccessKey;
    }

    @Override
    public boolean supports(URI uri) {
        if (StringUtils.isNotEmpty(uri.getScheme())) {
            if (uri.getScheme().equals("http") || uri.getScheme().equals("https")) {
                if (uri.getHost().endsWith(accountName + ".blob.core.windows.net")) {
                    return true;
                }
            }
        }
        return false;
    }

    private void ensureOpen() throws IOException {
        if (serviceURL == null) {
            synchronized (this) {
                if (serviceURL == null) {
                    try {
                        open();
                    } catch (InvalidKeyException e) {
                        throw new IOException("Error connecting to Azure Storage", e);
                    }
                }
            }
        }
    }

    private void open() throws InvalidKeyException {
        // Mute INFO from LoggingFactory.slf4jLogger.
        // See https://github.com/Azure/azure-storage-java/issues/433
        org.apache.log4j.Logger.getLogger(LoggingFactory.class).setLevel(Level.WARN);

        // Create a ServiceURL to call the Blob service. We will also use this to construct the ContainerURL
        SharedKeyCredentials creds = new SharedKeyCredentials(accountName, accountKey);
        // We are using a default pipeline here, you can learn more about it at
        // https://github.com/Azure/azure-storage-java/wiki/Azure-Storage-Java-V10-Overview
        try {
            serviceURL = new ServiceURL(new URL("https://" + accountName + ".blob.core.windows.net"),
                    StorageURL.createPipeline(creds, new PipelineOptions()
                            .withLoggingOptions(new LoggingOptions(3000))
                    ));
        } catch (MalformedURLException e) {
            throw new IllegalArgumentException(e);
        }
    }

    protected BlobURL createBlobURL(URI uri) throws IOException {
        ensureOpen();
        String path = uri.getPath();
        String blobName = path.substring(path.indexOf('/', 1) + 1);
        return createContainerURL(uri).createBlobURL(blobName);
    }

    protected ContainerURL createContainerURL(URI uri) throws IOException {
        ensureOpen();
        String path = uri.getPath();
        String containerName = path.substring(1, path.indexOf('/', 1));
        return serviceURL.createContainerURL(containerName);
    }

    public List<String> listContainers() throws IOException {
        ensureOpen();
        return serviceURL.listContainersSegment(null, null)
                .blockingGet()
                .body()
                .containerItems()
                .stream()
                .map(ContainerItem::name)
                .collect(Collectors.toList());
    }

    @Override
    public InputStream newInputStreamRaw(URI uri) throws IOException {
        BlobURL blobURL = createBlobURL(uri);

        Single<DownloadResponse> download = blobURL.download();
        Flowable<ByteBuffer> body = download.blockingGet().body(null);
        Iterator<ByteBuffer> byteBuffers = body.blockingIterable().iterator();

        return new AzureBlobInputStream(byteBuffers);
    }

    @Override
    public OutputStream newOutputStreamRaw(URI uri) throws IOException {
        return new AzureBlobOutputStream(createBlobURL(uri).toBlockBlobURL());
    }

    public OutputStream newOutputStreamLocalScratch(Path localScratch, URI uri) throws IOException {
        Path tmpFile;
        if (localScratch == null) {
            tmpFile = Files.createTempFile("azure_blob", ".tmp");
        } else {
            tmpFile = Files.createTempFile(localScratch, "azure_blob", ".tmp");
        }
        return new FileOutputStream(tmpFile.toFile()) {
            @Override
            public void close() throws IOException {
                super.close();
                copyFromLocal(tmpFile, uri);
                Files.deleteIfExists(tmpFile);
            }
        };
    }

    @Override
    public boolean exists(URI uri) throws IOException {
        try {
            getBlobGetPropertiesHeaders(uri);
            return true;
        } catch (FileNotFoundException e) {
            return false;
        }
    }

    @Override
    public boolean isDirectory(URI uri) throws IOException {
        return uri.getPath().endsWith("/");
    }

    @Override
    public boolean canWrite(URI uri) throws IOException {
        return true;
    }

    @Override
    public long size(URI uri) throws IOException {
        return getBlobGetPropertiesHeaders(uri).contentLength();
    }

    @Override
    public String md5(URI uri) throws IOException {
        byte[] bytes = getBlobGetPropertiesHeaders(uri).contentMD5();
        if (bytes == null) {
            return null;
        } else {
            return String.format("%032x", new BigInteger(1, bytes));
        }
    }

    private BlobGetPropertiesHeaders getBlobGetPropertiesHeaders(URI uri) throws IOException {
        BlobGetPropertiesHeaders blobProperties;
        try {
            blobProperties = createBlobURL(uri).getProperties().blockingGet().headers();
        } catch (StorageException e) {
            if (e.errorCode().equals(StorageErrorCode.BLOB_NOT_FOUND)) {
                throw new FileNotFoundException(uri.toString());
            } else {
                throw new IOException("Problem reading blob properties. ErrorCode: " + e.errorCode(), e);
            }
        }
        return blobProperties;
    }

    @Override
    public void copyFromLocal(Path localSourceFile, URI targetFile) throws IOException {
        BlockBlobURL blobURL = createBlobURL(targetFile).toBlockBlobURL();

        StopWatch stopWatch = StopWatch.createStarted();
        AsynchronousFileChannel fileChannel = AsynchronousFileChannel.open(localSourceFile);
        // Uploading a file to the blobURL using the high-level methods available in TransferManager class
        // Alternatively call the PutBlob/PutBlock low-level methods from BlockBlobURL type
        CommonRestResponse response = TransferManager.uploadFileToBlockBlob(fileChannel, blobURL, 8 * 1024 * 1024, null, null)
                .blockingGet();

        logger.info("[" + response.response().statusCode() + "] Completed copyFromLocal in " + TimeUtils.durationToString(stopWatch));
    }

    @Override
    public void copyToLocal(URI sourceFile, Path localTargetFile) throws IOException {
        BlobURL blobURL = createBlobURL(sourceFile);

        StopWatch stopWatch = StopWatch.createStarted();
        AsynchronousFileChannel fileChannel = AsynchronousFileChannel.open(localTargetFile,
                StandardOpenOption.CREATE, StandardOpenOption.WRITE);

        TransferManager.downloadBlobToFile(fileChannel, blobURL, null, null).blockingGet();
        logger.info("Completed copyToLocal in " + TimeUtils.durationToString(stopWatch));
    }

    //    public void move(URI source, URI target) throws IOException {
//        // Copy & delete
//    }

    @Override
    public boolean delete(URI uri) throws IOException {
        try {
            BlobDeleteResponse response = createBlobURL(uri).delete().blockingGet();
            logger.info("[" + response.statusCode() + "] Completed delete request.");
            return true;
        } catch (StorageException e) {
            if (e.errorCode().equals(StorageErrorCode.BLOB_NOT_FOUND)) {
                return false;
            } else {
                throw e;
            }
        }
    }

    private static class AzureBlobInputStream extends InputStream {
        private final Iterator<ByteBuffer> byteBuffers;
        private ByteBuffer current;

        AzureBlobInputStream(Iterator<ByteBuffer> byteBuffers) {
            this.byteBuffers = byteBuffers;
            current = ByteBuffer.wrap(new byte[0]);
        }

        @Override
        public int read() throws IOException {
            if (current.hasRemaining()) {
                return 0xFF & current.get();
            } else {
                if (byteBuffers.hasNext()) {
                    current = byteBuffers.next();
                    return read();
                } else {
                    return -1;
                }
            }
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            if (b == null) {
                throw new NullPointerException();
            } else if (off < 0 || len < 0 || len > b.length - off) {
                throw new IndexOutOfBoundsException();
            } else if (len == 0) {
                return 0;
            }
            int i = 0;
            while (len > 0) {
                while (!current.hasRemaining()) {
                    if (byteBuffers.hasNext()) {
                        current = byteBuffers.next();
                    } else {
                        if (i == 0) {
                            return -1;
                        } else {
                            return i;
                        }
                    }
                }
                int remaining = current.remaining();
                int readBytes;
                if (len > remaining) {
                    current.get(b, off, remaining);
                    readBytes = remaining;
                } else {
                    current.get(b, off, len);
                    readBytes = len;
                }
                i += readBytes;
                len -= readBytes;
                off += readBytes;
            }
            return i;
        }

        @Override
        public int available() throws IOException {
            while (!current.hasRemaining()) {
                if (byteBuffers.hasNext()) {
                    current = byteBuffers.next();
                } else {
                    return -1;
                }
            }
            return 1;
        }
    }

    /**
     * OutputStream that generates a Flowable of ByteBuffer.
     */
    private static class AzureBlobOutputStream extends OutputStream {
        private static final int BLOCK_SIZE = 8 * 1024 * 1024;
        private final ArrayBlockingQueue<CompletableFuture<ByteBuffer>> queue;
        private final Thread thread;
        private CompletableFuture<ByteBuffer> currentFuture;
        private ByteBuffer byteBuffer;

        AzureBlobOutputStream(BlockBlobURL blockBlobURL) {
            queue = new ArrayBlockingQueue<>(10);
            byteBuffer = ByteBuffer.allocate(BLOCK_SIZE);
            currentFuture = new CompletableFuture<>();
            queue.add(currentFuture);

            Flowable<ByteBuffer> flowable = Flowable.concat(() ->
                    new Iterator<Publisher<? extends ByteBuffer>>() {
                        @Override
                        public boolean hasNext() {
                            return !queue.isEmpty();
                        }

                        @Override
                        public Flowable<ByteBuffer> next() {
                            CompletableFuture<ByteBuffer> future = queue.poll();
                            if (future == null) {
                                throw new NoSuchElementException();
                            }
                            return Flowable.fromFuture(future);
                        }
                    });
            Single<BlockBlobCommitBlockListResponse> single =
                    TransferManager.uploadFromNonReplayableFlowable(flowable, blockBlobURL, BLOCK_SIZE, 5, null);

            // Use blockingGet in a Thread so we can join the thread at OutputStream.close()
            thread = new Thread(single::blockingGet);
            thread.start();

//            single.subscribeOn(Schedulers.newThread())
//                    .observeOn(Schedulers.single())
//                    .subscribe(System.out::println, Throwable::printStackTrace);
        }

        @Override
        public void write(int b) throws IOException {
            if (byteBuffer.capacity() == 0) {
                flush();
            }
            byteBuffer.put((byte) b);
        }

        @Override
        public synchronized void write(byte[] b, int off, int len) throws IOException {
            while (len > byteBuffer.remaining()) {
                int remaining = byteBuffer.remaining();
                byteBuffer.put(b, off, remaining);
                off += remaining;
                len -= remaining;
                flush();
            }
            byteBuffer.put(b, off, len);
        }

        @Override
        public void close() throws IOException {
            super.close();
            completeCurrentFuture();
            try {
                thread.join();
            } catch (InterruptedException e) {
                throw new IOException(e);
            }
        }

        @Override
        public synchronized void flush() throws IOException {
            CompletableFuture<ByteBuffer> nextFuture = new CompletableFuture<>();
            try {
                queue.offer(nextFuture, 30, TimeUnit.MINUTES);
            } catch (InterruptedException e) {
                throw new IOException(e);
            }
            completeCurrentFuture();
            currentFuture = nextFuture;
            byteBuffer = ByteBuffer.allocate(BLOCK_SIZE);
        }

        protected void completeCurrentFuture() {
            byteBuffer.limit(byteBuffer.position());
            byteBuffer.position(0);
            currentFuture.complete(byteBuffer);
        }
    }
}
