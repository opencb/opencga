package org.opencb.opencga.storage.core.io.managers;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.junit.*;
import org.opencb.opencga.core.common.TimeUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Created on 02/05/19.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class AzureBlobStorageIOConnectorTest {

    private AzureBlobStorageIOConnector io;
    private String azureStorageAccount = System.getenv("AZURE_STORAGE_ACCOUNT");
    private String azureStorageAccessKey = System.getenv("AZURE_STORAGE_ACCESS_KEY");
    private URI file;
    private static Path rootDir;

    @BeforeClass
    public static void beforeClass() throws IOException {
        rootDir = Paths.get("target/test-data", "junit-opencga-storage-" + TimeUtils.getTimeMillis() + "_" + RandomStringUtils.randomAlphabetic(3));
        Files.createDirectories(rootDir);
    }

    @Before
    public void setUp() throws Exception {
        Assume.assumeTrue(StringUtils.isNoneEmpty(azureStorageAccessKey));
        io = new AzureBlobStorageIOConnector(azureStorageAccount, azureStorageAccessKey);
        file = URI.create("https://" + azureStorageAccount + ".blob.core.windows.net/test/myTestFile.txt");
    }

    @Test
    public void testListContainers() throws IOException {
        System.out.println(io.listContainers());
    }

    @Test
    public void testIsValid() throws IOException {
        Assert.assertTrue(io.isValid(file));
    }

    @Test
    public void testIsValidIOManagerFactory() throws IOException {
        Assert.assertTrue(new IOConnectorProvider(LocalIOConnector.class, AzureBlobStorageIOConnector.class).isValid(file));
    }

    @Test(timeout = 10000)
    public void testUploadAndDelete() throws Exception {
        String message = "Hello world! " + System.currentTimeMillis();

        StopWatch stopWatch = StopWatch.createStarted();
        try(PrintStream out = new PrintStream(io.newOutputStream(file))) {
            for (int f = 0; f < 1024; f++) {
                for (int i = 0; i < (1024); i++) {
                    out.println(message);
                }
//                Thread.sleep(10);
            }
        }
        System.out.println(TimeUtils.durationToString(stopWatch));

        int expectedSize = (message.length() + 1) * 1024 * 1024;
        System.out.println("file size = " + expectedSize);

        Assert.assertEquals(expectedSize, io.size(file));
        Assert.assertTrue(io.exists(file));

        int size = 0;
        try(BufferedReader in = new BufferedReader(new InputStreamReader(io.newInputStream(file)))) {
            String readMessage;
            while ((readMessage = in.readLine()) != null) {
                size += readMessage.length() + 1;
            }
            Assert.assertEquals(expectedSize, size);
        }

        System.out.println("io.md5(file) = " + io.md5(file));
        Path localFile = rootDir.resolve("tmpFile.txt");
        io.copyToLocal(file, localFile);
        io.copyFromLocal(localFile, file);
        Assert.assertEquals(expectedSize, Files.size(localFile));

        System.out.println("io.md5(file) = " + io.md5(file));
        Assert.assertEquals(expectedSize, io.size(file));

        Assert.assertTrue(io.delete(file));
        Assert.assertFalse(io.delete(file));
    }

}