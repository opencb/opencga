package org.opencb.opencga.storage.core.io.managers;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.opencb.opencga.core.common.TimeUtils;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.net.URI;

/**
 * Created on 02/05/19.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class AzureBlobStorageIOManagerTest {

    private AzureBlobStorageIOManager io;
    private String azureStorageAccount = System.getenv("AZURE_STORAGE_ACCOUNT");
    private String azureStorageAccessKey = System.getenv("AZURE_STORAGE_ACCESS_KEY");

    @Before
    public void setUp() throws Exception {
        Assume.assumeTrue(StringUtils.isNoneEmpty(azureStorageAccessKey));
        io = new AzureBlobStorageIOManager(azureStorageAccount, azureStorageAccessKey);
    }

    @Test
    public void test() throws Exception {
        URI file = URI.create("https://" + azureStorageAccount + ".blob.core.windows.net/test/myTestFile.txt");

        Assert.assertTrue(io.supports(file));
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
        System.out.println("io.size(file) = " + io.size(file));
        System.out.println("io.md5(file) = " + io.md5(file));
        Assert.assertEquals(expectedSize, io.size(file));

        int size = 0;
        try(BufferedReader in = new BufferedReader(new InputStreamReader(io.newInputStream(file)))) {
            String readMessage;
            while ((readMessage = in.readLine()) != null) {
                size += readMessage.length() + 1;
            }
            Assert.assertEquals(expectedSize, size);
        }


        System.out.println(io.delete(file));
        System.out.println(io.delete(file));
    }

}