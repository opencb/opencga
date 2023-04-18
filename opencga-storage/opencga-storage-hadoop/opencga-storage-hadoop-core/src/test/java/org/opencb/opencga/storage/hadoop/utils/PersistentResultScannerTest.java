package org.opencb.opencga.storage.hadoop.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.UnknownScannerException;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;
import org.opencb.opencga.core.testclassification.duration.MediumTests;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageTest.HadoopExternalResource;

import java.util.Iterator;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.*;

/**
 * Created on 05/02/19.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
@Category(MediumTests.class)
public class PersistentResultScannerTest {

    @Rule
    public HadoopExternalResource hadoop = new HadoopExternalResource();

    private HBaseManager hBaseManager;


    @Before
    public void setUp() throws Exception {
        Configuration conf = hadoop.getConf();
        hBaseManager = new HBaseManager(conf);
    }

    @Test
    public void test() throws Exception {

        byte[] family = Bytes.toBytes("0");
        String tableName = "test";

        hBaseManager.createTableIfNeeded(tableName, family, Compression.Algorithm.NONE);
        hBaseManager.act(tableName, table -> {
            for (int i1 = 0; i1 < 100; i1++) {
                table.put(new Put(Bytes.toBytes(String.format("r_%03d", i1))).addColumn(family, Bytes.toBytes("value"), Bytes.toBytes(i1)));
            }
        });

        Scan scan = new Scan().setBatch(1).setCaching(1).setCacheBlocks(false);
        ResultScanner scanner = hBaseManager.getScanner(tableName, scan);

        assertThat(scanner, instanceOf(PersistentResultScanner.class));
        PersistentResultScanner persistentScanner = (PersistentResultScanner) scanner;

        int i = 0;
        for (Iterator<Result> iterator = scanner.iterator(); iterator.hasNext(); ) {
            Result result = iterator.next();
            System.out.println(Bytes.toString(result.getRow()));
            i++;
            if (i == 50 || i == 99) {
                int scannersCount = persistentScanner.getScannersCount();
                ResultScanner spy = Mockito.spy(persistentScanner.scanner);
                Mockito.doThrow(new UnknownScannerException("Mock exception")).when(spy).next();
                persistentScanner.scanner = spy;
                assertTrue(iterator.hasNext()); // Force exception

                assertEquals(scannersCount + 1, persistentScanner.getScannersCount());
                assertNotSame(spy, persistentScanner.scanner);
            }
        }

        assertEquals(100, i);
    }

}