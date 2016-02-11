package org.opencb.opencga.storage.hadoop.variant;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.tools.ant.types.Commandline;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.opencb.datastore.core.ObjectMap;
import org.opencb.opencga.storage.core.config.StorageConfiguration;
import org.opencb.opencga.storage.core.config.StorageEtlConfiguration;
import org.opencb.opencga.storage.core.variant.VariantStorageManager;
import org.opencb.opencga.storage.core.variant.VariantStorageManagerTestUtils;
import org.opencb.opencga.storage.core.variant.VariantStorageTest;
import org.opencb.opencga.storage.hadoop.utils.HBaseManager;
import org.opencb.opencga.storage.hadoop.variant.archive.ArchiveDriver;
import org.opencb.opencga.storage.hadoop.variant.index.VariantTableDriver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Created on 15/10/15
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class HadoopVariantStorageManagerTestUtils extends VariantStorageManagerTestUtils implements VariantStorageTest {

    public static AtomicReference<HBaseTestingUtility> utility = new AtomicReference<>(null);
    public static AtomicReference<Configuration> configuration = new AtomicReference<>(null);



    @BeforeClass
    public static void initializeMiniCluster() throws Exception {
//        ConsoleAppender stderr = (ConsoleAppender) LogManager.getRootLogger().getAppender("stderr");
//        stderr.setThreshold(Level.toLevel("debug"));

        if (utility.get() == null) {
            utility.set(new HBaseTestingUtility());
            utility.get().startMiniCluster(1);
            configuration.set(utility.get().getConfiguration());
            configuration.get().setBoolean("addDependencyJars", false);
            configuration.get().setBoolean("opencga.storage.hbase.table_compress", false);

//            MiniMRCluster miniMRCluster = utility.startMiniMapReduceCluster();
//            MiniMRClientCluster miniMRClientCluster = MiniMRClientClusterFactory.create(HadoopVariantStorageManagerTestUtils.class, 1, configuration);
//            miniMRClientCluster.start();

            HBaseManager hBaseManager = new HBaseManager(configuration.get());
            Connection con = ConnectionFactory.createConnection(configuration.get());

            String tableName = "table";
            byte[] columnFamily = Bytes.toBytes("0");
            hBaseManager.createTableIfNeeded(con, tableName, columnFamily);
            hBaseManager.act(con, tableName, table -> {
                table.put(Arrays.asList(new Put(Bytes.toBytes("r1")).addColumn(columnFamily, Bytes.toBytes("c"), Bytes.toBytes("value 1")),
                        new Put(Bytes.toBytes("r2")).addColumn(columnFamily, Bytes.toBytes("c"), Bytes.toBytes("value 2")),
                        new Put(Bytes.toBytes("r2")).addColumn(columnFamily, Bytes.toBytes("c2"), Bytes.toBytes("value 3"))));
            });

            hBaseManager.act(con, tableName, table -> {
                table.getScanner(columnFamily).forEach(result -> {
                    System.out.println("Row: " + Bytes.toString(result.getRow()));
                    for (Map.Entry<byte[], byte[]> entry : result.getFamilyMap(columnFamily).entrySet()) {
                        System.out.println(Bytes.toString(entry.getKey()) + " = " + Bytes.toString(entry.getValue()));
                    }
                });
            });

            TableName tname = TableName.valueOf(tableName);
            try (Admin admin = con.getAdmin()) {
                if (admin.tableExists(tname)) {
                    utility.get().deleteTable(tableName);
                }
            }

            con.close();

        }
    }
    @AfterClass
    public static void shutdownMiniCluster() throws Exception {
        try {
            if (utility.get() != null) {
                utility.get().shutdownMiniCluster();
            }
        } finally {
            utility.set(null);
        }
    }

    @Override
    public HadoopVariantStorageManager getVariantStorageManager() throws Exception {

        HadoopVariantStorageManager manager = new HadoopVariantStorageManager();

        InputStream is = HadoopVariantStorageManagerTestUtils.class.getClassLoader().getResourceAsStream("storage-configuration.yml");
        StorageConfiguration storageConfiguration = StorageConfiguration.load(is);
        storageConfiguration.setDefaultStorageEngineId(HadoopVariantStorageManager.STORAGE_ENGINE_ID);
        StorageEtlConfiguration variantConfiguration = storageConfiguration.getStorageEngine(HadoopVariantStorageManager.STORAGE_ENGINE_ID).getVariant();
        ObjectMap options = variantConfiguration.getOptions();

        //Make a copy of the configuration
        Configuration conf = new Configuration(false);
        HadoopVariantStorageManagerTestUtils.configuration.get().forEach(e -> conf.set(e.getKey(), e.getValue()));
        conf.forEach(entry -> options.put(entry.getKey(), entry.getValue()));

        FileSystem fs = FileSystem.get(HadoopVariantStorageManagerTestUtils.configuration.get());
        String intermediateDirectory = fs.getHomeDirectory().toUri().resolve("opencga_test/").toString();
        System.out.println(HadoopVariantStorageManager.OPENCGA_STORAGE_HADOOP_INTERMEDIATE_HDFS_DIRECTORY + " = " + intermediateDirectory);
        options.put(HadoopVariantStorageManager.OPENCGA_STORAGE_HADOOP_INTERMEDIATE_HDFS_DIRECTORY, intermediateDirectory);
        variantConfiguration.getDatabase().setHosts(Collections.singletonList("hbase://" + HadoopVariantStorageManagerTestUtils.configuration.get().get(HConstants.ZOOKEEPER_QUORUM)));

        manager.setConfiguration(storageConfiguration, HadoopVariantStorageManager.STORAGE_ENGINE_ID);
        manager.mrExecutor = new TestMRExecutor(HadoopVariantStorageManagerTestUtils.configuration.get());
        manager.conf = conf;
        return manager;
    }

    @Override
    public void clearDB(String tableName) throws Exception {
        TableName tname = TableName.valueOf(tableName);
        try (Connection con = ConnectionFactory.createConnection(configuration.get()); Admin admin = con.getAdmin()) {
            if (admin.tableExists(tname)) {
                utility.get().deleteTable(tableName);
            }
        }
    }

    class TestMRExecutor implements MRExecutor {

        private final Configuration configuration;

        public TestMRExecutor(Configuration configuration) {
            this.configuration = configuration;
        }

        @Override
        public int run(String executable, String args) {
            try {
                if (executable.endsWith(ArchiveDriver.class.getName())) {
//                new ArchiveDriver(configuration).run(Commandline.translateCommandline(args));
//                    ArchiveDriver.externalJobConfigurator = job -> {
//                        MiniMRCluster miniMRCluster = utility.startMiniMapReduceCluster();
//                        miniMRCluster;
//                    }
                    System.out.println("Executing ArchiveDriver");
                    int r = ArchiveDriver.privateMain(Commandline.translateCommandline(args), configuration);
                    System.out.println("Finish execution ArchiveDriver");

                    return r;
                } else if (executable.endsWith(VariantTableDriver.class.getName())) {
                    System.out.println("Executing VariantTableDriver");
                    int r = VariantTableDriver.privateMain(Commandline.translateCommandline(args), configuration);
                    System.out.println("Finish execution VariantTableDriver");
                    return r;
                }
            } catch (Exception e) {
                e.printStackTrace();
                return -1;
            }
            return 0;
        }
    }

}
