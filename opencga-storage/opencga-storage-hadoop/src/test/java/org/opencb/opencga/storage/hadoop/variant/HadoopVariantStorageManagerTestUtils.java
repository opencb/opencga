package org.opencb.opencga.storage.hadoop.variant;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.log4j.Level;
import org.apache.tools.ant.types.Commandline;
import org.junit.Assert;
import org.junit.rules.ExternalResource;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.storage.core.config.StorageConfiguration;
import org.opencb.opencga.storage.core.config.StorageEtlConfiguration;
import org.opencb.opencga.storage.core.variant.VariantStorageTest;
import org.opencb.opencga.storage.hadoop.utils.HBaseManager;
import org.opencb.opencga.storage.hadoop.variant.archive.ArchiveDriver;
import org.opencb.opencga.storage.hadoop.variant.executors.MRExecutor;
import org.opencb.opencga.storage.hadoop.variant.index.VariantTableDeletionDriver;
import org.opencb.opencga.storage.hadoop.variant.index.VariantTableDriver;
import org.opencb.opencga.storage.hadoop.variant.index.VariantTableMapper;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Created on 15/10/15
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public interface HadoopVariantStorageManagerTestUtils /*extends VariantStorageManagerTestUtils */ extends VariantStorageTest {

    AtomicReference<HBaseTestingUtility> utility = new AtomicReference<>(null);
    AtomicReference<Configuration> configuration = new AtomicReference<>(null);

    class HadoopExternalResource extends ExternalResource implements HadoopVariantStorageManagerTestUtils {

        @Override
        public void before() throws Throwable {
            if (utility.get() == null) {

                //Disable HBase logging
                org.apache.log4j.Logger.getLogger(FSNamesystem.class.getName() + ".audit").setLevel(Level.WARN);

                utility.set(new HBaseTestingUtility());
                utility.get().startMiniCluster(1);
                configuration.set(utility.get().getConfiguration());

    //            MiniMRCluster miniMRCluster = utility.startMiniMapReduceCluster();
    //            MiniMRClientCluster miniMRClientCluster = MiniMRClientClusterFactory.create(HadoopVariantStorageManagerTestUtils.class, 1, configuration);
    //            miniMRClientCluster.start();

//                checkHBaseMiniCluster();

            }
        }

        @Override
        public void after() {
            try {
                try {
                    if (utility.get() != null) {
                        utility.get().shutdownMiniCluster();
                    }
                } finally {
                    utility.set(null);
                }
            } catch (Exception e) {
                Assert.fail(e.getMessage());
            }
        }

        public Configuration getConf() {
            return configuration.get();
        }

        private void checkHBaseMiniCluster() throws IOException {
            Connection con = ConnectionFactory.createConnection(configuration.get());
            HBaseManager hBaseManager = new HBaseManager(configuration.get(), con);

            String tableName = "table";
            byte[] columnFamily = Bytes.toBytes("0");
            hBaseManager.createTableIfNeeded(tableName, columnFamily, Compression.Algorithm.NONE);
            hBaseManager.act(tableName, table -> {
                table.put(Arrays.asList(new Put(Bytes.toBytes("r1")).addColumn(columnFamily, Bytes.toBytes("c"), Bytes.toBytes("value 1")),
                        new Put(Bytes.toBytes("r2")).addColumn(columnFamily, Bytes.toBytes("c"), Bytes.toBytes("value 2")),
                        new Put(Bytes.toBytes("r2")).addColumn(columnFamily, Bytes.toBytes("c2"), Bytes.toBytes("value 3"))));
            });

            hBaseManager.act(tableName, table -> {
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

    @Override
    default HadoopVariantStorageManager getVariantStorageManager() throws Exception {

        HadoopVariantStorageManager manager = new HadoopVariantStorageManager();

        //Make a copy of the configuration
        Configuration conf = new Configuration(false);
        HBaseConfiguration.merge(conf, HadoopVariantStorageManagerTestUtils.configuration.get());
        StorageConfiguration storageConfiguration = getStorageConfiguration(conf);

        manager.setConfiguration(storageConfiguration, HadoopVariantStorageManager.STORAGE_ENGINE_ID);
        manager.mrExecutor = new TestMRExecutor(conf);
        manager.conf = conf;
        return manager;
    }

    static StorageConfiguration getStorageConfiguration(Configuration conf) throws IOException {
        StorageConfiguration storageConfiguration;
        try (InputStream is = HadoopVariantStorageManagerTestUtils.class.getClassLoader().getResourceAsStream("storage-configuration.yml")) {
            storageConfiguration = StorageConfiguration.load(is);
        }
        return updateStorageConfiguration(storageConfiguration, conf);
    }

    static StorageConfiguration updateStorageConfiguration(StorageConfiguration storageConfiguration, Configuration conf) throws IOException {
        storageConfiguration.setDefaultStorageEngineId(HadoopVariantStorageManager.STORAGE_ENGINE_ID);
        StorageEtlConfiguration variantConfiguration = storageConfiguration.getStorageEngine(HadoopVariantStorageManager.STORAGE_ENGINE_ID).getVariant();
        ObjectMap options = variantConfiguration.getOptions();

        options.put(HadoopVariantStorageManager.EXTERNAL_MR_EXECUTOR, TestMRExecutor.class);
        TestMRExecutor.setStaticConfiguration(conf);

        options.put(GenomeHelper.CONFIG_HBASE_ADD_DEPENDENCY_JARS, false);
        EnumSet<Compression.Algorithm> supportedAlgorithms = EnumSet.of(Compression.Algorithm.NONE, HBaseTestingUtility.getSupportedCompressionAlgorithms());

        options.put(ArchiveDriver.CONFIG_ARCHIVE_TABLE_COMPRESSION, supportedAlgorithms.contains(Compression.Algorithm.GZ)
                ? Compression.Algorithm.GZ.getName()
                : Compression.Algorithm.NONE.getName());
        options.put(VariantTableDriver.CONFIG_VARIANT_TABLE_COMPRESSION, supportedAlgorithms.contains(Compression.Algorithm.SNAPPY)
                ? Compression.Algorithm.SNAPPY.getName()
                : Compression.Algorithm.NONE.getName());

        FileSystem fs = FileSystem.get(HadoopVariantStorageManagerTestUtils.configuration.get());
        String intermediateDirectory = fs.getHomeDirectory().toUri().resolve("opencga_test/").toString();
        System.out.println(HadoopVariantStorageManager.OPENCGA_STORAGE_HADOOP_INTERMEDIATE_HDFS_DIRECTORY + " = " + intermediateDirectory);
        options.put(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, conf.get(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY));
        options.put(HadoopVariantStorageManager.OPENCGA_STORAGE_HADOOP_INTERMEDIATE_HDFS_DIRECTORY, intermediateDirectory);

        variantConfiguration.getDatabase().setHosts(Collections.singletonList("hbase://" + HadoopVariantStorageManagerTestUtils.configuration.get().get(HConstants.ZOOKEEPER_QUORUM)));
        return storageConfiguration;
    }

    default void clearHBase() throws Exception {
        try (Connection con = ConnectionFactory.createConnection(configuration.get()); Admin admin = con.getAdmin()) {
            for (TableName tableName : admin.listTableNames()) {
                utility.get().deleteTableIfAny(tableName);
            }
        }
    }

    @Override
    default void clearDB(String tableName) throws Exception {
        utility.get().deleteTableIfAny(TableName.valueOf(tableName));
    }

    class TestMRExecutor implements MRExecutor {

        private static Configuration staticConfiguration;
        private final Configuration configuration;

        public TestMRExecutor() {
            this.configuration = new Configuration(staticConfiguration);
        }

        public TestMRExecutor(Configuration configuration) {
            this.configuration = configuration;
        }

        public static void setStaticConfiguration(Configuration staticConfiguration) {
            TestMRExecutor.staticConfiguration = staticConfiguration;
        }

        @Override
        public int run(String executable, String args) {
            try {
                // Copy configuration
                Configuration conf = new Configuration(false);
                HBaseConfiguration.merge(conf, configuration);
                if (executable.endsWith(ArchiveDriver.class.getName())) {
                    System.out.println("Executing ArchiveDriver : " + executable + " " + args);
                    int r = ArchiveDriver.privateMain(Commandline.translateCommandline(args), conf);
                    System.out.println("Finish execution ArchiveDriver");

                    return r;
                } else if (executable.endsWith(VariantTableDriver.class.getName())) {
                    System.out.println("Executing VariantTableDriver : " + executable + " " + args);
                    int r = VariantTableDriver.privateMain(Commandline.translateCommandline(args), conf, new VariantTableDriver(){
                        @Override
                        protected Class<? extends TableMapper> getMapperClass() {
                            return VariantTableMapperFail.class;
                        }
                    });
                    System.out.println("Finish execution VariantTableDriver");
                    return r;
                } else if (executable.endsWith(VariantTableDeletionDriver.class.getName())) {
                    System.out.println("Executing VariantTableDeletionDriver : " + executable + " " + args);
                    int r = VariantTableDeletionDriver.privateMain(Commandline.translateCommandline(args), conf);
                    System.out.println("Finish execution VariantTableDeletionDriver");
                    return r;
                }
            } catch (Exception e) {
                e.printStackTrace();
                return -1;
            }
            return 0;
        }
    }


    class VariantTableMapperFail extends VariantTableMapper {

        public static final String SLICE_TO_FAIL = "slice.to.fail";
        private String sliceToFail = "";
        private AtomicBoolean hadFail = new AtomicBoolean();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);

            hadFail.set(false);
            sliceToFail = context.getConfiguration().get(SLICE_TO_FAIL, sliceToFail);

        }

        @Override
        protected void doMap(VariantMapReduceContext ctx) throws IOException, InterruptedException {
            if (Bytes.toString(ctx.getCurrRowKey()).equals(sliceToFail)) {
                if (!hadFail.getAndSet(true)) {
                    System.out.println("DO FAIL!!");
                    ctx.getContext().getCounter(COUNTER_GROUP_NAME, "TEST.FAIL").increment(1);
                    throw new RuntimeException();
                }
            }
            super.doMap(ctx);
        }
    }

}
