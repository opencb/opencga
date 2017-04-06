package org.opencb.opencga.storage.benchmark;

import org.opencb.opencga.storage.benchmark.variant.VariantBenchmarkRunner;
import org.opencb.opencga.storage.benchmark.variant.generators.GeneQueryGenerator;
import org.opencb.opencga.storage.benchmark.variant.generators.RegionQueryGenerator;
import org.opencb.opencga.storage.core.StorageEngineFactory;
import org.opencb.opencga.storage.core.config.StorageConfiguration;

import java.io.FileInputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;


public class Main {


    public static void main(String[] args) {
        try {
            privateMain(args);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    public static void privateMain(String[] args) throws Exception {

//        File jmeterHome = new File(System.getProperty("jmeter.home"));
        Path jmeterHome = Paths.get("/home/hpccoll1/opt/jmeter/");
        Path dataDir = Paths.get("/home/hpccoll1/appl/opencga/jmeter");
        Path applDir = Paths.get("/home/hpccoll1/opt/opencga_platinum");
        Path outdirPath = Paths.get("").toAbsolutePath();

        StorageConfiguration storageConfiguration;


        try (FileInputStream is = new FileInputStream(applDir.resolve("conf").resolve("storage-configuration.yml").toFile())) {
            storageConfiguration = StorageConfiguration.load(is);
            StorageEngineFactory.configure(storageConfiguration);
        }

        storageConfiguration.getBenchmark().setStorageEngine("mongodb");
        storageConfiguration.getBenchmark().setDatabaseName("opencga_jmeter_platinum_platinum");
        storageConfiguration.getBenchmark().setNumRepetitions(10);
        storageConfiguration.getBenchmark().setConcurrency(5);

        VariantBenchmarkRunner variantBenchmarkRunner = new VariantBenchmarkRunner(storageConfiguration, jmeterHome, outdirPath);
        variantBenchmarkRunner.addThreadGroup(VariantBenchmarkRunner.ConnectionType.REST, dataDir,
                Arrays.asList(GeneQueryGenerator.class, RegionQueryGenerator.class));
//        variantBenchmarkRunner.newThreadGroup(VariantBenchmarkRunner.ConnectionType.REST, dataDir,
//                Arrays.asList(RegionQueryGenerator.class));
//        variantBenchmarkRunner.newThreadGroup(VariantBenchmarkRunner.ConnectionType.REST, dataDir,
//                Arrays.asList(GeneQueryGenerator.class));
        variantBenchmarkRunner.run();
    }
}
