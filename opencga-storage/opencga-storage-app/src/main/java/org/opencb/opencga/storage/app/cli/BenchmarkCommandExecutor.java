package org.opencb.opencga.storage.app.cli;

import org.opencb.opencga.storage.core.StorageManagerFactory;
import org.opencb.opencga.storage.core.benchmark.BenchmarkManager;
import org.opencb.opencga.storage.core.config.BenchmarkConfiguration;
import org.opencb.opencga.storage.core.config.StorageEngineConfiguration;
import org.opencb.opencga.storage.core.variant.VariantStorageManager;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;

import java.util.Arrays;

/**
 * Created by pawan on 06/11/15.
 */
public class BenchmarkCommandExecutor extends CommandExecutor {

    private CliOptionsParser.BenchmarkCommandOptions benchmarkCommandOptions;


    public BenchmarkCommandExecutor(CliOptionsParser.BenchmarkCommandOptions benchmarkCommandOptions) {
        super(benchmarkCommandOptions.logLevel, benchmarkCommandOptions.verbose,
                benchmarkCommandOptions.configFile);

        this.logFile = benchmarkCommandOptions.logFile;
        this.benchmarkCommandOptions = benchmarkCommandOptions;
    }


    @Override
    public void execute() throws Exception {

        String storageEngine = (benchmarkCommandOptions.storageEngine != null && !benchmarkCommandOptions.storageEngine.isEmpty())
                ? benchmarkCommandOptions.storageEngine
                : configuration.getDefaultStorageEngineId();
        logger.debug("Storage Engine set to '{}'", storageEngine);


        StorageEngineConfiguration storageConfiguration = configuration.getStorageEngine(storageEngine);

        StorageManagerFactory storageManagerFactory = new StorageManagerFactory(configuration);
        VariantStorageManager variantStorageManager;
        if (storageEngine == null || storageEngine.isEmpty()) {
            variantStorageManager = storageManagerFactory.getVariantStorageManager();
        } else {
            variantStorageManager = storageManagerFactory.getVariantStorageManager(storageEngine);
        }
        storageConfiguration.getVariant().getOptions().putAll(benchmarkCommandOptions.params);


        // Overwrite default options from configuration.yaml with CLI parameters
        if (benchmarkCommandOptions.storageEngine != null) {
            configuration.getBenchmark().setStorageEngines(Arrays.asList(benchmarkCommandOptions.storageEngine.split(",")));
        }

        if (benchmarkCommandOptions.tableName != null) {
            configuration.getBenchmark().setTables(Arrays.asList(benchmarkCommandOptions.tableName.split(",")));
        }

        if (benchmarkCommandOptions.queries != null) {
            configuration.getBenchmark().setQueries(Arrays.asList(benchmarkCommandOptions.queries.split(",")));
        }
        configuration.getBenchmark().setNumRepetitions(benchmarkCommandOptions.repetition);
//        configuration.getBenchmark().setQueries(Arrays.asList(benchmarkCommandOptions.queries.split(",")));
//        configuration.getBenchmark().setTables(Arrays.asList(benchmarkCommandOptions.tableName.split(",")));
        System.out.println(configuration.getBenchmark());


        VariantDBAdaptor dbAdaptor = variantStorageManager.getDBAdaptor(benchmarkCommandOptions.storageEngine);

//        benchmark();
        BenchmarkManager benchmarkManager = new BenchmarkManager(configuration);
        benchmarkManager.variantBenchmark();

    }


//    private void benchmark() throws Exception {
//        String load = benchmarkCommandOptions.load;
//        int numOfRepetition = benchmarkCommandOptions.repetition;
//        String tableName = benchmarkCommandOptions.tableName;
//        String query  = benchmarkCommandOptions.query;
//        boolean loadRequired;
//
//        if (load != null) {
//            loadRequired = true;
//        } else {
//            loadRequired = false;
//        }
//        String[] args = {load, "" + numOfRepetition, tableName, query, Boolean.toString(loadRequired)};
//
//        new BenchmarkManager().run(args);
//
//    }

}
