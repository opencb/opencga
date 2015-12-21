package org.opencb.opencga.storage.app.cli;

import org.opencb.datastore.core.Query;
import org.opencb.opencga.storage.core.StorageManagerFactory;
import org.opencb.opencga.storage.core.benchmark.BenchmarkManager;
import org.opencb.opencga.storage.core.variant.VariantStorageManager;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;

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
        /**
         * Create DBAdaptor
         */
        VariantStorageManager variantStorageManager = StorageManagerFactory.get().getVariantStorageManager(benchmarkCommandOptions.storageEngine);

        VariantDBAdaptor dbAdaptor = variantStorageManager.getDBAdaptor(benchmarkCommandOptions.storageEngine);

        benchmark();
    }


    private void benchmark() throws Exception {
        String load = benchmarkCommandOptions.load;
        String numOfRepetition = benchmarkCommandOptions.repetition;
        String tableName = benchmarkCommandOptions.tableName;
        String query  = benchmarkCommandOptions.query;
        boolean loadRequired;

        if (load != null) {
            loadRequired = true;
        } else {
            loadRequired = false;
        }
        String[] args = {load, numOfRepetition, tableName, query, Boolean.toString(loadRequired)};

        new BenchmarkManager().run(args);

    }

}
