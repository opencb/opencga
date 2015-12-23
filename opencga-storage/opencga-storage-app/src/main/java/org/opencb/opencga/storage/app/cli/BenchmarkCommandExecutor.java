package org.opencb.opencga.storage.app.cli;

import com.beust.jcommander.ParameterException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.opencb.opencga.storage.core.StorageManagerFactory;
import org.opencb.opencga.storage.core.benchmark.BenchmarkManager;
import org.opencb.opencga.storage.core.config.DatabaseCredentials;
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

        // Overwrite default options from configuration.yaml with CLI parameters
        if (benchmarkCommandOptions.storageEngine != null && !benchmarkCommandOptions.storageEngine.isEmpty()) {
            configuration.getBenchmark().setStorageEngine(benchmarkCommandOptions.storageEngine);
        } else {
            configuration.getBenchmark().setStorageEngine(configuration.getDefaultStorageEngineId());
            logger.debug("Storage Engine for benchmarking set to '{}'", configuration.getDefaultStorageEngineId());
        }

        if (benchmarkCommandOptions.repetition > 0) {
            configuration.getBenchmark().setNumRepetitions(benchmarkCommandOptions.repetition);
        }

        if (benchmarkCommandOptions.database != null && !benchmarkCommandOptions.database.isEmpty()) {
            configuration.getBenchmark().setDatabaseName(benchmarkCommandOptions.database);
        }

        if (benchmarkCommandOptions.table != null && !benchmarkCommandOptions.table.isEmpty()) {
            configuration.getBenchmark().setTable(benchmarkCommandOptions.table);
        }

        if (benchmarkCommandOptions.queries != null) {
            configuration.getBenchmark().setQueries(Arrays.asList(benchmarkCommandOptions.queries.split(",")));
        }

        DatabaseCredentials databaseCredentials = configuration.getBenchmark().getDatabase();
        if (benchmarkCommandOptions.host != null && !benchmarkCommandOptions.host.isEmpty()) {
            databaseCredentials.setHosts(Arrays.asList(benchmarkCommandOptions.host.split(",")));
        }

        logger.debug("Benchmark configuration: {}", configuration.getBenchmark());

        // validate
        checkParams();


//        VariantDBAdaptor dbAdaptor = variantStorageManager.getDBAdaptor(benchmarkCommandOptions.storageEngine);
        BenchmarkManager benchmarkManager = new BenchmarkManager(configuration);
        benchmarkManager.variantBenchmark();
    }

    private void checkParams() {
        if (configuration.getBenchmark().getDatabaseName() == null || configuration.getBenchmark().getDatabaseName().isEmpty()) {
            System.out.println("...");
            throw new ParameterException("");
        }

    }

}
