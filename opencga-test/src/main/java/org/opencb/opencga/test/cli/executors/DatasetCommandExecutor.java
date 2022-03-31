package org.opencb.opencga.test.cli.executors;

import org.opencb.opencga.test.cli.options.DatasetCommandOptions;
import org.opencb.opencga.test.config.Configuration;
import org.opencb.opencga.test.config.OpencgaTestConfiguration;
import org.opencb.opencga.test.manager.DatasetPlanExecutionGenerator;
import org.opencb.opencga.test.utils.OpencgaLogger;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

public class DatasetCommandExecutor extends Executor {


    public void execute() {
        try {
            System.out.println(DatasetCommandOptions.configFile);
            File initialFile = new File(DatasetCommandOptions.configFile);
            InputStream confStream = new FileInputStream(initialFile);
            Configuration configuration = OpencgaTestConfiguration.load(confStream);
            DatasetPlanExecutionGenerator datasetGenerator = new DatasetPlanExecutionGenerator(configuration);
            if (DatasetCommandOptions.simulate) {
                datasetGenerator.simulate();
            } else if (DatasetCommandOptions.run) {
                datasetGenerator.execute();
            }
        } catch (IOException e) {
            OpencgaLogger.printLog(e.getMessage(), e);
            System.exit(-1);
        }
    }
}
