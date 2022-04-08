package org.opencb.opencga.test.cli.executors;

import org.opencb.commons.utils.PrintUtils;
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
            File initialFile = new File(DatasetCommandOptions.configFile);
            InputStream confStream = new FileInputStream(initialFile);
            Configuration configuration = OpencgaTestConfiguration.load(confStream);
            DatasetPlanExecutionGenerator datasetGenerator = new DatasetPlanExecutionGenerator(configuration);
            if (DatasetCommandOptions.simulate) {
                PrintUtils.println("Simulate option found ", PrintUtils.Color.CYAN);
                datasetGenerator.simulate();
            } else if (DatasetCommandOptions.run) {
                PrintUtils.println("Run option found ", PrintUtils.Color.CYAN);
                datasetGenerator.execute();
            } else if (DatasetCommandOptions.resume) {
                PrintUtils.println("Execute option found ", PrintUtils.Color.CYAN);
                datasetGenerator.resume();
            }
        } catch (IOException e) {
            OpencgaLogger.printLog(e.getMessage(), e);
            System.exit(-1);
        }
    }
}
