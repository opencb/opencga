package org.opencb.opencga.test.cli.executors;

import org.opencb.commons.utils.PrintUtils;
import org.opencb.opencga.test.cli.options.DatasetCommandOptions;
import org.opencb.opencga.test.config.Configuration;
import org.opencb.opencga.test.config.Environment;
import org.opencb.opencga.test.config.OpencgaTestConfiguration;
import org.opencb.opencga.test.manager.DatasetPlanExecutionGenerator;
import org.opencb.opencga.test.utils.DatasetTestUtils;
import org.opencb.opencga.test.utils.OpencgaLogger;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;

public class DatasetCommandExecutor extends Executor {


    @Override
    public void execute() {
        try {
            File initialFile = new File(DatasetCommandOptions.configFile);
            InputStream confStream = new FileInputStream(initialFile);
            Configuration configuration = OpencgaTestConfiguration.load(confStream);
            DatasetPlanExecutionGenerator datasetGenerator = new DatasetPlanExecutionGenerator(configuration);
            if (!checkInputParams(configuration)) {
                PrintUtils.println("Configuration validation: ", PrintUtils.Color.CYAN, "FAIL", PrintUtils.Color.RED);
                System.exit(-1);
            }
            PrintUtils.println("Configuration validation: ", PrintUtils.Color.CYAN, "OK", PrintUtils.Color.WHITE);
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

    @Override
    public boolean checkInputParams(Configuration configuration) {
        if (DatasetCommandOptions.simulate) {
            return validateSimulateConfiguration(configuration);
        } else if (DatasetCommandOptions.run) {
            return validateExecutionConfiguration(configuration);
        } else if (DatasetCommandOptions.resume) {
            return validateExecutionConfiguration(configuration);
        } else {
            PrintUtils.println("Configuration validation: ", PrintUtils.Color.CYAN, "FAIL", PrintUtils.Color.RED);
            PrintUtils.println("Execute option not found, valid options are --run --simulate --resume", PrintUtils.Color.CYAN);
            System.exit(-1);
        }
        return false;
    }

    private boolean validateSimulateConfiguration(Configuration configuration) {
        boolean res = checkReference(configuration);
        for (Environment env : configuration.getEnvs()) {
            res = isValidInputDirs(res, env);
        }
        return res;
    }


    private boolean validateExecutionConfiguration(Configuration configuration) {
        boolean res = checkReference(configuration);
        for (Environment env : configuration.getEnvs()) {
            res = isValidInputDirs(res, env);
            res = checkDirectory(DatasetTestUtils.getInputTemplatesDirPath(env)) && res;
        }
        return res;
    }

    private boolean isValidInputDirs(boolean res, Environment env) {
        res = checkDirectory(DatasetTestUtils.getInputEnvironmentDirPath(env)) && res;
        if (!env.getAligner().isSkip() && !DatasetTestUtils.areSkippedAllCallers(env.getCallers())) {
            res = checkDirectory(DatasetTestUtils.getInputFastqDirPath(env)) && res;
        } else if (env.getAligner().isSkip() && !DatasetTestUtils.areSkippedAllCallers(env.getCallers())) {
            res = checkDirectory(DatasetTestUtils.getInputBamDirPath(env)) && res;
        } else if (DatasetTestUtils.areSkippedAllCallers(env.getCallers())) {
            res = checkDirectory(DatasetTestUtils.getInputVCFDirPath(env)) && res;
        }
        return res;
    }

    private boolean checkReference(Configuration configuration) {
        for (Environment env : configuration.getEnvs()) {
            if (!checkDirectory(env.getReference().getIndex())) {
                PrintUtils.printError("The index must be present.");
                return false;
            }
        }
        return true;
    }

    private boolean checkDirectory(String dir) {
        if (!Files.exists(Paths.get(dir))) {
            PrintUtils.println("Volume " + PrintUtils.format(dir, PrintUtils.Color.RED) + " is not present.");
            return false;
        }
        return true;
    }

}
