package org.opencb.opencga.test.cli.executors;

import org.opencb.opencga.test.cli.options.RunCommandOptions;
import org.opencb.opencga.test.config.TestConfiguration;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

public class RunCommandExecutor extends Executor {


    public void execute() {
        try {
            File initialFile = new File(RunCommandOptions.configFile);
            InputStream confStream = new FileInputStream(initialFile);
            TestConfiguration.load(confStream);
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(-1);
        }
    }
}
