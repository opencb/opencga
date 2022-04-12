package org.opencb.opencga.test.cli.executors;

import org.opencb.opencga.test.config.Configuration;

public abstract class Executor {

    abstract public void execute();

    abstract public boolean checkInputParams(Configuration configuration);


}
