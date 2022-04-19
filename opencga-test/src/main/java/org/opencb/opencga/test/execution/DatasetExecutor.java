package org.opencb.opencga.test.execution;

import org.opencb.opencga.test.config.Configuration;
import org.opencb.opencga.test.execution.models.DatasetExecutionPlan;

import java.util.List;

public abstract class DatasetExecutor {

    protected Configuration configuration;

    public DatasetExecutor(Configuration configuration) {
        this.configuration = configuration;
    }

    abstract void execute(List<DatasetExecutionPlan> datasetPlanExecutionList);


}
