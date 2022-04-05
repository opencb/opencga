package org.opencb.opencga.test.execution;

import java.util.List;

public interface DatasetExecutor {

    void execute(List<DatasetExecutionPlan> datasetPlanExecutionList);
}
