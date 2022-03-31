package org.opencb.opencga.test.plan;

import java.util.List;

public interface PlanExecutor {

    void execute(List<DatasetPlanExecution> datasetPlanExecutionList);
}
