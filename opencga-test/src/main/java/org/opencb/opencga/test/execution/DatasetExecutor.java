package org.opencb.opencga.test.execution;

import org.opencb.opencga.test.execution.models.DatasetExecutionPlan;

import java.util.List;

public interface DatasetExecutor {

    void execute(List<DatasetExecutionPlan> datasetPlanExecutionList);


}
