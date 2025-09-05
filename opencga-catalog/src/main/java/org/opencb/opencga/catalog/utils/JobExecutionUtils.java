package org.opencb.opencga.catalog.utils;

import org.opencb.opencga.core.common.IOUtils;
import org.opencb.opencga.core.config.ExecutionQueue;
import org.opencb.opencga.core.models.job.MinimumRequirements;

import java.util.List;
import java.util.stream.Collectors;

public class JobExecutionUtils {

    public static List<ExecutionQueue> findOptimalQueues(List<ExecutionQueue> queues, MinimumRequirements requirements) {
        // Sort the queues based on their number of CPUs and memory
        // Also filter out queues that do not meet the minimum requirements
        return queues.stream()
                .filter(queue -> queue.getProcessorType().equals(requirements.getProcessorType())
                        && Integer.parseInt(queue.getCpu()) >= Integer.parseInt(requirements.getCpu())
                        && IOUtils.fromHumanReadableToByte(queue.getMemory()) >= IOUtils.fromHumanReadableToByte(requirements.getMemory()))
                .sorted((q1, q2) -> {
                    int cpuComparison = Integer.compare(Integer.parseInt(q1.getCpu()), Integer.parseInt(q2.getCpu()));
                    if (cpuComparison != 0) {
                        return cpuComparison;
                    }
                    return Long.compare(IOUtils.fromHumanReadableToByte(q1.getMemory()), IOUtils.fromHumanReadableToByte(q2.getMemory()));
                })
                .collect(Collectors.toList());
    }

}
