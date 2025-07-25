package org.opencb.opencga.catalog.utils;

import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.opencb.opencga.core.config.ExecutionQueue;
import org.opencb.opencga.core.models.job.MinimumRequirements;
import org.opencb.opencga.core.testclassification.duration.ShortTests;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@Category(ShortTests.class)
public class JobExecutionUtilsTest {

    @Test
    public void testFindOptimalQueuesBasicFiltering() {
        // Create test queues
        ExecutionQueue queue1 = new ExecutionQueue().setId("queue1").setType(ExecutionQueue.ExecutionType.CPU).setCpu(2).setMemory("4GB");
        ExecutionQueue queue2 = new ExecutionQueue().setId("queue2").setType(ExecutionQueue.ExecutionType.CPU).setCpu(4).setMemory("8GB");
        ExecutionQueue queue3 = new ExecutionQueue().setId("queue3").setType(ExecutionQueue.ExecutionType.GPU).setCpu(8).setMemory("16GB");

        List<ExecutionQueue> queues = Arrays.asList(queue1, queue2, queue3);

        // Test filtering by type and minimum requirements
        MinimumRequirements requirements = new MinimumRequirements().setType(ExecutionQueue.ExecutionType.CPU).setCpu("2").setMemory("4GB");

        List<ExecutionQueue> result = JobExecutionUtils.findOptimalQueues(queues, requirements);

        assertEquals(2, result.size());
        assertEquals("queue1", result.get(0).getId());
        assertEquals("queue2", result.get(1).getId());
    }

    @Test
    public void testFindOptimalQueuesSortingByCpu() {
        // Create queues with different CPU counts but same memory
        ExecutionQueue queue1 = new ExecutionQueue().setId("queue1").setType(ExecutionQueue.ExecutionType.CPU).setCpu(8).setMemory("8GB");
        ExecutionQueue queue2 = new ExecutionQueue().setId("queue2").setType(ExecutionQueue.ExecutionType.CPU).setCpu(2).setMemory("8GB");
        ExecutionQueue queue3 = new ExecutionQueue().setId("queue3").setType(ExecutionQueue.ExecutionType.CPU).setCpu(4).setMemory("8GB");

        List<ExecutionQueue> queues = Arrays.asList(queue1, queue2, queue3);

        MinimumRequirements requirements = new MinimumRequirements().setType(ExecutionQueue.ExecutionType.CPU).setCpu("2").setMemory("8GB");

        List<ExecutionQueue> result = JobExecutionUtils.findOptimalQueues(queues, requirements);

        assertEquals(3, result.size());
        // Should be sorted by CPU in ascending order
        assertEquals("queue2", result.get(0).getId()); // 2 CPUs
        assertEquals("queue3", result.get(1).getId()); // 4 CPUs
        assertEquals("queue1", result.get(2).getId()); // 8 CPUs
    }

    @Test
    public void testFindOptimalQueuesSortingByMemory() {
        // Create queues with same CPU but different memory
        ExecutionQueue queue1 = new ExecutionQueue().setId("queue1").setType(ExecutionQueue.ExecutionType.CPU).setCpu(4).setMemory("16GB");
        ExecutionQueue queue2 = new ExecutionQueue().setId("queue2").setType(ExecutionQueue.ExecutionType.CPU).setCpu(4).setMemory("4GB");
        ExecutionQueue queue3 = new ExecutionQueue().setId("queue3").setType(ExecutionQueue.ExecutionType.CPU).setCpu(4).setMemory("8GB");

        List<ExecutionQueue> queues = Arrays.asList(queue1, queue2, queue3);

        MinimumRequirements requirements = new MinimumRequirements().setType(ExecutionQueue.ExecutionType.CPU).setCpu("4").setMemory("4GB");

        List<ExecutionQueue> result = JobExecutionUtils.findOptimalQueues(queues, requirements);

        assertEquals(3, result.size());
        // Should be sorted by memory in ascending order when CPU is equal
        assertEquals("queue2", result.get(0).getId()); // 4GB
        assertEquals("queue3", result.get(1).getId()); // 8GB
        assertEquals("queue1", result.get(2).getId()); // 16GB
    }

    @Test
    public void testFindOptimalQueuesFiltersByMinimumCpu() {
        ExecutionQueue queue1 = new ExecutionQueue().setId("queue1").setType(ExecutionQueue.ExecutionType.CPU).setCpu(1).setMemory("8GB");
        ExecutionQueue queue2 = new ExecutionQueue().setId("queue2").setType(ExecutionQueue.ExecutionType.CPU).setCpu(4).setMemory("8GB");

        List<ExecutionQueue> queues = Arrays.asList(queue1, queue2);

        MinimumRequirements requirements = new MinimumRequirements().setType(ExecutionQueue.ExecutionType.CPU).setCpu("2").setMemory("8GB");

        List<ExecutionQueue> result = JobExecutionUtils.findOptimalQueues(queues, requirements);

        assertEquals(1, result.size());
        assertEquals("queue2", result.get(0).getId());
    }

    @Test
    public void testFindOptimalQueuesFiltersByMinimumMemory() {
        ExecutionQueue queue1 = new ExecutionQueue().setId("queue1").setType(ExecutionQueue.ExecutionType.CPU).setCpu(4).setMemory("2GB");
        ExecutionQueue queue2 = new ExecutionQueue().setId("queue2").setType(ExecutionQueue.ExecutionType.CPU).setCpu(4).setMemory("8GB");

        List<ExecutionQueue> queues = Arrays.asList(queue1, queue2);

        MinimumRequirements requirements = new MinimumRequirements().setType(ExecutionQueue.ExecutionType.CPU).setCpu("4").setMemory("4GB");

        List<ExecutionQueue> result = JobExecutionUtils.findOptimalQueues(queues, requirements);

        assertEquals(1, result.size());
        assertEquals("queue2", result.get(0).getId());
    }

    @Test
    public void testFindOptimalQueuesFiltersByType() {
        ExecutionQueue queue1 = new ExecutionQueue().setId("queue1").setType(ExecutionQueue.ExecutionType.GPU).setCpu(4).setMemory("8GB");
        ExecutionQueue queue2 = new ExecutionQueue().setId("queue2").setType(ExecutionQueue.ExecutionType.CPU).setCpu(4).setMemory("8GB");

        List<ExecutionQueue> queues = Arrays.asList(queue1, queue2);

        MinimumRequirements requirements = new MinimumRequirements().setType(ExecutionQueue.ExecutionType.CPU).setCpu("4").setMemory("8GB");

        List<ExecutionQueue> result = JobExecutionUtils.findOptimalQueues(queues, requirements);

        assertEquals(1, result.size());
        assertEquals("queue2", result.get(0).getId());
    }

    @Test
    public void testFindOptimalQueuesEmptyResult() {
        ExecutionQueue queue1 = new ExecutionQueue().setId("queue1").setType(ExecutionQueue.ExecutionType.CPU).setCpu(2).setMemory("4GB");

        List<ExecutionQueue> queues = Collections.singletonList(queue1);

        // Requirements that no queue can meet
        MinimumRequirements requirements = new MinimumRequirements().setType(ExecutionQueue.ExecutionType.CPU).setCpu("8").setMemory("16GB");

        List<ExecutionQueue> result = JobExecutionUtils.findOptimalQueues(queues, requirements);

        assertTrue(result.isEmpty());
    }

    @Test
    public void testFindOptimalQueuesEmptyInput() {
        List<ExecutionQueue> queues = Collections.emptyList();
        MinimumRequirements requirements = new MinimumRequirements().setType(ExecutionQueue.ExecutionType.CPU).setCpu("4").setMemory("8GB");

        List<ExecutionQueue> result = JobExecutionUtils.findOptimalQueues(queues, requirements);

        assertTrue(result.isEmpty());
    }
}