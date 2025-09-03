/*
 * Copyright 2015-2020 OpenCB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.opencb.opencga.master.monitor.executors;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.opencb.opencga.core.config.Configuration;
import org.opencb.opencga.core.config.Execution;
import org.opencb.opencga.core.config.ExecutionQueue;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Created by pfurio on 22/08/16.
 */
public class ExecutorFactory implements Closeable {

    private Map<String, String> queueToExecutorMap;
    private Map<String, BatchExecutor> executor;

    public ExecutorFactory(Configuration configuration) {
        this.queueToExecutorMap = new HashMap<>();
        this.executor = new HashMap<>();

        Set<ExecutionQueue> executorQueues = new HashSet<>();
        if (CollectionUtils.isNotEmpty(configuration.getAnalysis().getExecution().getQueues())) {
            for (ExecutionQueue queue : configuration.getAnalysis().getExecution().getQueues()) {
                if (StringUtils.isEmpty(queue.getId())) {
                    throw new IllegalArgumentException("Queue id cannot be null or empty");
                }
                if (StringUtils.isEmpty(queue.getExecutor())) {
                    throw new IllegalArgumentException("Queue " + queue.getId() + " does not have an associated executor");
                }
                if (queueToExecutorMap.containsKey(queue.getId())) {
                    throw new IllegalArgumentException("Queue " + queue.getId() + " is already defined");
                }
                queueToExecutorMap.put(queue.getId(), queue.getExecutor().toLowerCase());
                executorQueues.add(queue);
            }
        } else {
            queueToExecutorMap.put("default", "local");
            executorQueues.add(new ExecutionQueue().setId("local"));
        }

        Execution execution = configuration.getAnalysis().getExecution();
        for (ExecutionQueue executorQueue : executorQueues) {
            switch (executorQueue.getId()) {
                case "local":
                    LocalExecutor localExecutor = new LocalExecutor(execution);
                    this.executor.put("local", localExecutor);
                    break;
                case "sge":
                    SGEExecutor sgeExecutor = new SGEExecutor(execution);
                    this.executor.put("sge", sgeExecutor);
                    break;
//            case "azure":
//            case "azure-batch":
//                this.executor = new AzureBatchExecutor(execution);
//                break;
                case "k8s":
                case "kubernetes":
                    K8SExecutor k8SExecutor = new K8SExecutor(configuration, executorQueue);
                    this.executor.put(executorQueue.getId(), k8SExecutor);
                    break;
                default:
                    throw new UnsupportedOperationException("Unsupported execution mode { " + executorQueue.getId()
                            + " }, accepted modes are : local, sge, k8s, kubernetes");
            }
        }
    }

    public BatchExecutor getExecutor(String queueId) {
        if (!queueToExecutorMap.containsKey(queueId)) {
            throw new IllegalArgumentException("Queue " + queueId + " does not have an associated executor");
        }
        String executorId = queueToExecutorMap.get(queueId);
        if (!executor.containsKey(executorId)) {
            throw new IllegalArgumentException("Executor " + executorId + " does not exist");
        }
        return executor.get(executorId);
    }

    @Override
    public void close() throws IOException {
        for (BatchExecutor executor : this.executor.values()) {
            executor.close();
        }
    }
}
