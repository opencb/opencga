/*
 * Copyright 2015 OpenCB
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

package org.opencb.opencga.storage.core.runner;

import org.opencb.commons.io.DataReader;
import org.opencb.commons.io.DataWriter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;

/**
 * Created by jacobo on 5/02/15.
 */
@Deprecated
public class ThreadRunner {

    private ExecutorService executorService;

    private List<ReadNode> readNodes = new LinkedList<>();
    private List<Node> taskNodes = new LinkedList<>();
    private List<WriterNode> writerNodes = new LinkedList<>();
    private List<Node> nodes = new LinkedList<>();
    private final int batchSize;

    private final Object syncObject = new Object();
    private static final List<Object> SINGLETON_LIST = Collections.singletonList(new Object());
    private static final List POISON_PILL = new LinkedList();

    public ThreadRunner(ExecutorService executorService, int batchSize) {
        this.executorService = executorService;
        this.batchSize = batchSize;
    }

    public <I, O> TaskNode<I, O> newTaskNode(List<Task<I, O>> tasks) {
        TaskNode<I, O> taskNode = new TaskNode<>(tasks, "task-node-" + taskNodes.size());
        taskNodes.add(taskNode);
        return taskNode;
    }

    public <T> SimpleTaskNode<T> newSimpleTaskNode(org.opencb.commons.run.Task<T> task, int n) {
        List<org.opencb.commons.run.Task<T>> tasks = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            tasks.add(task);
        }
        SimpleTaskNode<T> taskNode = new SimpleTaskNode<>(tasks, "task-node-" + taskNodes.size());
        taskNodes.add(taskNode);
        return taskNode;
    }

    public <T> SimpleTaskNode<T> newSimpleTaskNode(List<org.opencb.commons.run.Task<T>> tasks) {
        SimpleTaskNode<T> taskNode = new SimpleTaskNode<>(tasks, "task-node-" + taskNodes.size());
        taskNodes.add(taskNode);
        return taskNode;
    }

    public <I, O> TaskNode<I, O> newTaskNode(Task<I, O> task, int n) {
        List<Task<I, O>> tasks = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            tasks.add(task);
        }
        TaskNode<I, O> taskNode = new TaskNode<>(tasks, "task-node-" + taskNodes.size());
        taskNodes.add(taskNode);
        return taskNode;
    }

    public <O> ReadNode<O> newReaderNode(List<DataReader<O>> readers) {
        ReadNode<O> readNode = new ReadNode<>(readers, "reader-node-" + readNodes.size());
        readNodes.add(readNode);
        return readNode;
    }

    public <O> ReadNode<O> newReaderNode(DataReader<O> reader, int n) {
        List<DataReader<O>> readers = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            readers.add(reader);
        }
        ReadNode<O> readNode = new ReadNode<>(readers, "reader-node-" + readNodes.size());
        readNodes.add(readNode);
        return readNode;
    }

    public <I> WriterNode<I> newWriterNode(List<DataWriter<I>> writers) {
        WriterNode<I> writerNode = new WriterNode<>(writers, "writer-node-" + writerNodes.size());
        writerNodes.add(writerNode);
        return writerNode;
    }

    public <I> WriterNode<I> newWriterNode(DataWriter<I> writer, int n) {
        List<DataWriter<I>> writers = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            writers.add(writer);
        }
        WriterNode<I> writerNode = new WriterNode<>(writers, "writer-node-" + writerNodes.size());
        writerNodes.add(writerNode);
        return writerNode;
    }

    public void run() {
        start();
        join();
    }

    public void start() {
        nodes.addAll(readNodes);
        nodes.addAll(taskNodes);
        nodes.addAll(writerNodes);

        for (Node node : nodes) {
            node.init();
            node.pre();
        }

        for (ReadNode readNode : readNodes) {
            readNode.start();
        }
    }

    public void join() {
        boolean allFinalized;
        synchronized (syncObject) {
            do {
                allFinalized = true;
                for (Node node : nodes) {
                    if (!node.isFinished()) {
                        System.out.println("Node " + node.name + " is not finished pending:" + node.pendingJobs + " lastBatch:" + node
                                .lastBatch);
                        allFinalized = false;
                        break;
                    } /*else {
                        System.out.println("Node " + node.name + " is finished");
                    }*/
                }
                if (!allFinalized) {
                    try {
                        System.out.println("WAIT");
                        syncObject.wait();
                        System.out.println("NOTIFY");
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            } while (!allFinalized);
        }

        for (Node node : nodes) {
            node.post();
        }

        executorService.shutdown();
    }

    public abstract class Task<I, O> {
        public boolean pre() {
            return true;
        }

        public abstract List<O> apply(List<I> batch) throws IOException;

        public boolean post() {
            return true;
        }
    }

    public final class ReadNode<O> extends Node<Object, O, DataReader<O>> {
        private ReadNode(List<DataReader<O>> tasks, String name) {
            super(tasks, name);
        }

        public void start() {
            submit(SINGLETON_LIST);
        }

        @Override
        List<O> doJob(List<Object> b) {
//            System.out.println(name + " - read start - " );
            List<O> reddenBatch = super.doJob(b);
            if (reddenBatch != null) {
//                System.out.println(name + " - read end - " + reddenBatch.size());
                if (!reddenBatch.isEmpty()) {
//                    System.out.println(name + " - non empty list! - " + reddenBatch.size());
                    start();
                } else {
//                    System.out.println("Empty list! Lets submit the last batch " + !isLastBatchSent());
                    if (!isLastBatchSent()) {
                        submit(POISON_PILL);
                    }
                }
            } else {
//                System.out.println(name + " - read end NULL taskQueue.size : " + taskQueue.size());
                System.out.println("Empty block");
            }
            return reddenBatch;
        }

        @Override
        protected List<O> execute(DataReader<O> reader, List<Object> ignored) {
            List<O> read = reader.read(batchSize);
            return read;
        }

        @Override
        protected void pre() {
            for (DataReader<O> reader : tasks) {
                reader.open();
                reader.pre();
            }
        }

        @Override
        protected void post() {
            for (DataReader<O> reader : tasks) {
                reader.post();
                reader.close();
            }
        }
    }

    public final class TaskNode<I, O> extends Node<I, O, Task<I, O>> {
        private TaskNode(List<Task<I, O>> tasks, String name) {
            super(tasks, name);
        }

        @Override
        protected List<O> execute(Task<I, O> task, List<I> batch) {
            try {
                return task.apply(batch);
            } catch (IOException e) {
                e.printStackTrace();
            }
            return Collections.emptyList();
        }

        @Override
        protected void pre() {
            for (Task<I, O> task : tasks) {
                task.pre();
            }
        }

        @Override
        protected void post() {
            for (Task<I, O> task : tasks) {
                task.post();
            }
        }
    }

    public final class SimpleTaskNode<I> extends Node<I, I, org.opencb.commons.run.Task<I>> {
        private SimpleTaskNode(List<org.opencb.commons.run.Task<I>> tasks, String name) {
            super(tasks, name);
        }

        @Override
        protected void pre() {
            for (org.opencb.commons.run.Task<I> task : tasks) {
                task.pre();
            }
        }

        @Override
        protected void post() {
            for (org.opencb.commons.run.Task<I> task : tasks) {
                task.post();
            }
        }

        @Override
        protected List<I> execute(org.opencb.commons.run.Task<I> task, List<I> batch) {
            try {
                task.apply(batch);
            } catch (IOException e) {
                e.printStackTrace();
            }
            return batch;
        }
    }

    public final class WriterNode<I> extends Node<I, Object, DataWriter<I>> {
        private WriterNode(List<DataWriter<I>> tasks, String name) {
            super(tasks, name);
        }

        @Override
        protected void pre() {
            for (DataWriter<I> writer : tasks) {
                writer.open();
                writer.pre();
            }
        }

        @Override
        protected void post() {
            for (DataWriter<I> writer : tasks) {
                writer.post();
                writer.close();
            }
        }

        @Override
        protected List<Object> execute(DataWriter<I> writer, List<I> batch) {
            writer.write(batch);
            return SINGLETON_LIST;
        }
    }

    abstract class Node<I, O, EXECUTOR> {
        protected final List<EXECUTOR> tasks;
        protected final String name;
        private final BlockingQueue<EXECUTOR> taskQueue;
        private List<Node<O, ?, ?>> nodes;
        private int pendingJobs;
        private boolean lastBatch;
        private boolean lastBatchSent;

        public Node(List<EXECUTOR> tasks, String name) {
            this.tasks = tasks;
            this.name = name;
            taskQueue = new ArrayBlockingQueue<>(tasks.size(), false, tasks);
            nodes = new LinkedList<>();
        }

        /* package */ void init() {
            pendingJobs = 0;
            lastBatch = false;
            lastBatchSent = false;
        }

        protected abstract void pre();

        protected abstract void post();

        /*package*/ List<O> doJob(List<I> batch) {
            List<O> generatedBatch;
            assert !lastBatchSent;

            if (batch == POISON_PILL) {
                lastBatch = true;
                synchronized (name) {
                    pendingJobs--;
                }
//                System.out.println(name + " - lastBatch");
                generatedBatch = Collections.emptyList();
            } else {

                EXECUTOR task = taskQueue.poll();

                boolean nextNodesAvailable = true;
                for (Node<O, ?, ?> node : nodes) {
                    nextNodesAvailable &= node.isAvailable();
                }

                if (task == null) { //No available task
                    resubmit(batch);
                    generatedBatch = null;
                } else if (!nextNodesAvailable) {   //Next nodes have to many batches.
                    try {
                        taskQueue.put(task);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    resubmit(batch);
                    generatedBatch = null;
                } else {    //Execute

                    generatedBatch = execute(task, batch);
//                    System.out.println(name + " - end job - " + generatedBatch.size());
                    for (Node<O, ?, ?> node : nodes) {
                        node.submit(generatedBatch);
                    }

                    try {
                        taskQueue.put(task);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    synchronized (name) {
                        pendingJobs--;
                    }
//                    System.out.println(name + " - pendingJobs " + pendingJobs);
                }
            }

            if (isFinished()) {
                if (!lastBatchSent) {
                    for (Node<O, ?, ?> node : nodes) {
                        node.submit(POISON_PILL);
                    }
                    lastBatchSent = true;
                }
                System.out.println("Node '" + name + "' is finished");
                synchronized (syncObject) {
                    syncObject.notify();
                }
            } else {
                System.out.println("Node '" + name + "' pendingJobs " + pendingJobs);
            }
            return generatedBatch;
        }

        protected abstract List<O> execute(EXECUTOR task, List<I> batch);

        private void resubmit(final List<I> batch) {
            executorService.submit(new Runnable() {
                public void run() {
                    doJob(batch);
                }
            });
        }

        /*package*/ void submit(final List<I> batch) {
//            System.out.println("Submitting batch: pendingJobs = " + pendingJobs + " - " + "[" + (isAvailable()? " " : "*") + "]" + name
// + " - " + Thread.currentThread().getName());
            pendingJobs++;
            resubmit(batch);
        }

        public boolean isAvailable() {
            return pendingJobs < tasks.size();
        }

        public boolean isFinished() {
            return pendingJobs == 0 && lastBatch;
        }

        public boolean isLastBatchSent() {
            return lastBatchSent;
        }

        public Node<I, O, EXECUTOR> append(Node<O, ?, ?> node) {
            nodes.add(node);
            return this;
        }

    }

}
