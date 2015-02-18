package org.opencb.opencga.storage.core;

        import java.util.*;
        import java.util.concurrent.*;

/**
 * Created by jacobo on 5/02/15.
 */
public class ThreadRunner {
    private ExecutorService executorService;

    private List<ReadNode> readNodes = new LinkedList<>();
    private List<TaskNode> taskNodes = new LinkedList<>();
    private List<WriterNode> writerNodes = new LinkedList<>();
    private List<Node> nodes = new LinkedList<>();
    private final Object syncObject = new Object();

    private static final List<Object> SINGLETON_LIST = Collections.singletonList(new Object());
    private static final List LAST_BATCH = new LinkedList();

    public ThreadRunner(ExecutorService executorService) {
        this.executorService = executorService;
    }

    public <I,O> TaskNode<I,O> newTaskNode(List<Task<I,O>> tasks) {
        TaskNode<I, O> taskNode = new TaskNode<>(tasks, "task-node-" + taskNodes.size());
        taskNodes.add(taskNode);
        return taskNode;
    }

    public <I,O> TaskNode<I,O> newTaskNode(Task<I,O> task, int n) {
        List<Task<I,O>> tasks = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            tasks.add(task);
        }
        TaskNode<I, O> taskNode = new TaskNode<>(tasks, "task-node-" + taskNodes.size());
        taskNodes.add(taskNode);
        return taskNode;
    }

    public <O> ReadNode<O> newReaderNode(List<Reader<O>> readers) {
        List<Task<Object, O>> tasks = new ArrayList<>(readers.size());
        for (Reader<O> r : readers) {
            tasks.add(r);
        }
        ReadNode<O> readNode = new ReadNode<>(tasks, "reader-node-" + readNodes.size());
        readNodes.add(readNode);
        return readNode;
    }

    public <O> ReadNode<O> newReaderNode(Reader<O> reader, int n) {
        List<Task<Object, O>> tasks = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            tasks.add(reader);
        }
        ReadNode<O> readNode = new ReadNode<>(tasks, "reader-node-" + readNodes.size());
        readNodes.add(readNode);
        return readNode;
    }

    public <I> WriterNode<I> newWriterNode(List<Writer<I>> writers) {
        List<Task<I, Object>> tasks = new ArrayList<>(writers.size());
        for (Writer<I> w : writers) {
            tasks.add(w);
        }
        WriterNode<I> writerNode = new WriterNode<>(tasks, "writer-node-" + writerNodes.size());
        writerNodes.add(writerNode);
        return writerNode;
    }

    public <I> WriterNode<I> newWriterNode(Writer<I> writer, int n) {
        List<Task<I, Object>> tasks = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            tasks.add(writer);
        }
        WriterNode<I> writerNode = new WriterNode<>(tasks, "writer-node-" + writerNodes.size());
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
                        System.out.println("Node " + node.name + " is not finished pending:" + node.pendingJobs + " lastBatch:" + node.lastBatch);
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

    public static abstract class Task<I, O> /*extends org.opencb.commons.run.Task<I>*/ {
        public boolean pre() {return true;}
//        public abstract boolean apply(List<I> batch);
        public boolean post() {return true;}

        public abstract List<O> process(List<I> batch);
    }

    public static abstract class Reader<O> extends Task<Object, O> /*implements DataReader<O>*/ {
        public boolean open() {return true;}
        @Override public boolean pre() {return true;}
        public abstract List<O> read();
        @Override public boolean post() {return true;}
        public boolean close() {return true;}

        public final List<O> process(List<Object> batch) {
            return read();
        }
    }

    public static abstract class Writer<I> extends Task<I, Object> /*implements DataWriter<I>*/ {
        public boolean open() {return true;}
        @Override public boolean pre() {return true;}
        public abstract boolean write(List<I> batch);
        @Override public boolean post() {return true;}
        public boolean close() {return true;}

        public final List<Object> process(List<I> batch) {
            write(batch);
            return Collections.emptyList();
        }
    }

    public class ReadNode<O> extends Node<Object, O> {
        private ReadNode(List<Task<Object, O>> tasks, String name) {
            super(tasks, name);
        }

        public void start() {
            submit(SINGLETON_LIST);
        }

        @Override
        List<O> doJob(List<Object> batch) {
            List<O> oList = super.doJob(batch);
            if (oList != null)
                if (!oList.isEmpty()) {
                    start();
                } else {
                    if (!lastBatchSended) {
                        submit(LAST_BATCH);
                    }
//                    lastBatch = true;
//                    System.out.println("Reader '" + name + "' has finished " + isFinished());
//                    synchronized (syncObject) {
//                        syncObject.notify();
//                    }
                }
            return oList;
        }
    }


    protected class TaskNode<I, O> extends Node<I,O> {
        private TaskNode(List<Task<I, O>> tasks, String name) {
            super(tasks, name);
        }
    }

    protected class WriterNode<I> extends Node<I,Object> {
        private WriterNode(List<Task<I, Object>> tasks, String name) {
            super(tasks, name);
        }
    }

    abstract class Node<I, O> {
        private final List<Task<I, O>> tasks;
        private final BlockingQueue<Task<I,O>> taskQueue;
        private List<Node<O, ?>> nodes;
        private int pendingJobs;
        protected final String name;
        protected boolean lastBatch;
        protected boolean lastBatchSended;

        public Node(List<Task<I, O>> tasks, String name) {
            this.tasks = tasks;
            this.name = name;
            taskQueue = new ArrayBlockingQueue<>(tasks.size(), false, tasks);
            nodes = new LinkedList<>();
        }

        /*package*/ void pre () {
            pendingJobs = 0;
            lastBatch = false;
            lastBatchSended = false;
            for (Task<I, O> task : tasks) {
                task.pre();
            }
        }

        /*package*/ void post () {
            for (Task<I, O> task : tasks) {
                task.post();
            }
        }

        /*package*/ List<O> doJob(List<I> batch) {
            List<O> generatedBatch;
            assert lastBatchSended == false;
            if (batch == LAST_BATCH) {
                lastBatch = true;
                pendingJobs--;
                System.out.println(name + " - lastBatch");
                generatedBatch = Collections.emptyList();
            } else {

                Task<I, O> task = null;
                task = taskQueue.poll();

                boolean nextNodesAvailable = true;
                for (Node<O, ?> node : nodes) {
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
                } else {

                    generatedBatch = task.process(batch);
                    System.out.println(name + " - end job");
                    for (Node<O, ?> node : nodes) {
                        node.submit(generatedBatch);
                    }

                    try {
                        taskQueue.put(task);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    pendingJobs--;  //
//                    System.out.println(name + " - pendingJobs " + pendingJobs);
                }
            }
            if (isFinished()) {
                if (!lastBatchSended) {
                    generatedBatch = LAST_BATCH;
                    for (Node<O, ?> node : nodes) {
                        node.submit(LAST_BATCH);
                    }
                    lastBatchSended = true;
                }
                System.out.println("Node '" + name + "' is finished");
                synchronized (syncObject) {
                    syncObject.notify();
                }
            }
            return generatedBatch;
        }

        private void resubmit(final List<I> batch) {
            executorService.submit(new Runnable() {
                public void run() {
                    doJob(batch);
                }
            });
        }

        /*package*/ void submit(final List<I> batch) {
            System.out.println("Submitting batch: pendingJobs = " + pendingJobs + " - " + "[" + (isAvailable()? " " : "*") + "]" + name + " - " + Thread.currentThread().getName());
            pendingJobs ++;
            resubmit(batch);
        }

        public boolean isAvailable() {
            return pendingJobs < tasks.size();
        }

        public boolean isFinished() {
            return pendingJobs == 0 && lastBatch;
        }

        public void append(Node<O, ?> node) {
            nodes.add(node);
        }

    }

}
