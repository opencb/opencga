package org.opencb.opencga.master.tasks.monitors;

import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.catalog.db.api.DBIterator;
import org.opencb.opencga.catalog.db.api.TaskDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.catalog.managers.TaskManager;
import org.opencb.opencga.catalog.monitor.executors.BatchExecutor;
import org.opencb.opencga.catalog.monitor.executors.ExecutorFactory;
import org.opencb.opencga.core.models.Task;
import org.opencb.opencga.core.models.common.Enums;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class TaskDaemon implements Runnable {

    protected int interval;
    protected CatalogManager catalogManager;
    protected TaskManager taskManager;
    protected BatchExecutor batchExecutor;

    protected Enums.Resource resource;
    protected Enums.Action action;

    protected boolean exit = false;

    protected String token;
    protected Logger logger;

    private static final int NUM_TASKS_HANDLED = 50;

    private final Query pendingTasksQuery;
    private final Query queuedTasksQuery;
    private final Query runningTasksQuery;
    private final QueryOptions queryOptions;

    public TaskDaemon(int interval, String token, CatalogManager catalogManager) {
        this.interval = interval;
        this.catalogManager = catalogManager;
        this.taskManager = catalogManager.getTaskManager();
        this.token = token;
        logger = LoggerFactory.getLogger(this.getClass());
        ExecutorFactory executorFactory = new ExecutorFactory(catalogManager.getConfiguration());
        this.batchExecutor = executorFactory.getExecutor();

        pendingTasksQuery = new Query()
                .append(TaskDBAdaptor.QueryParams.RESOURCE.key(), resource)
                .append(TaskDBAdaptor.QueryParams.ACTION.key(), action)
                .append(TaskDBAdaptor.QueryParams.STATUS_NAME.key(), Enums.ExecutionStatus.PENDING);
        queuedTasksQuery = new Query()
                .append(TaskDBAdaptor.QueryParams.RESOURCE.key(), resource)
                .append(TaskDBAdaptor.QueryParams.ACTION.key(), action)
                .append(TaskDBAdaptor.QueryParams.STATUS_NAME.key(), Enums.ExecutionStatus.QUEUED);
        runningTasksQuery = new Query()
                .append(TaskDBAdaptor.QueryParams.RESOURCE.key(), resource)
                .append(TaskDBAdaptor.QueryParams.ACTION.key(), action)
                .append(TaskDBAdaptor.QueryParams.STATUS_NAME.key(), Enums.ExecutionStatus.RUNNING);
        // Sort tasks by creation date
        queryOptions = new QueryOptions()
                .append(QueryOptions.SORT, TaskDBAdaptor.QueryParams.CREATION_DATE.key())
                .append(QueryOptions.ORDER, QueryOptions.ASCENDING);
    }

    @Override
    public void run() {

        while (!exit) {
            try {
                Thread.sleep(interval);
            } catch (InterruptedException e) {
                if (!exit) {
                    e.printStackTrace();
                }
            }

            long pendingTasks = -1;
            long queuedTasks = -1;
            long runningTasks = -1;
            try {
                pendingTasks = taskManager.count(null, pendingTasksQuery, token).getNumMatches();
                queuedTasks = taskManager.count(null, queuedTasksQuery, token).getNumMatches();
                runningTasks = taskManager.count(null, runningTasksQuery, token).getNumMatches();
            } catch (CatalogException e) {
                logger.error("{}", e.getMessage(), e);
            }
            logger.info("----- {} {} TASK DAEMON  ----- pending={}, queued={}, running={}", resource, action, pendingTasks, queuedTasks,
                    runningTasks);

            /*
            PENDING TASKS
             */
            checkPendingTasks();

            /*
            QUEUED TASKS
             */
            checkQueuedTasks();

            /*
            RUNNING TASKS
             */
            checkRunningTasks();

        }
    }

    protected void checkPendingTasks() {
        int pendingTasksHandled = 0;
        try (DBIterator<Task> iterator = taskManager.iterator(null, pendingTasksQuery, queryOptions, token)) {
            while (pendingTasksHandled < NUM_TASKS_HANDLED && iterator.hasNext()) {
                Task task = iterator.next();
                pendingTasksHandled += checkPendingTask(task);
            }
        } catch (CatalogException e) {
            logger.error("{}", e.getMessage(), e);
        }
    }

    protected abstract int checkPendingTask(Task task);

    protected void checkQueuedTasks() {
        int queuedTasksHandled = 0;
        try (DBIterator<Task> iterator = taskManager.iterator(null, queuedTasksQuery, queryOptions, token)) {
            while (queuedTasksHandled < NUM_TASKS_HANDLED && iterator.hasNext()) {
                Task task = iterator.next();
                queuedTasksHandled += checkQueuedTask(task);
            }
        } catch (CatalogException e) {
            logger.error("{}", e.getMessage(), e);
        }
    }

    protected abstract int checkQueuedTask(Task task);

    protected void checkRunningTasks() {
        int runningTasksHandled = 0;
        try (DBIterator<Task> iterator = taskManager.iterator(null, runningTasksQuery, queryOptions, token)) {
            while (runningTasksHandled < NUM_TASKS_HANDLED && iterator.hasNext()) {
                Task task = iterator.next();
                runningTasksHandled += checkRunningTask(task);
            }
        } catch (CatalogException e) {
            logger.error("{}", e.getMessage(), e);
        }
    }

    protected abstract int checkRunningTask(Task task);


//    private void checkDeletedFiles() throws CatalogException {
//        QueryResult<File> files = catalogManager.getFileManager().get((long) -1, new Query(FileDBAdaptor.QueryParams.STATUS_NAME.key(),
//                File.FileStatus.TRASHED), new QueryOptions(), sessionId);
//
//        long currentTimeMillis = System.currentTimeMillis();
//        for (File file: files.getResult()) {
//            try {
//                //TODO: skip if the file is a non-empty folder
//                long deleteTimeMillis = TimeUtils.toDate(file.getStatus().getDate()).toInstant().toEpochMilli();
////                long deleteDate = new ObjectMap(file.getAttributes()).getLong(file.getName().getCreationDate(), 0);
//                if ((currentTimeMillis - deleteTimeMillis) > deleteDelayMillis) {
////                            QueryResult<Study> studyQueryResult =
////                                    catalogManager.getStudy(catalogManager.getStudyIdByFileId(file.getId()), sessionId);
////                            Study study = studyQueryResult.getResult().get(0);
////                            logger.info("Deleting file {} from study {id: {}, alias: {}}", file, study.getId(), study.getAlias());
//                    catalogFileUtils.delete(file, sessionId);
//                } else {
//                    logger.info("Don't delete file {id: {}, path: '{}', attributes: {}}}", file.getId(), file.getPath(),
//                            file.getAttributes());
//                    logger.info("{}s", (currentTimeMillis - deleteTimeMillis) / 1000);
//                }
//            } catch (Exception e) {
//                e.printStackTrace();
//            }
//        }
//    }

//    private void checkPendingRemoveFiles() throws CatalogException {
//        QueryResult<File> files = catalogManager.getFileManager().get((long) -1, new Query(FileDBAdaptor.QueryParams.STATUS_NAME.key(),
//                File.FileStatus.DELETED), new QueryOptions(), sessionId);
//
//        long currentTimeMillis = System.currentTimeMillis();
//        for (File file: files.getResult()) {
//            long deleteTimeMillis = TimeUtils.toDate(file.getStatus().getDate()).toInstant().toEpochMilli();
//            if ((currentTimeMillis - deleteTimeMillis) > deleteDelayMillis) {
//                if (file.getType().equals(File.Type.FILE)) {
//                    catalogManager.getFileManager().delete(null, Long.toString(file.getId()), null, sessionId);
//                } else {
//                    System.out.println("empty block");
//                }
//            }
//        }
//    }
}
