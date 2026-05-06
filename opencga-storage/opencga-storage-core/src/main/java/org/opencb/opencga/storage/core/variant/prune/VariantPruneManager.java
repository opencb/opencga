package org.opencb.opencga.storage.core.variant.prune;

import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.metadata.models.TaskMetadata;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

public abstract class VariantPruneManager {

    public static final String OPERATION_NAME = "VariantPrune";
    protected final Logger logger = LoggerFactory.getLogger(getClass());
    private final VariantStorageEngine engine;

    protected VariantPruneManager(VariantStorageEngine engine) {
        this.engine = engine;
    }

    protected VariantStorageEngine getEngine() {
        return engine;
    }

    public void prune(boolean dryMode, boolean resume, URI outdir) throws StorageEngineException {
        List<TaskMetadata> tasks = pre(dryMode, resume);
        Thread hook = addHook(tasks);
        try {
            runPrune(dryMode, outdir);
            post(tasks, true);
        } catch (Exception e) {
            try {
                post(tasks, false);
            } catch (Exception e1) {
                e.addSuppressed(e1);
            }
            throw e;
        } finally {
            removeHook(hook);
        }
    }

    protected abstract void runPrune(boolean dryMode, URI outdir) throws StorageEngineException;

    private void removeHook(Thread hook) {
        Runtime.getRuntime().removeShutdownHook(hook);
    }

    private Thread addHook(List<TaskMetadata> tasks) {
        Thread hook = new Thread(() -> {
            try {
                post(tasks, false);
            } catch (StorageEngineException e) {
                logger.error("Catch error while running shutdown hook.", e);
            }
        });
        Runtime.getRuntime().addShutdownHook(hook);
        return hook;
    }

    protected List<TaskMetadata> pre(boolean dryMode, boolean resume) throws StorageEngineException {
        VariantStorageMetadataManager mm = engine.getMetadataManager();

        List<TaskMetadata> tasks = new LinkedList<>();
        List<String> studiesWithoutStats = new LinkedList<>();

        // First check no running operations in any study
        for (Integer studyId : mm.getStudies().values()) {
            // Do not allow concurrent operations at all.
            mm.checkTaskCanRun(studyId, OPERATION_NAME, Collections.emptyList(), resume,
                    TaskMetadata.Type.REMOVE, tm -> false);
        }

        // Check that all variant stats are updated
        for (Integer studyId : mm.getStudies().values()) {
            if (!mm.getCohortMetadata(studyId, StudyEntry.DEFAULT_COHORT).isStatsReady()) {
                studiesWithoutStats.add(mm.getStudyName(studyId));
            }
            // FIXME: What if not invalid?
            //   Might happen if some samples were deleted, or when loading split files?
        }

        // Discard studies without loaded files.
        // These can't have the stats computed.
        studiesWithoutStats.removeIf(study -> mm.getIndexedFiles(mm.getStudyId(study)).isEmpty());

        if (!studiesWithoutStats.isEmpty()) {
            throw new StorageEngineException("Unable to run variant prune operation. "
                    + "Please, run variant stats index on cohort '" + StudyEntry.DEFAULT_COHORT
                    + "' for studies " + studiesWithoutStats);
        }

        // If no dry-mode, add the new tasks
        if (!dryMode) {
            for (Integer studyId : mm.getStudies().values()) {
                // Do not allow concurrent operations at all.
                tasks.add(mm.addRunningTask(studyId, OPERATION_NAME, Collections.emptyList(), resume,
                        TaskMetadata.Type.REMOVE, tm -> false));
            }
        }

        return tasks;
    }

    protected void post(List<TaskMetadata> tasks, boolean success) throws StorageEngineException {
        VariantStorageMetadataManager mm = engine.getMetadataManager();
        for (TaskMetadata task : tasks) {
            mm.updateTask(task.getStudyId(), task.getId(), t -> {
                if (success) {
                    t.addStatus(TaskMetadata.Status.READY);
                } else {
                    t.addStatus(TaskMetadata.Status.ERROR);
                }
            });
        }
    }
}
