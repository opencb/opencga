package org.opencb.opencga.storage.core.variant.score;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.metadata.models.StudyMetadata;
import org.opencb.opencga.storage.core.metadata.models.TaskMetadata;
import org.opencb.opencga.storage.core.metadata.models.VariantScoreMetadata;
import org.opencb.opencga.storage.core.variant.VariantStorageOptions;
import org.opencb.opencga.storage.core.variant.dummy.DummyVariantStorageMetadataDBAdaptorFactory;

import java.io.IOException;
import java.net.URI;
import java.util.Collections;
import java.util.concurrent.ExecutionException;

public class VariantScoreTest {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    private VariantScoreRemover remover;
    private VariantStorageMetadataManager metadataManager;
    private StudyMetadata study;
    private VariantScoreLoader loader;
    private VariantScoreMetadata score1;
    private VariantScoreMetadata score2;
    private VariantScoreMetadata score3;

    @Before
    public void setUp() throws Exception {
        metadataManager = new VariantStorageMetadataManager(new DummyVariantStorageMetadataDBAdaptorFactory(true));
        loader = new VariantScoreLoader(metadataManager, null) {
            @Override
            protected void load(URI scoreFile, VariantScoreMetadata scoreMetadata, VariantScoreFormatDescriptor descriptor, ObjectMap options)
                    throws ExecutionException, IOException {
                if (options.getBoolean("fail", false)) {
                    throw new IOException("Fail");
                }
            }
        };
        remover = new VariantScoreRemover(metadataManager) {
            @Override
            protected void remove(VariantScoreMetadata scoreMetadata, ObjectMap options) throws StorageEngineException {
                if (options.getBoolean("fail", false)) {
                    throw new StorageEngineException("Fail");
                }
            }
        };

        study = metadataManager.createStudy("STUDY");
        Integer cohortId = metadataManager.registerCohort(study.getName(), "COHORT", Collections.emptyList());
        score1 = metadataManager.getOrCreateVariantScoreMetadata(study.getId(), "score1", cohortId, null);
        score2 = metadataManager.getOrCreateVariantScoreMetadata(study.getId(), "score2", cohortId, null);
        score3 = metadataManager.getOrCreateVariantScoreMetadata(study.getId(), "score3", cohortId, null);
    }

    @Test
    public void testLoadRemove() throws Exception {
        loader.loadVariantScore(null, study.getName(), score1.getName(), "COHORT", null, new VariantScoreFormatDescriptor(1, 2, 3), new ObjectMap());
        Assert.assertEquals(TaskMetadata.Status.READY, metadataManager.getVariantScoreMetadata(study.getId(), "score1").getIndexStatus());

        remover.remove("STUDY", "score1", new ObjectMap());
        Assert.assertNull(metadataManager.getVariantScoreMetadata(study.getId(), "score1"));
    }

    @Test
    public void testLoadTwice() throws Exception {
        loader.loadVariantScore(null, study.getName(), score1.getName(), "COHORT", null, new VariantScoreFormatDescriptor(1, 2, 3), new ObjectMap());

        thrown.expect(StorageEngineException.class);
        thrown.expectMessage("Variant score 'score1' is in status 'READY'");
        loader.loadVariantScore(null, study.getName(), score1.getName(), "COHORT", null, new VariantScoreFormatDescriptor(1, 2, 3), new ObjectMap());
    }

    @Test
    public void testLoadResume() throws Exception {
        try {
            loader.loadVariantScore(null, study.getName(), score1.getName(), "COHORT", null, new VariantScoreFormatDescriptor(1, 2, 3), new ObjectMap("fail", true));
            Assert.fail();
        } catch (StorageEngineException ignore) {
        }
        Assert.assertEquals(TaskMetadata.Status.ERROR, metadataManager.getVariantScoreMetadata(study.getId(), "score1").getIndexStatus());
        loader.loadVariantScore(null, study.getName(), score1.getName(), "COHORT", null, new VariantScoreFormatDescriptor(1, 2, 3), new ObjectMap());
        Assert.assertEquals(TaskMetadata.Status.READY, metadataManager.getVariantScoreMetadata(study.getId(), "score1").getIndexStatus());
    }

    @Test
    public void testLoadErrorRemove() throws Exception {
        try {
            loader.loadVariantScore(null, study.getName(), score1.getName(), "COHORT", null, new VariantScoreFormatDescriptor(1, 2, 3), new ObjectMap("fail", true));
            Assert.fail();
        } catch (StorageEngineException ignore) {
        }
        Assert.assertEquals(TaskMetadata.Status.ERROR, metadataManager.getVariantScoreMetadata(study.getId(), "score1").getIndexStatus());
        remover.remove("STUDY", "score1", new ObjectMap());
        Assert.assertNull(metadataManager.getVariantScoreMetadata(study.getId(), "score1"));
    }

    @Test
    public void testLoadRunningRemoveFail() throws Exception {
        loader.loadVariantScore(null, study.getName(), score1.getName(), "COHORT", null, new VariantScoreFormatDescriptor(1, 2, 3), new ObjectMap());
        metadataManager.updateVariantScoreMetadata(study.getId(), score1.getId(), variantScoreMetadata -> variantScoreMetadata.setIndexStatus(TaskMetadata.Status.RUNNING));
        Assert.assertEquals(TaskMetadata.Status.RUNNING, metadataManager.getVariantScoreMetadata(study.getId(), "score1").getIndexStatus());

        thrown.expect(StorageEngineException.class);
        thrown.expectMessage("Score 'score1' is in status 'RUNNING' right now");
        remover.remove("STUDY", "score1", new ObjectMap());
    }

    @Test
    public void testLoadRunningRemoveForce() throws Exception {
        loader.loadVariantScore(null, study.getName(), score1.getName(), "COHORT", null, new VariantScoreFormatDescriptor(1, 2, 3), new ObjectMap());
        metadataManager.updateVariantScoreMetadata(study.getId(), score1.getId(), variantScoreMetadata -> variantScoreMetadata.setIndexStatus(TaskMetadata.Status.RUNNING));
        Assert.assertEquals(TaskMetadata.Status.RUNNING, metadataManager.getVariantScoreMetadata(study.getId(), "score1").getIndexStatus());

        remover.remove("STUDY", "score1", new ObjectMap(VariantStorageOptions.FORCE.key(), true));
        Assert.assertNull(metadataManager.getVariantScoreMetadata(study.getId(), "score1"));
    }

    @Test
    public void testLoadRemoveError() throws Exception {
        loader.loadVariantScore(null, study.getName(), score1.getName(), "COHORT", null, new VariantScoreFormatDescriptor(1, 2, 3), new ObjectMap());
        Assert.assertEquals(TaskMetadata.Status.READY, metadataManager.getVariantScoreMetadata(study.getId(), "score1").getIndexStatus());

        try {
            remover.remove("STUDY", "score1", new ObjectMap("fail", true));
            Assert.fail();
        } catch (StorageEngineException ignore) {
        }
        Assert.assertEquals(TaskMetadata.Status.ERROR, metadataManager.getVariantScoreMetadata(study.getId(), "score1").getIndexStatus());
        Assert.assertEquals(TaskMetadata.Status.ERROR, metadataManager.getVariantScoreMetadata(study.getId(), "score1").getRemoveStatus());
        remover.remove("STUDY", "score1", new ObjectMap());
        Assert.assertNull(metadataManager.getVariantScoreMetadata(study.getId(), "score1"));
    }

}
