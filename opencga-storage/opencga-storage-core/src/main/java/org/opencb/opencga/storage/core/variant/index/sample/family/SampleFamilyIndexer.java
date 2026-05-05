package org.opencb.opencga.storage.core.variant.index.sample.family;

import org.opencb.commons.datastore.core.DataResult;
import org.opencb.commons.datastore.core.Event;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.metadata.models.SampleMetadata;
import org.opencb.opencga.storage.core.metadata.models.TaskMetadata;
import org.opencb.opencga.storage.core.metadata.models.Trio;
import org.opencb.opencga.storage.core.variant.index.sample.SampleIndexDBAdaptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

import static org.opencb.opencga.core.api.ParamConstants.OVERWRITE;

public abstract class SampleFamilyIndexer {

    protected final VariantStorageMetadataManager metadataManager;
    protected final SampleIndexDBAdaptor sampleIndexDBAdaptor;
    private final Logger logger = LoggerFactory.getLogger(SampleFamilyIndexer.class);

    public SampleFamilyIndexer(SampleIndexDBAdaptor sampleIndexDBAdaptor) {
        this.sampleIndexDBAdaptor = sampleIndexDBAdaptor;
        this.metadataManager = sampleIndexDBAdaptor.getMetadataManager();
    }

    public DataResult<Trio> load(String study, List<Trio> trios, ObjectMap options) throws StorageEngineException {
        int studyId = metadataManager.getStudyId(study);
        int version = sampleIndexDBAdaptor.getSchemaFactory().getSampleIndexConfigurationLatest(studyId, true).getVersion();
        return load(study, trios, options, version);
    }

    public DataResult<Trio> load(String study, List<Trio> trios, ObjectMap options, int version) throws StorageEngineException {
        trios = new LinkedList<>(trios);
        DataResult<Trio> dr = new DataResult<>();
        dr.setResults(trios);
        dr.setEvents(new LinkedList<>());

        boolean overwrite = options.getBoolean(OVERWRITE, false);
        if (trios.isEmpty()) {
            throw new StorageEngineException("Undefined family trios");
        }

        int studyId = metadataManager.getStudyId(study);

        Iterator<Trio> iterator = trios.iterator();
        while (iterator.hasNext()) {
            Trio trio = iterator.next();

            final Integer fatherId;
            final Integer motherId;
            final Integer childId;

            childId = metadataManager.getSampleId(studyId, trio.getChild());
            if (trio.getFather() == null) {
                fatherId = -1;
            } else {
                fatherId = metadataManager.getSampleIdOrFail(studyId, trio.getFather());
            }
            if (trio.getMother() == null) {
                motherId = -1;
            } else {
                motherId = metadataManager.getSampleIdOrFail(studyId, trio.getMother());
            }

            SampleMetadata sampleMetadata = metadataManager.getSampleMetadata(studyId, childId);
            if (!overwrite && sampleMetadata.getFamilyIndexStatus(version) == TaskMetadata.Status.READY) {
                String msg = "Skip sample " + sampleMetadata.getName() + ". Already precomputed!";
                logger.info(msg);
                dr.getEvents().add(new Event(Event.Type.INFO, msg));
                iterator.remove();
            } else {
                boolean fatherDefined = fatherId != -1;
                boolean motherDefined = motherId != -1;
                if (fatherDefined && !fatherId.equals(sampleMetadata.getFather())
                        || motherDefined && !motherId.equals(sampleMetadata.getMother())) {
                    metadataManager.updateSampleMetadata(studyId, sampleMetadata.getId(), s -> {
                        if (fatherDefined) {
                            s.setFather(fatherId);
                        }
                        if (motherDefined) {
                            s.setMother(motherId);
                        }
                        s.setFamilyIndexDefined(true);
                    });
                }
            }
        }
        if (trios.isEmpty()) {
            logger.info("Nothing to do!");
            return dr;
        }

        run(study, trios, options, studyId, version);
        postRun(studyId, version);
        return dr;
    }

    protected void run(String study, List<Trio> trios, ObjectMap options, int studyId, int version) throws StorageEngineException {
        // By default, run all in a single batch
        runBatch(study, trios, options, studyId, version);
    }


    protected void runBatch(String study, List<Trio> trios, ObjectMap options, int studyId, int version)
            throws StorageEngineException {
        if (trios.size() < 20) {
            List<String> childNames = trios.stream().map(Trio::getChild).collect(Collectors.toList());
            logger.info("Run sample family indexer on study '{}'({}), schema version {}, for trios with children {}",
                    metadataManager.getStudyName(studyId), studyId, version, childNames);
        } else {
            logger.info("Run sample family indexer on study '{}'({}), schema version {}, for {} trios",
                    metadataManager.getStudyName(studyId), studyId, version, trios.size());
        }
        indexBatch(study, trios, options, studyId, version);
        postIndexBatch(studyId, trios, version);
    }

    protected abstract void indexBatch(String study, List<Trio> trios, ObjectMap options, int studyId, int version)
            throws StorageEngineException;

    protected void postIndexBatch(int studyId, List<Trio> trios, int version)
            throws StorageEngineException {
        VariantStorageMetadataManager metadataManager = sampleIndexDBAdaptor.getMetadataManager();
        for (Trio trio : trios) {
            Integer father = trio.getFather() == null ? -1 : metadataManager.getSampleId(studyId, trio.getFather());
            Integer mother = trio.getMother() == null ? -1 : metadataManager.getSampleId(studyId, trio.getMother());
            Integer child = metadataManager.getSampleId(studyId, trio.getChild());
            metadataManager.updateSampleMetadata(studyId, child, sampleMetadata -> {
                sampleMetadata.setMendelianErrorStatus(TaskMetadata.Status.READY);
                sampleMetadata.setFamilyIndexStatus(TaskMetadata.Status.READY, version);
                sampleMetadata.setFamilyIndexDefined(true);
                if (father > 0) {
                    sampleMetadata.setFather(father);
                }
                if (mother > 0) {
                    sampleMetadata.setMother(mother);
                }
            });
        }
    }

    public void postRun(int studyId, int version)
            throws StorageEngineException {
        sampleIndexDBAdaptor.updateSampleIndexSchemaStatus(studyId, version);
    }

}
