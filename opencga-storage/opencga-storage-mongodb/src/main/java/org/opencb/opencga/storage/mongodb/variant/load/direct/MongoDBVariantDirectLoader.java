package org.opencb.opencga.storage.mongodb.variant.load.direct;

import com.google.common.collect.LinkedListMultimap;
import com.mongodb.WriteConcern;
import org.bson.Document;
import org.bson.types.Binary;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.commons.ProgressLogger;
import org.opencb.commons.datastore.mongodb.MongoDBCollection;
import org.opencb.commons.io.DataWriter;
import org.opencb.opencga.storage.core.metadata.models.StudyMetadata;
import org.opencb.opencga.storage.mongodb.variant.adaptors.VariantMongoDBAdaptor;
import org.opencb.opencga.storage.mongodb.variant.converters.DocumentToVariantConverter;
import org.opencb.opencga.storage.mongodb.variant.converters.stage.StageDocumentToVariantConverter;
import org.opencb.opencga.storage.mongodb.variant.load.MongoDBVariantWriteResult;
import org.opencb.opencga.storage.mongodb.variant.load.stage.MongoDBVariantStageLoader;
import org.opencb.opencga.storage.mongodb.variant.load.stage.StageWriteOperations;
import org.opencb.opencga.storage.mongodb.variant.load.variants.MongoDBOperations;
import org.opencb.opencga.storage.mongodb.variant.load.variants.MongoDBVariantMergeLoader;

import java.util.Collections;
import java.util.List;

/**
 * Created on 20/02/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class MongoDBVariantDirectLoader implements DataWriter<MongoDBOperations> {

    private final MongoDBVariantStageLoader stageLoader;
    private final MongoDBVariantMergeLoader variantsLoader;
    private final boolean skipStage;
    private final DocumentToVariantConverter variantConverter = new DocumentToVariantConverter();
    private final StageDocumentToVariantConverter stageConverter = new StageDocumentToVariantConverter();

    public MongoDBVariantDirectLoader(VariantMongoDBAdaptor dbAdaptor, final StudyMetadata studyMetadata, int fileId,
                                      boolean resume, ProgressLogger progressLogger) {
        this(dbAdaptor, studyMetadata, fileId, resume, progressLogger, false, null);
    }

    public MongoDBVariantDirectLoader(VariantMongoDBAdaptor dbAdaptor, final StudyMetadata studyMetadata, int fileId,
                                      boolean resume, ProgressLogger progressLogger, boolean skipStage) {
        this(dbAdaptor, studyMetadata, fileId, resume, progressLogger, skipStage, null);
    }

    public MongoDBVariantDirectLoader(VariantMongoDBAdaptor dbAdaptor, final StudyMetadata studyMetadata, int fileId,
                                      boolean resume, ProgressLogger progressLogger, boolean skipStage,
                                      WriteConcern writeConcern) {
        this.skipStage = skipStage;
        MongoDBCollection stageCollection = dbAdaptor.getStageCollection(studyMetadata.getId());
        if (skipStage) {
            stageLoader = null;
        } else {
            stageLoader = new MongoDBVariantStageLoader(stageCollection, studyMetadata.getId(), fileId, resume, true);
        }
        MongoDBCollection variantsCollection = dbAdaptor.getVariantsCollection();
        if (writeConcern != null) {
            variantsCollection = variantsCollection.withWriteConcern(writeConcern);
        }
        variantsLoader = new MongoDBVariantMergeLoader(
                variantsCollection,
                stageCollection,
                dbAdaptor.getStudiesCollection(),
                studyMetadata, Collections.singletonList(fileId), resume, false, progressLogger);
    }

    @Override
    public boolean open() {
        if (stageLoader != null) {
            stageLoader.open();
        }
        variantsLoader.open();
        return true;
    }

    @Override
    public boolean pre() {
        if (stageLoader != null) {
            stageLoader.pre();
        }
        variantsLoader.pre();
        return true;
    }

    @Override
    public boolean post() {
        if (stageLoader != null) {
            stageLoader.post();
        }
        variantsLoader.post();
        return true;
    }

    @Override
    public boolean close() {
        if (stageLoader != null) {
            stageLoader.close();
        }
        variantsLoader.close();
        return true;
    }

    @Override
    public boolean write(List<MongoDBOperations> batch) {
        if (!skipStage) {
            LinkedListMultimap<Document, Binary> map = LinkedListMultimap.create();
            for (MongoDBOperations mongoDBOperations : batch) {
                for (Document document : mongoDBOperations.getNewStudy().getVariants()) {
                    Variant variant = variantConverter.convertToDataModelType(document);
                    Document stageDocument = stageConverter.convertToStorageType(variant);
                    map.put(stageDocument, null);
                }
            }
            stageLoader.write(new StageWriteOperations(map));
        }

        variantsLoader.write(batch);

        return true;
    }

    public MongoDBVariantWriteResult getResult() {
        return variantsLoader.getResult();
    }

}
