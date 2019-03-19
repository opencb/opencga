package org.opencb.opencga.storage.mongodb.variant.load.direct;

import com.google.common.collect.LinkedListMultimap;
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

    public MongoDBVariantDirectLoader(VariantMongoDBAdaptor dbAdaptor, final StudyMetadata studyMetadata, int fileId,
                                      boolean resume, ProgressLogger progressLogger) {
        MongoDBCollection stageCollection = dbAdaptor.getStageCollection(studyMetadata.getId());
        stageLoader = new MongoDBVariantStageLoader(stageCollection, studyMetadata.getId(), fileId, resume, true);
        variantsLoader = new MongoDBVariantMergeLoader(
                dbAdaptor.getVariantsCollection(),
                stageCollection,
                dbAdaptor.getStudiesCollection(),
                studyMetadata, Collections.singletonList(fileId), resume, false, progressLogger);
    }

    @Override
    public boolean open() {
        stageLoader.open();
        variantsLoader.open();
        return true;
    }

    @Override
    public boolean pre() {
        stageLoader.pre();
        variantsLoader.pre();
        return true;
    }

    @Override
    public boolean post() {
        stageLoader.post();
        variantsLoader.post();
        return true;
    }

    @Override
    public boolean close() {
        stageLoader.close();
        variantsLoader.close();
        return true;
    }

    @Override
    public boolean write(List<MongoDBOperations> batch) {
        LinkedListMultimap<Document, Binary> map = LinkedListMultimap.create();
        for (MongoDBOperations mongoDBOperations : batch) {
            for (Document document : mongoDBOperations.getNewStudy().getVariants()) {
                Variant variant = new DocumentToVariantConverter().convertToDataModelType(document);
                Document stageDocument = new StageDocumentToVariantConverter().convertToStorageType(variant);
                map.put(stageDocument, null);
            }
        }

        stageLoader.write(map);

        variantsLoader.write(batch);

        return true;
    }

    public MongoDBVariantWriteResult getResult() {
        return variantsLoader.getResult();
    }

}
