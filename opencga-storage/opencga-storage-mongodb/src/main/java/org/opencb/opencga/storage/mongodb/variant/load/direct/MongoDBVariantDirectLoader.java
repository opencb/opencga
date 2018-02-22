package org.opencb.opencga.storage.mongodb.variant.load.direct;

import com.google.common.collect.ListMultimap;
import org.apache.commons.lang3.tuple.Pair;
import org.bson.Document;
import org.bson.types.Binary;
import org.opencb.commons.io.DataWriter;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.mongodb.variant.adaptors.VariantMongoDBAdaptor;
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
public class MongoDBVariantDirectLoader implements DataWriter<Pair<ListMultimap<Document, Binary>, MongoDBOperations>> {

    private final MongoDBVariantStageLoader stageLoader;
    private final MongoDBVariantMergeLoader variantsLoader;

    public MongoDBVariantDirectLoader(VariantMongoDBAdaptor dbAdaptor, final StudyConfiguration studyConfiguration, int fileId,
                                      boolean resume) {
        stageLoader = new MongoDBVariantStageLoader(dbAdaptor.getStageCollection(), studyConfiguration.getStudyId(), fileId, resume, true);
        variantsLoader = new MongoDBVariantMergeLoader(
                dbAdaptor.getVariantsCollection(),
                dbAdaptor.getStageCollection(),
                dbAdaptor.getStudiesCollection(),
                studyConfiguration, Collections.singletonList(fileId), resume, false, null);
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
    public boolean write(List<Pair<ListMultimap<Document, Binary>, MongoDBOperations>> batch) {
        for (Pair<ListMultimap<Document, Binary>, MongoDBOperations> pair : batch) {
            // Write in stage collection
            stageLoader.write(pair.getKey());

            // Write in variants collection
            variantsLoader.write(pair.getValue());
        }
        return true;
    }

    public MongoDBVariantWriteResult getResult() {
        return variantsLoader.getResult();
    }

}
