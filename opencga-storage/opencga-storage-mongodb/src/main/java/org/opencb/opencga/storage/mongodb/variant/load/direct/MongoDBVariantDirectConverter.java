package org.opencb.opencga.storage.mongodb.variant.load.direct;

import com.google.common.collect.ListMultimap;
import org.apache.commons.lang3.tuple.Pair;
import org.bson.Document;
import org.bson.types.Binary;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.commons.ProgressLogger;
import org.opencb.commons.datastore.core.ComplexTypeConverter;
import org.opencb.commons.run.Task;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.mongodb.variant.MongoDBVariantStoragePipeline;
import org.opencb.opencga.storage.mongodb.variant.adaptors.VariantMongoDBAdaptor;
import org.opencb.opencga.storage.mongodb.variant.load.stage.MongoDBVariantStageConverterTask;
import org.opencb.opencga.storage.mongodb.variant.load.variants.MongoDBOperations;
import org.opencb.opencga.storage.mongodb.variant.load.variants.MongoDBVariantMerger;

import java.util.Collections;
import java.util.List;

import static org.opencb.opencga.storage.mongodb.variant.load.stage.MongoDBVariantStageLoader.STAGE_TO_VARIANT_CONVERTER;

/**
 * Created on 21/02/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class MongoDBVariantDirectConverter implements Task<Variant, Pair<ListMultimap<Document, Binary>, MongoDBOperations>> {

    public static final ComplexTypeConverter<Variant, Binary> EMPTY_CONVERTER = new ComplexTypeConverter<Variant, Binary>() {
        @Override public Variant convertToDataModelType(Binary binary) {
            return null;
        }
        @Override public Binary convertToStorageType(Variant variant) {
            return null;
        }
    };
    private final MongoDBVariantStageConverterTask stageConverter;

    private final MongoDBVariantMergerDirect variantConverter;

    private final ProgressLogger progressLogger;

    public MongoDBVariantDirectConverter(VariantMongoDBAdaptor dbAdaptor, final StudyConfiguration studyConfiguration, int fileId,
                                         boolean resume, ProgressLogger progressLogger) {
        this.progressLogger = progressLogger;
        stageConverter = new MongoDBVariantStageConverterTask(null, EMPTY_CONVERTER);

        variantConverter = new MongoDBVariantMergerDirect(dbAdaptor, studyConfiguration, fileId, resume);
    }

    @Override
    public void pre() throws Exception {
        variantConverter.pre();
        stageConverter.pre();
    }

    @Override
    public void post() throws Exception {
        variantConverter.post();
        stageConverter.post();
    }

    public List<Pair<ListMultimap<Document, Binary>, MongoDBOperations>> apply(List<Variant> variants) {
        // Convert into stage collection format
        ListMultimap<Document, Binary> archiveDocuments = stageConverter.convert(variants);

        // Convert into variants collection formar
        MongoDBOperations mongoDBOperations = variantConverter.processVariants(variants, new MongoDBOperations());

        progressLogger.increment(variants.size(), () -> "up to variant " + variants.get(variants.size() - 1));
        return Collections.singletonList(Pair.of(archiveDocuments, mongoDBOperations));
    }

    public long getSkippedVariants() {
        return stageConverter.getSkippedVariants();
    }

    private static class MongoDBVariantMergerDirect extends MongoDBVariantMerger {

        private String studyIdStr;
        private String fileIdStr;

        MongoDBVariantMergerDirect(VariantMongoDBAdaptor dbAdaptor, StudyConfiguration studyConfiguration,
                                   int fileId, boolean resume) {
            super(dbAdaptor, studyConfiguration, Collections.singletonList(fileId), studyConfiguration.getIndexedFiles(), resume, true);
            studyIdStr = String.valueOf(studyConfiguration.getStudyId());
            fileIdStr = String.valueOf(fileId);
        }

        protected MongoDBOperations processVariants(List<Variant> variants, MongoDBOperations mongoDBOps) {
            for (Variant variant : variants) {
                if (MongoDBVariantStoragePipeline.SKIPPED_VARIANTS.contains(variant.getType())) {
                    continue;
                }
                Document stageDocument = STAGE_TO_VARIANT_CONVERTER.convertToStorageType(variant);
                stageDocument.append(studyIdStr, new Document(fileIdStr, Collections.singletonList(variant)));
                processVariant(stageDocument, variant, mongoDBOps);
            }
            return mongoDBOps;
        }
    }
}
