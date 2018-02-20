package org.opencb.opencga.storage.mongodb.variant.load.variants;

import com.google.common.collect.ListMultimap;
import org.bson.Document;
import org.bson.types.Binary;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.commons.ProgressLogger;
import org.opencb.commons.datastore.core.ComplexTypeConverter;
import org.opencb.commons.io.DataWriter;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.mongodb.variant.MongoDBVariantStoragePipeline;
import org.opencb.opencga.storage.mongodb.variant.adaptors.VariantMongoDBAdaptor;
import org.opencb.opencga.storage.mongodb.variant.load.MongoDBVariantWriteResult;
import org.opencb.opencga.storage.mongodb.variant.load.stage.MongoDBVariantStageConverterTask;
import org.opencb.opencga.storage.mongodb.variant.load.stage.MongoDBVariantStageLoader;

import java.util.Collections;
import java.util.List;

import static org.opencb.opencga.storage.mongodb.variant.load.stage.MongoDBVariantStageLoader.STAGE_TO_VARIANT_CONVERTER;

/**
 * Created on 20/02/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class MongoDBVariantDirectLoader implements DataWriter<Variant> {

    public static final ComplexTypeConverter<Variant, Binary> EMPTY_CONVERTER = new ComplexTypeConverter<Variant, Binary>() {
        @Override public Variant convertToDataModelType(Binary binary) {
            return null;
        }
        @Override public Binary convertToStorageType(Variant variant) {
            return null;
        }
    };
    private final MongoDBVariantStageConverterTask stageConverter;
    private final MongoDBVariantStageLoader stageLoader;

    private final MongoDBVariantDirectConverter variantConverter;
    private final MongoDBVariantMergeLoader variantsLoader;

    private final ProgressLogger progressLogger;

    public MongoDBVariantDirectLoader(VariantMongoDBAdaptor dbAdaptor, final StudyConfiguration studyConfiguration, int fileId,
                                      boolean resume, ProgressLogger progressLogger) {
        this.progressLogger = progressLogger;
        stageConverter = new MongoDBVariantStageConverterTask(null, EMPTY_CONVERTER);
        stageLoader = new MongoDBVariantStageLoader(dbAdaptor.getStageCollection(), studyConfiguration.getStudyId(), fileId, resume, true);
        variantsLoader = new MongoDBVariantMergeLoader(
                dbAdaptor.getVariantsCollection(),
                dbAdaptor.getStageCollection(),
                dbAdaptor.getStudiesCollection(),
                studyConfiguration, Collections.singletonList(fileId), resume, false, null);
        variantConverter = new MongoDBVariantDirectConverter(dbAdaptor, studyConfiguration, fileId, resume);
    }

    @Override
    public boolean pre() {
        stageConverter.pre();
        stageLoader.pre();
        variantsLoader.pre();
        return true;
    }

    @Override
    public boolean post() {
        stageConverter.post();
        stageLoader.post();
        variantsLoader.post();
        return true;
    }

    @Override
    public boolean open() {
        stageLoader.open();
        variantsLoader.open();
        return true;
    }

    @Override
    public boolean close() {
        stageLoader.close();
        variantsLoader.close();
        return true;
    }

    @Override
    public boolean write(List<Variant> variants) {
        // Write in archive collection
        List<ListMultimap<Document, Binary>> archiveDocuments = stageConverter.apply(variants);
        stageLoader.write(archiveDocuments);

        // Write in archive collection
        MongoDBOperations mongoDBOperations = variantConverter.processVariants(variants, new MongoDBOperations());
        variantsLoader.write(mongoDBOperations);

        progressLogger.increment(variants.size(), () -> "up to variant " + variants.get(variants.size() - 1));
        return true;
    }

    public MongoDBVariantWriteResult getResult() {
        MongoDBVariantWriteResult result = variantsLoader.getResult();
        result.setSkippedVariants(stageConverter.getSkippedVariants());
        return result;
    }

    private static class MongoDBVariantDirectConverter extends MongoDBVariantMerger {

        private String studyIdStr;
        private String fileIdStr;

        MongoDBVariantDirectConverter(VariantMongoDBAdaptor dbAdaptor, StudyConfiguration studyConfiguration,
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
