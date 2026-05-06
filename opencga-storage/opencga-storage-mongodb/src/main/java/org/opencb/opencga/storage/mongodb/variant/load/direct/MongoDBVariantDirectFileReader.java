package org.opencb.opencga.storage.mongodb.variant.load.direct;

import org.bson.Document;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.commons.io.DataReader;
import org.opencb.opencga.storage.mongodb.variant.MongoDBVariantStoragePipeline;
import org.opencb.opencga.storage.mongodb.variant.converters.stage.StageDocumentToVariantConverter;
import org.opencb.opencga.storage.mongodb.variant.load.stage.MongoDBVariantStageLoader;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Reads variants from a VCF file reader and wraps them as stage-like Documents
 * for direct loading into the variants collection, bypassing the stage collection entirely.
 *
 * Each variant is converted to a Document with the same structure as a stage document:
 * <pre>
 * {
 *   "_id": "1_100_A_C",
 *   "ref": "A",
 *   "alt": "C",
 *   "end": 100,
 *   "&lt;studyId&gt;": {
 *     "&lt;fileId&gt;": [variant],
 *     "_n": true
 *   }
 * }
 * </pre>
 *
 * All variants are marked as new study ({@code _n: true}), so the merger always takes
 * the UPSERT path. Combined with {@code addToSet} for the studies array, this is safe
 * for both first and subsequent loads.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class MongoDBVariantDirectFileReader implements DataReader<Document> {

    private final DataReader<Variant> variantReader;
    private final String studyIdStr;
    private final String fileId;

    private final StageDocumentToVariantConverter converter = new StageDocumentToVariantConverter();

    private long skippedVariants = 0;

    public MongoDBVariantDirectFileReader(DataReader<Variant> variantReader, int studyId, int fileId) {
        this.variantReader = variantReader;
        this.studyIdStr = String.valueOf(studyId);
        this.fileId = String.valueOf(fileId);
    }

    @Override
    public boolean open() {
        return variantReader.open();
    }

    @Override
    public boolean pre() {
        variantReader.pre();
        return true;
    }

    @Override
    public boolean post() {
        variantReader.post();
        return true;
    }

    @Override
    public boolean close() {
        return variantReader.close();
    }

    @Override
    public List<Document> read(int batchSize) {
        List<Document> documents = new ArrayList<>(batchSize);

        while (documents.size() < batchSize) {
            List<Variant> variants = variantReader.read(batchSize - documents.size());
            if (variants == null || variants.isEmpty()) {
                break;
            }
            for (Variant variant : variants) {
                if (MongoDBVariantStoragePipeline.SKIPPED_VARIANTS.contains(variant.getType())) {
                    skippedVariants++;
                    continue;
                }
                documents.add(toStageDocument(variant));
            }
        }

        return documents;
    }

    private Document toStageDocument(Variant variant) {
        Document document = converter.convertToStorageType(variant);

        Document studyDocument = new Document();
        studyDocument.append(fileId, Collections.singletonList(variant));
        studyDocument.append(MongoDBVariantStageLoader.NEW_STUDY_FIELD, true);
        document.put(studyIdStr, studyDocument);

        return document;
    }

    public long getSkippedVariants() {
        return skippedVariants;
    }
}
