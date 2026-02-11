package org.opencb.opencga.storage.mongodb.variant.converters;

import org.apache.commons.lang3.StringUtils;
import org.bson.Document;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.StructuralVariation;
import org.opencb.opencga.storage.mongodb.variant.adaptors.VariantMongoDBAdaptor;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import static org.opencb.opencga.storage.mongodb.variant.converters.DocumentToVariantConverter.*;

public class VariantToDocumentConverter extends AbstractDocumentConverter {

    private final StudyEntryToDocumentConverter variantStudyEntryConverter;
    private final DocumentToVariantAnnotationConverter variantAnnotationConverter;
    private final DocumentToVariantStatsConverter statsConverter;

    public VariantToDocumentConverter(StudyEntryToDocumentConverter variantStudyEntryConverter,
                                      DocumentToVariantStatsConverter statsConverter,
                                      DocumentToVariantAnnotationConverter variantAnnotationConverter) {
        this.variantStudyEntryConverter = variantStudyEntryConverter;
        this.variantAnnotationConverter = variantAnnotationConverter;
        this.statsConverter = statsConverter;
    }

    public Document convertToStorageType(Variant variant) {
        // Attributes easily calculated
        Document mongoVariant = new Document("_id", buildStorageId(variant))
                .append(ID_FIELD, variant.toString())    //Do not include IDs.
                .append(CHROMOSOME_FIELD, variant.getChromosome())
                .append(START_FIELD, variant.getStart())
                .append(END_FIELD, variant.getEnd())
                .append(LENGTH_FIELD, variant.getLength())
                .append(REFERENCE_FIELD, variant.getReference())
                .append(ALTERNATE_FIELD, variant.getAlternate())
                .append(TYPE_FIELD, variant.getType().name());

        // SV
        if (variant.getSv() != null) {
            StructuralVariation sv = variant.getSv();
            Document mongoSv = new Document();
            mongoSv.put(SV_CISTART_FIELD, Arrays.asList(sv.getCiStartLeft(), sv.getCiStartRight()));
            mongoSv.put(SV_CIEND_FIELD, Arrays.asList(sv.getCiEndLeft(), sv.getCiEndRight()));
            if (sv.getCopyNumber() != null) {
                mongoSv.put(SV_CN_FIELD, sv.getCopyNumber());
            }
            if (StringUtils.isNotEmpty(sv.getLeftSvInsSeq()) || StringUtils.isNotEmpty(sv.getRightSvInsSeq())) {
                mongoSv.put(SV_INS_SEQ, Arrays.asList(sv.getLeftSvInsSeq(), sv.getRightSvInsSeq()));
            }
            if (sv.getType() != null) {
                mongoSv.put(SV_TYPE, sv.getType().toString());
            }
            if (sv.getBreakend() != null) {
                Document mongoBnd = new Document();
                putNotNull(mongoBnd, SV_BND_ORIENTATION, sv.getBreakend().getOrientation().toString());
                putNotNull(mongoBnd, SV_BND_INS_SEQ, sv.getBreakend().getInsSeq());
                if (sv.getBreakend().getMate() != null) {
                    Document mongoBndMate = new Document();
                    putNotNull(mongoBndMate, SV_BND_MATE_CHR, sv.getBreakend().getMate().getChromosome());
                    putNotNull(mongoBndMate, SV_BND_MATE_POS, sv.getBreakend().getMate().getPosition());
                    putNotNull(mongoBndMate, SV_BND_MATE_CI_POS_L, sv.getBreakend().getMate().getCiPositionLeft());
                    putNotNull(mongoBndMate, SV_BND_MATE_CI_POS_R, sv.getBreakend().getMate().getCiPositionRight());
                    mongoBnd.append(SV_BND_MATE, mongoBndMate);
                }
                mongoSv.append(SV_BND, mongoBnd);
            }
            mongoVariant.put(SV_FIELD, mongoSv);
        }

        // Internal fields used for query optimization (dictionary named "_at")
        Document at = new Document();
        mongoVariant.append(AT_FIELD, at);

        // Two different chunk sizes are calculated for different resolution levels: 1k and 10k
        List<String> chunkIds = new LinkedList<>();
        String chunkSmall = variant.getChromosome() + "_" + variant.getStart() / VariantMongoDBAdaptor.CHUNK_SIZE_SMALL + "_"
                + VariantMongoDBAdaptor.CHUNK_SIZE_SMALL / 1000 + "k";
        String chunkBig = variant.getChromosome() + "_" + variant.getStart() / VariantMongoDBAdaptor.CHUNK_SIZE_BIG + "_"
                + VariantMongoDBAdaptor.CHUNK_SIZE_BIG / 1000 + "k";
        String chunkLarge = variant.getChromosome() + "_" + variant.getStart() / VariantMongoDBAdaptor.CHUNK_SIZE_LARGE + "_"
                + VariantMongoDBAdaptor.CHUNK_SIZE_LARGE / 1000 + "k";
        chunkIds.add(chunkSmall);
        chunkIds.add(chunkBig);
        chunkIds.add(chunkLarge);
        at.append(CHUNK_IDS_FIELD, chunkIds);

        // Files
        if (variantStudyEntryConverter != null) {
            List<Document> mongoFiles = new LinkedList<>();
            for (StudyEntry archiveFile : variant.getStudies()) {
                mongoFiles.add(variantStudyEntryConverter.convertToStorageType(variant, archiveFile));
            }
            mongoVariant.append(STUDIES_FIELD, mongoFiles);
        }

//        // Annotations
        mongoVariant.append(ANNOTATION_FIELD, null);
        if (variantAnnotationConverter != null) {
            if (variant.getAnnotation() != null
                    && variant.getAnnotation().getConsequenceTypes() != null
                    && !variant.getAnnotation().getConsequenceTypes().isEmpty()) {
                Document annotation = variantAnnotationConverter.convertToStorageType(variant.getAnnotation());
                mongoVariant.append(ANNOTATION_FIELD, annotation);
            }
        }

        // Statistics
        if (statsConverter != null) {
            List mongoStats = statsConverter.convertCohortsToStorageType(variant.getStudiesMap());
            mongoVariant.put(STATS_FIELD, mongoStats);
        }

        return mongoVariant;
    }
}
