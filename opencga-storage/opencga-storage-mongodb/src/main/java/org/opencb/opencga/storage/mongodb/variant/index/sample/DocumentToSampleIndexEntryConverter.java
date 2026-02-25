package org.opencb.opencga.storage.mongodb.variant.index.sample;

import org.bson.Document;
import org.bson.types.Binary;
import org.opencb.commons.datastore.core.ComplexTypeConverter;
import org.opencb.opencga.storage.core.variant.index.sample.genotype.SampleIndexVariantBiConverter;
import org.opencb.opencga.storage.core.variant.index.sample.models.SampleIndexEntry;
import org.opencb.opencga.storage.core.variant.index.sample.models.SampleIndexEntryIterator;
import org.opencb.opencga.storage.core.variant.index.sample.models.SampleIndexVariant;
import org.opencb.opencga.storage.core.variant.index.sample.schema.SampleIndexSchema;

import java.util.*;
import java.util.stream.Collectors;

public class DocumentToSampleIndexEntryConverter implements ComplexTypeConverter<SampleIndexEntry, Document> {

    public static final String SAMPLE_ID = "sampleId";
    public static final String CHROMOSOME = "chromosome";
    public static final String BATCH_START = "batchStart";
    public static final String DISCREPANCIES = "discrepancies";
    public static final String MENDELIAN = "mendelian";
    public static final String COUNT = "count";

    public static final String GT_COUNT = "count";
    public static final String GT_VARIANTS = "variants";
    public static final String GT_FILE_INDEX = "fileIdx";
    public static final String GT_FILE_DATA = "fileData";
    public static final String GT_ANNOTATION_INDEX = "annotationIdx";
    public static final String GT_ANNOTATION_COUNTS = "annotationCounts";
    public static final String GT_CT_INDEX = "ctIdx";
    public static final String GT_BT_INDEX = "btIdx";
    public static final String GT_TF_INDEX = "tfIdx";
    public static final String GT_CT_BT_TF_INDEX = "ctBtTfIdx";
    public static final String GT_POP_FREQ_INDEX = "popFreqIdx";
    public static final String GT_CLINICAL_INDEX = "clinicalIdx";
    public static final String GT_PARENTS_INDEX = "parentsIdx";

    public static final String GT_LIST = "gts";

    public static final String KEY_PREFIX_GT = "gt_";
    private static final char KEY_SEPARATOR = '_';

    public static final List<String> GT_FIELDS = Arrays.asList(
            GT_COUNT,
            GT_VARIANTS,
            GT_FILE_INDEX,
            GT_FILE_DATA,
            GT_ANNOTATION_INDEX,
            GT_ANNOTATION_COUNTS,
            GT_CT_INDEX,
            GT_BT_INDEX,
            GT_TF_INDEX,
            GT_CT_BT_TF_INDEX,
            GT_POP_FREQ_INDEX,
            GT_CLINICAL_INDEX,
            GT_PARENTS_INDEX
    );
    public static final List<String> BINARY_FIELDS = Arrays.asList(
            GT_VARIANTS,
            GT_FILE_INDEX,
            GT_FILE_DATA,
            GT_ANNOTATION_INDEX,
            GT_CT_INDEX,
            GT_BT_INDEX,
            GT_TF_INDEX,
            GT_CT_BT_TF_INDEX,
            GT_POP_FREQ_INDEX,
            GT_CLINICAL_INDEX,
            GT_PARENTS_INDEX
    );

    @Override
    public SampleIndexEntry convertToDataModelType(Document object) {
        if (object == null) {
            return null;
        }
        SampleIndexEntry entry = new SampleIndexEntry(
                object.getInteger(SAMPLE_ID, -1),
                object.getString(CHROMOSOME),
                object.getInteger(BATCH_START, 0));
        entry.setDiscrepancies(object.getInteger(DISCREPANCIES, 0));

        Binary mendelian = object.get(MENDELIAN, Binary.class);
        if (mendelian != null && mendelian.length() > 0) {
            byte[] data = mendelian.getData();
            entry.setMendelianVariants(data, 0, data.length);
        }

        Map<String, SampleIndexEntry.SampleIndexGtEntry> gts = new HashMap<>();
        List<String> gtList = object.getList(GT_LIST, String.class);
        if (gtList != null) {
            for (String gt : gtList) {
                gts.computeIfAbsent(gt, SampleIndexEntry.SampleIndexGtEntry::new);
            }
        }
        for (String key : object.keySet()) {
            if (!key.startsWith(KEY_PREFIX_GT)) {
                continue;
            }
            String remainder = key.substring(KEY_PREFIX_GT.length());
            int idx = remainder.indexOf(KEY_SEPARATOR);
            if (idx <= 0 || idx >= remainder.length() - 1) {
                continue;
            }
            String gt = gtFromKey(remainder.substring(0, idx));
            String field = remainder.substring(idx + 1);
            SampleIndexEntry.SampleIndexGtEntry gtEntry = gts.get(gt);
            if (gtEntry == null) {
                // This should not happen as we have already created the GT entries from the GT_LIST
                // Ignore this entry, as it might be a leftover from a previous version or a deleted GT
                continue;
            }
            applyGtField(gtEntry, field, object.get(key));
        }
        entry.setGts(gts);
        return entry;
    }

    @Override
    public Document convertToStorageType(SampleIndexEntry object) {
        if (object == null) {
            return null;
        }
        Document document = new Document()
                .append("_id", buildDocumentId(object.getSampleId(), object.getChromosome(), object.getBatchStart()))
                .append(SAMPLE_ID, object.getSampleId())
                .append(CHROMOSOME, object.getChromosome())
                .append(BATCH_START, object.getBatchStart())
                .append(DISCREPANCIES, object.getDiscrepancies());

        putByteSlice(document, MENDELIAN, object.getMendelianVariantsValue(),
                object.getMendelianVariantsOffset(), object.getMendelianVariantsLength());

        if (object.getGts() != null) {
            int count = 0;
            List<String> gts = new ArrayList<>(object.getGts().keySet());
            document.append(GT_LIST, gts);
            for (String gt : gts) {
                SampleIndexEntry.SampleIndexGtEntry gtEntry = object.getGts().get(gt);
                if (gtEntry.getCount() > 0) {
                    putGtField(document, gt, GT_COUNT, gtEntry.getCount());
                    count += gtEntry.getCount();
                }
                putGtField(document, gt, GT_VARIANTS,
                        toBinary(gtEntry.getVariants(), gtEntry.getVariantsOffset(), gtEntry.getVariantsLength()));
                putGtField(document, gt, GT_FILE_INDEX,
                        toBinary(gtEntry.getFileIndex(), gtEntry.getFileIndexOffset(), gtEntry.getFileIndexLength()));
                putGtField(document, gt, GT_FILE_DATA,
                        toBinary(gtEntry.getFileData(), gtEntry.getFileDataOffset(), gtEntry.getFileDataLength()));
                putGtField(document, gt, GT_ANNOTATION_INDEX,
                        toBinary(gtEntry.getAnnotationIndex(), gtEntry.getAnnotationIndexOffset(),
                                gtEntry.getAnnotationIndexLength()));
                if (gtEntry.getAnnotationCounts() != null) {
                    putGtField(document, gt, GT_ANNOTATION_COUNTS,
                            Arrays.stream(gtEntry.getAnnotationCounts()).boxed().collect(Collectors.toList()));
                } else {
                    putGtField(document, gt, GT_ANNOTATION_COUNTS, null);
                }
                putGtField(document, gt, GT_CT_INDEX,
                        toBinary(gtEntry.getConsequenceTypeIndex(), gtEntry.getConsequenceTypeIndexOffset(),
                                gtEntry.getConsequenceTypeIndexLength()));
                putGtField(document, gt, GT_BT_INDEX,
                        toBinary(gtEntry.getBiotypeIndex(), gtEntry.getBiotypeIndexOffset(),
                                gtEntry.getBiotypeIndexLength()));
                putGtField(document, gt, GT_TF_INDEX,
                        toBinary(gtEntry.getTranscriptFlagIndex(), gtEntry.getTranscriptFlagIndexOffset(),
                                gtEntry.getTranscriptFlagIndexLength()));
                putGtField(document, gt, GT_CT_BT_TF_INDEX,
                        toBinary(gtEntry.getCtBtTfIndex(), gtEntry.getCtBtTfIndexOffset(),
                                gtEntry.getCtBtTfIndexLength()));
                putGtField(document, gt, GT_POP_FREQ_INDEX,
                        toBinary(gtEntry.getPopulationFrequencyIndex(),
                                gtEntry.getPopulationFrequencyIndexOffset(),
                                gtEntry.getPopulationFrequencyIndexLength()));
                putGtField(document, gt, GT_CLINICAL_INDEX,
                        toBinary(gtEntry.getClinicalIndex(), gtEntry.getClinicalIndexOffset(),
                                gtEntry.getClinicalIndexLength()));
                putGtField(document, gt, GT_PARENTS_INDEX,
                        toBinary(gtEntry.getParentsIndex(), gtEntry.getParentsIndexOffset(),
                                gtEntry.getParentsIndexLength()));
            }
            if (count > 0) {
                document.put(COUNT, count);
            }
        }
        return document;
    }

    public Map<String, TreeSet<SampleIndexVariant>> convertToGtVariantMap(Document document, SampleIndexSchema schema) {
        SampleIndexEntry entry = convertToDataModelType(document);
        if (entry.getGts() == null) {
            return new HashMap<>();
        }
        Map<String, TreeSet<SampleIndexVariant>> map = new HashMap<>();

        SampleIndexVariantBiConverter biConverter = new SampleIndexVariantBiConverter(schema);
        SampleIndexVariant.SampleIndexVariantComparator comparator
                = new SampleIndexVariant.SampleIndexVariantComparator(schema);

        for (Map.Entry<String, SampleIndexEntry.SampleIndexGtEntry> gtEntry : entry.getGts().entrySet()) {
            byte[] variantsBytes = gtEntry.getValue().getVariants();
            if (variantsBytes == null) {
                continue;
            }
            SampleIndexEntryIterator iterator = biConverter.toVariantsIterator(entry, gtEntry.getKey());
            TreeSet<SampleIndexVariant> variants = new TreeSet<>(comparator);
            while (iterator.hasNext()) {
                SampleIndexVariant sampleIndexVariant = iterator.nextSampleIndexVariant();
                variants.add(sampleIndexVariant);
            }
            map.put(gtEntry.getKey(), variants);
        }
        return map;
    }

    private void applyGtField(SampleIndexEntry.SampleIndexGtEntry gtEntry, String field, Object value) {
        if (value == null) {
            return;
        }
        switch (field) {
            case GT_COUNT:
                if (value instanceof Number) {
                    gtEntry.setCount(((Number) value).intValue());
                }
                break;
            case GT_ANNOTATION_COUNTS:
                List<?> list = (List<?>) value;
                gtEntry.setAnnotationCounts(list.stream()
                        .mapToInt(o -> ((Number) o).intValue())
                        .toArray());
                break;
            default:
                Binary binary = (Binary) value;
                byte[] data = binary.getData();
                switch (field) {
                    case GT_VARIANTS:
                        gtEntry.setVariants(data, 0, data.length);
                        break;
                    case GT_FILE_INDEX:
                        gtEntry.setFileIndex(data, 0, data.length);
                        break;
                    case GT_FILE_DATA:
                        gtEntry.setFileDataIndex(data, 0, data.length);
                        break;
                    case GT_ANNOTATION_INDEX:
                        gtEntry.setAnnotationIndex(data, 0, data.length);
                        break;
                    case GT_CT_INDEX:
                        gtEntry.setConsequenceTypeIndex(data, 0, data.length);
                        break;
                    case GT_BT_INDEX:
                        gtEntry.setBiotypeIndex(data, 0, data.length);
                        break;
                    case GT_TF_INDEX:
                        gtEntry.setTranscriptFlagIndex(data, 0, data.length);
                        break;
                    case GT_CT_BT_TF_INDEX:
                        gtEntry.setCtBtTfIndex(data, 0, data.length);
                        break;
                    case GT_POP_FREQ_INDEX:
                        gtEntry.setPopulationFrequencyIndex(data, 0, data.length);
                        break;
                    case GT_CLINICAL_INDEX:
                        gtEntry.setClinicalIndex(data, 0, data.length);
                        break;
                    case GT_PARENTS_INDEX:
                        gtEntry.setParentsIndex(data, 0, data.length);
                        break;
                    default:
                        throw new IllegalStateException("Unsupported GT field '" + field + "'");
                }
                break;
        }
    }

    private Binary toBinary(byte[] bytes, int offset, int length) {
        if (bytes == null || length == 0) {
            return null;
        }
        byte[] slice = Arrays.copyOfRange(bytes, offset, offset + length);
        return new Binary(slice);
    }

    private static String gtToKey(String gt) {
        return gt.replace('/', 'v').replace('|', 'p');
    }

    private static String gtFromKey(String gtKey) {
        return gtKey.replace('v', '/').replace('p', '|');
    }

    private void putGtField(Document document, String gt, String field, Object value) {
        String key = getGenotypeField(gt, field);
        if (value == null) {
            document.remove(key);
        } else {
            document.put(key, value);
        }
    }

    public static String getGenotypeField(String gt, String field) {
        return KEY_PREFIX_GT + gtToKey(gt) + KEY_SEPARATOR + field;
    }

    private void putByteSlice(Document document, String key, byte[] bytes, int offset, int length) {
        Binary binary = toBinary(bytes, offset, length);
        if (binary == null) {
            document.remove(key);
        } else {
            document.put(key, binary);
        }
    }

    public static String buildDocumentId(SampleIndexEntry entry) {
        return buildDocumentId(entry.getSampleId(), entry.getChromosome(), entry.getBatchStart());
    }

    public static String buildDocumentId(int sampleId, String chromosome, int batchStart) {
        return String.format("%d_%s_%010d", sampleId, chromosome, batchStart);
    }
}
