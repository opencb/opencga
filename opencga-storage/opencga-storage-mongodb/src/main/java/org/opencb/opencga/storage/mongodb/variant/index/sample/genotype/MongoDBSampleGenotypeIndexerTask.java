package org.opencb.opencga.storage.mongodb.variant.index.sample.genotype;

import org.bson.Document;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.AlternateCoordinate;
import org.opencb.biodata.models.variant.avro.OriginalCall;
import org.opencb.biodata.models.variant.avro.VariantType;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.run.Task;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.metadata.models.SampleMetadata;
import org.opencb.opencga.storage.core.variant.index.sample.SampleIndexDBAdaptor;
import org.opencb.opencga.storage.core.variant.index.sample.genotype.SampleIndexEntryBuilder;
import org.opencb.opencga.storage.core.variant.index.sample.genotype.SampleIndexVariantConverter;
import org.opencb.opencga.storage.core.variant.index.sample.models.SampleIndexEntry;
import org.opencb.opencga.storage.core.variant.index.sample.models.SampleIndexEntryChunk;
import org.opencb.opencga.storage.core.variant.index.sample.models.SampleIndexVariant;
import org.opencb.opencga.storage.core.variant.index.sample.schema.SampleIndexSchema;
import org.opencb.opencga.storage.mongodb.variant.MongoDBVariantStorageOptions;
import org.opencb.opencga.storage.mongodb.variant.converters.DocumentToSamplesConverter;
import org.opencb.opencga.storage.mongodb.variant.converters.DocumentToStudyEntryConverter;
import org.opencb.opencga.storage.mongodb.variant.converters.DocumentToVariantConverter;

import java.util.*;
import java.util.stream.Collectors;

import static org.opencb.opencga.storage.core.variant.index.sample.schema.SampleIndexSchema.getChunkStart;
import static org.opencb.opencga.storage.core.variant.index.sample.schema.SampleIndexSchema.validGenotype;

/**
 * MongoDB-specific {@link Task} that builds sample-index entries directly from raw MongoDB Documents
 * returned by {@link org.opencb.opencga.storage.mongodb.variant.adaptors.VariantMongoDBAdaptor#nativeIterator}.
 *
 * <p>Unlike {@link org.opencb.opencga.storage.core.variant.index.sample.genotype.SampleGenotypeIndexerTask},
 * this class operates on unmerged data: each file document is processed independently, so samples
 * that appear in multiple files are indexed once per file, exactly as they were written during the
 * original load. This ensures that rebuilding the sample index is idempotent and produces the same
 * result as the initial load.
 */
public class MongoDBSampleGenotypeIndexerTask implements Task<Document, SampleIndexEntry> {

    private final int studyId;
    private final List<Integer> sampleIds;
    private final SampleIndexSchema schema;
    private final boolean compressExtraParams;
    private final SampleIndexVariantConverter converter;
    private final VariantStorageMetadataManager metadataManager;

    /** sampleId → index in {@code sampleIds} list. */
    private final Map<Integer, Integer> sampleIdToIdx;
    /** [sampleIdx] → (fileId → filePosition). */
    @SuppressWarnings("unchecked")
    private final Map<Integer, Integer>[] fileIdxMap;
    /** [sampleIdx] — whether this sample has multiple files. */
    private final boolean[] multiFileIndex;

    /** cache: fileId → ordered sample-id list. */
    private final Map<Integer, List<Integer>> samplesInFileCache = new HashMap<>();

    /** Buffer: SampleIndexEntryChunk → one SampleIndexEntryBuilder per sampleId. */
    private final Map<SampleIndexEntryChunk, List<SampleIndexEntryBuilder>> buffer = new LinkedHashMap<>();

    public MongoDBSampleGenotypeIndexerTask(SampleIndexDBAdaptor dbAdaptor,
                                            int studyId, List<Integer> sampleIds,
                                            ObjectMap options, SampleIndexSchema schema) {
        this.studyId = studyId;
        this.sampleIds = sampleIds;
        this.schema = schema;
        this.converter = new SampleIndexVariantConverter(schema);
        this.metadataManager = dbAdaptor.getMetadataManager();

        boolean compress = metadataManager.getStudyMetadata(studyId).getAttributes()
                .getBoolean(MongoDBVariantStorageOptions.EXTRA_GENOTYPE_FIELDS_COMPRESS.key(),
                        MongoDBVariantStorageOptions.EXTRA_GENOTYPE_FIELDS_COMPRESS.defaultValue());
        this.compressExtraParams = compress;

        sampleIdToIdx = new HashMap<>(sampleIds.size());
        for (int i = 0; i < sampleIds.size(); i++) {
            sampleIdToIdx.put(sampleIds.get(i), i);
        }

        fileIdxMap = new Map[sampleIds.size()];
        multiFileIndex = new boolean[sampleIds.size()];
        for (int i = 0; i < sampleIds.size(); i++) {
            Integer sampleId = sampleIds.get(i);
            SampleMetadata sampleMetadata = metadataManager.getSampleMetadata(studyId, sampleId);
            List<Integer> files = sampleMetadata.getFiles();
            Map<Integer, Integer> map = new HashMap<>(files.size());
            for (int idx = 0; idx < files.size(); idx++) {
                map.put(files.get(idx), idx);
            }
            fileIdxMap[i] = map;
            multiFileIndex[i] = files.size() > 1;
        }
    }

    @Override
    public List<SampleIndexEntry> apply(List<Document> docs) {
        for (Document doc : docs) {
            String typeStr = doc.getString(DocumentToVariantConverter.TYPE_FIELD);
            if (VariantType.NO_VARIATION.name().equals(typeStr)) {
                continue;
            }

            Variant variant = buildVariant(doc);
            SampleIndexEntryChunk indexChunk = new SampleIndexEntryChunk(
                    variant.getChromosome(), getChunkStart(variant.getStart()));

            List<Document> fileDocs = doc.getList(DocumentToVariantConverter.FILES_FIELD, Document.class);
            if (fileDocs == null) {
                continue;
            }
            for (Document fileDoc : fileDocs) {
                Integer sid = fileDoc.getInteger(DocumentToStudyEntryConverter.STUDYID_FIELD);
                if (sid == null || sid != studyId) {
                    continue;
                }
                int fid = fileDoc.getInteger(DocumentToStudyEntryConverter.FILEID_FIELD);
                if (fid < 0) {
                    fid = -fid;
                }

                Document mgt = fileDoc.get(DocumentToStudyEntryConverter.FILE_GENOTYPE_FIELD, Document.class);
                if (mgt == null) {
                    continue;
                }

                // Parse file-level data once per file
                OriginalCall call = parseOriginalCall(fileDoc);
                List<AlternateCoordinate> alts = parseAlternates(fileDoc);
                Document attrsDoc = fileDoc.get(DocumentToStudyEntryConverter.ATTRIBUTES_FIELD, Document.class);
                Document sampleDataDoc = fileDoc.get(DocumentToStudyEntryConverter.SAMPLE_DATA_FIELD, Document.class);

                // TODO: Convert each file to a partial SampleIndexVariant, and then addSampleDataIndexValues to it
                //    See SampleIndexDriver#SampleIndexerMapper#map() for an example of how to convert a Variant
                //    to a SampleIndexVariant reusing the fileIndex

                // Cache samples-in-file list for sampleData position lookup
                List<Integer> samplesInFile = samplesInFileCache.computeIfAbsent(
                        fid, f -> new ArrayList<>(metadataManager.getFileMetadata(studyId, f).getSamples()));

                for (Map.Entry<String, Object> mgtEntry : mgt.entrySet()) {
                    String gt = DocumentToSamplesConverter.genotypeToDataModelType(mgtEntry.getKey());
                    if (!validGenotype(gt)) {
                        continue;
                    }
                    @SuppressWarnings("unchecked")
                    List<Integer> mgtSampleIds = (List<Integer>) mgtEntry.getValue();
                    for (Integer sampleId : mgtSampleIds) {
                        Integer sampleIdx = sampleIdToIdx.get(sampleId);
                        if (sampleIdx == null) {
                            continue;
                        }
                        Integer filePosition = fileIdxMap[sampleIdx].get(fid);
                        if (filePosition == null) {
                            continue;
                        }

                        int samplePosInFile = samplesInFile.indexOf(sampleId);
                        final Document finalSampleDataDoc = sampleDataDoc;
                        final int finalSamplePosInFile = samplePosInFile;

                        SampleIndexVariant entry = converter.createSampleIndexVariant(
                                filePosition, variant, call, alts,
                                k -> {
                                    if (attrsDoc == null) {
                                        return null;
                                    } else {
                                        Object o = attrsDoc.get(k);
                                        if (o == null) {
                                            return null;
                                        } else {
                                            return o.toString();
                                        }
                                    }
                                },
                                // TODO: optimize by extracting sampleData for all samples in file at once, instead of per-sample lookups
                                k -> DocumentToSamplesConverter.extractSampleField(
                                        finalSampleDataDoc, finalSamplePosInFile, k, compressExtraParams));

                        List<SampleIndexEntryBuilder> builders = buffer.computeIfAbsent(indexChunk, c -> {
                            List<SampleIndexEntryBuilder> list = new ArrayList<>(sampleIds.size());
                            for (int i = 0; i < sampleIds.size(); i++) {
                                list.add(new SampleIndexEntryBuilder(sampleIds.get(i),
                                        c.getChromosome(), c.getBatchStart(), schema, false, multiFileIndex[i]));
                            }
                            return list;
                        });
                        builders.get(sampleIdx).add(gt, entry);
                    }
                }
            }
        }
        return convert(3);
    }

    @Override
    public List<SampleIndexEntry> drain() {
        return convert(0);
    }

    private List<SampleIndexEntry> convert(int remain) {
        if (buffer.size() <= remain) {
            return Collections.emptyList();
        }
        List<SampleIndexEntry> entries = new ArrayList<>();
        Iterator<Map.Entry<SampleIndexEntryChunk, List<SampleIndexEntryBuilder>>> it = buffer.entrySet().iterator();
        while (buffer.size() > remain && it.hasNext()) {
            List<SampleIndexEntryBuilder> builders = it.next().getValue();
            for (SampleIndexEntryBuilder builder : builders) {
                if (!builder.isEmpty()) {
                    entries.add(builder.buildEntry());
                }
            }
            it.remove();
        }
        return entries;
    }

    private static Variant buildVariant(Document doc) {
        Variant v = new Variant(
                doc.getString(DocumentToVariantConverter.CHROMOSOME_FIELD),
                doc.getInteger(DocumentToVariantConverter.START_FIELD),
                doc.getInteger(DocumentToVariantConverter.END_FIELD),
                doc.getString(DocumentToVariantConverter.REFERENCE_FIELD),
                doc.getString(DocumentToVariantConverter.ALTERNATE_FIELD));
        String typeStr = doc.getString(DocumentToVariantConverter.TYPE_FIELD);
        if (typeStr != null) {
            v.setType(VariantType.valueOf(typeStr));
        }
        return v;
    }

    private static OriginalCall parseOriginalCall(Document fileDoc) {
        Document ori = fileDoc.get(DocumentToStudyEntryConverter.ORI_FIELD, Document.class);
        if (ori == null) {
            return null;
        }
        return new OriginalCall(ori.getString("s"), ori.getInteger("i"));
    }

    private static List<AlternateCoordinate> parseAlternates(Document fileDoc) {
        List<Document> altDocs = fileDoc.getList(DocumentToStudyEntryConverter.ALTERNATES_FIELD, Document.class);
        if (altDocs == null || altDocs.isEmpty()) {
            return Collections.emptyList();
        }
        return altDocs.stream()
                .map(DocumentToStudyEntryConverter::convertToAlternateCoordinate)
                .collect(Collectors.toList());
    }
}
