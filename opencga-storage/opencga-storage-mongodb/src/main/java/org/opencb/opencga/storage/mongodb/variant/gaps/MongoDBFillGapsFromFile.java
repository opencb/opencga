package org.opencb.opencga.storage.mongodb.variant.gaps;

import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Updates;
import org.apache.commons.lang3.time.StopWatch;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.VariantFileMetadata;
import org.opencb.biodata.models.variant.avro.SampleEntry;
import org.opencb.biodata.tools.variant.VariantSorterTask;
import org.opencb.biodata.tools.variant.VariantVcfHtsjdkReader;
import org.opencb.commons.ProgressLogger;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.mongodb.MongoDBCollection;
import org.opencb.commons.io.DataReader;
import org.opencb.commons.run.Task;
import org.opencb.commons.utils.CompressionUtils;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.io.plain.StringDataReader;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.metadata.models.FileMetadata;
import org.opencb.opencga.storage.core.metadata.models.StudyMetadata;
import org.opencb.opencga.storage.core.variant.VariantStorageOptions;
import org.opencb.opencga.storage.core.variant.gaps.VcfFileVariantIterator;
import org.opencb.opencga.storage.core.variant.gaps.VariantOverlappingStatus;
import org.opencb.opencga.storage.core.variant.index.sample.schema.SampleIndexSchema;
import org.opencb.opencga.storage.core.variant.io.VariantReaderUtils;
import org.opencb.opencga.storage.core.variant.transform.VariantNormalizerFactory;
import org.opencb.opencga.storage.mongodb.variant.MongoDBVariantStorageOptions;
import org.opencb.opencga.storage.mongodb.variant.adaptors.VariantMongoDBAdaptor;
import org.opencb.opencga.storage.mongodb.variant.converters.AbstractDocumentConverter;
import org.opencb.opencga.storage.mongodb.variant.converters.DocumentToSamplesConverter;
import org.opencb.opencga.storage.mongodb.variant.converters.DocumentToVariantConverter;
import org.opencb.opencga.storage.mongodb.variant.protobuf.VariantMongoDBProto;
import org.opencb.opencga.storage.mongodb.variant.search.MongoDBVariantSearchIndexUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.zip.GZIPInputStream;

import static org.opencb.opencga.storage.mongodb.variant.converters.DocumentToStudyEntryConverter.*;

/**
 * Orchestrates gap-filling from VCF files and writes results to MongoDB.
 * Mirrors the flow of hadoop's FillGapsFromFile but writes to MongoDB instead of HBase.
 */
public class MongoDBFillGapsFromFile {

    private static final int BULK_SIZE = 500;

    private final VariantMongoDBAdaptor dbAdaptor;
    private final VariantStorageMetadataManager metadataManager;
    private final ObjectMap options;
    private final Logger logger = LoggerFactory.getLogger(MongoDBFillGapsFromFile.class);

    public MongoDBFillGapsFromFile(VariantMongoDBAdaptor dbAdaptor,
                                   VariantStorageMetadataManager metadataManager,
                                   ObjectMap options) {
        this.dbAdaptor = dbAdaptor;
        this.metadataManager = metadataManager;
        this.options = options;
    }

    /**
     * Fill gaps from VCF files and write results to MongoDB.
     *
     * @param study      Study name or ID
     * @param inputFiles VCF file URIs
     * @param gapsGenotype Genotype to use for gaps (e.g. "0/0")
     * @throws StorageEngineException on error
     */
    public void fillGaps(String study, List<URI> inputFiles, String gapsGenotype)
            throws StorageEngineException {
        try {
            StudyMetadata studyMetadata = metadataManager.getStudyMetadata(study);
            int studyId = studyMetadata.getId();
            StopWatch stopWatch = StopWatch.createStarted();

            MongoDBFillGapsTask fillGapsTask = new MongoDBFillGapsTask(
                    metadataManager, studyMetadata, false, gapsGenotype);

            // Build sample name→id map and file→sampleIds list for converter
            Map<String, Integer> sampleIdsMap = new HashMap<>();
            Map<Integer, List<Integer>> fileSampleIdsMap = new HashMap<>();
            for (URI inputFile : inputFiles) {
                File file = Paths.get(inputFile).toFile();
                String fileName = file.getName();
                int fileId = metadataManager.getFileId(studyMetadata.getId(), fileName);
                FileMetadata fileMetadata = metadataManager.getFileMetadata(studyMetadata.getId(), fileId);
                fileSampleIdsMap.put(fileId, new ArrayList<>(fileMetadata.getSamples()));
                for (Integer sampleId : fileMetadata.getSamples()) {
                    String sampleName = metadataManager.getSampleName(studyMetadata.getId(), sampleId);
                    sampleIdsMap.put(sampleName, sampleId);
                }
            }

            long filesLengthBytes = 0;
            List<VcfFileVariantIterator> fileIterators = new ArrayList<>();
            for (URI inputFile : inputFiles) {
                File file = Paths.get(inputFile).toFile();
                if (!file.exists()) {
                    throw new FileNotFoundException("File not found: " + file);
                }
                long fileLength = file.length();
                filesLengthBytes += fileLength;
                String fileName = file.getName();
                int fileId = metadataManager.getFileId(studyMetadata.getId(), fileName);
                FileMetadata fileMetadata = metadataManager.getFileMetadata(studyMetadata.getId(), fileId);
                LinkedHashSet<Integer> sampleIds = fileMetadata.getSamples();
                VariantFileMetadata variantFileMetadata =
                        metadataManager.getVariantFileMetadata(studyMetadata.getId(), fileId);

                InputStream inputStream = Files.newInputStream(file.toPath());
                StringDataReader.SizeInputStream sizeInputStream =
                        new StringDataReader.SizeInputStream(inputStream, fileLength);
                inputStream = sizeInputStream;
                if (fileName.endsWith(".gz")) {
                    inputStream = new GZIPInputStream(inputStream);
                }

                DataReader<Variant> reader = new VariantVcfHtsjdkReader(inputStream,
                        new VariantFileMetadata(Integer.toString(fileId), Integer.toString(fileId))
                                .toVariantStudyMetadata(studyMetadata.getName()));
                ObjectMap thisOptions = new ObjectMap(this.options);
                if (VariantReaderUtils.isGvcf(fileName)
                        || fileMetadata.getAttributes().getBoolean(VariantStorageOptions.GVCF.key(), false)) {
                    logger.info("GVCF file detected: " + fileName);
                    thisOptions.put(VariantStorageOptions.GVCF.key(), true);
                }
                Task<Variant, Variant> normalizer =
                        new VariantNormalizerFactory(thisOptions).newNormalizer(variantFileMetadata);
                reader = reader.then(normalizer);
                VariantSorterTask sorter = new VariantSorterTask(100, SampleIndexSchema.VARIANT_COMPARATOR);
                reader = reader.then(sorter);
                fileIterators.add(new VcfFileVariantIterator(file, fileName, fileId, sampleIds,
                        reader.iterator(), sizeInputStream, 10000));
            }

            int numVariants = 0;
            int numUpdates = 0;
            AbstractDocumentConverter idConverter = new AbstractDocumentConverter() { };
            ProgressLogger progressLogger = new ProgressLogger("Processed", filesLengthBytes, 100);
            MongoDBCollection variantsCollection = dbAdaptor.getVariantsCollection();
            long ts = System.currentTimeMillis();

            List<Bson> bulkQueries = new ArrayList<>(BULK_SIZE);
            List<Bson> bulkUpdates = new ArrayList<>(BULK_SIZE);
            QueryOptions queryOptions = new QueryOptions();

            String chromosome = getNextChromosome(fileIterators);
            while (chromosome != null) {
                final String chr = chromosome;
                for (VcfFileVariantIterator fileIterator : fileIterators) {
                    fileIterator.setChromosome(chr);
                }
                Variant variant = getNextVariant(fileIterators, null);
                EnumMap<VariantOverlappingStatus, Integer> statusCount =
                        new EnumMap<>(VariantOverlappingStatus.class);

                while (variant != null) {
                    Integer start = variant.getStart();
                    numVariants++;

                    List<Document> gapFileDocs = new ArrayList<>();
                    for (VcfFileVariantIterator fileIterator : fileIterators) {
                        VariantOverlappingStatus status = fillGapsTask.fillGaps(
                                variant, fileIterator.getSampleIds(),
                                fileIterator.getFileId(), fileIterator);
                        statusCount.merge(status, 1, Integer::sum);
                        fileIterator.trim();
                        progressLogger.increment(fileIterator.getReadBytes(),
                                () -> " Bytes up to position " + chr + ":" + start);

                        // Retrieve results from the task
                        List<MongoDBFillGapsTask.FillGapsResult> results = fillGapsTask.getAndClearResults();
                        for (MongoDBFillGapsTask.FillGapsResult result : results) {
                            if (result.getStatus() == VariantOverlappingStatus.NONE) {
                                continue;
                            }
                            Document fileDoc = buildFileDocument(
                                    studyId, fileIterator.getFileId(),
                                    result.getFilledVariant(), result.getMissingSamples(),
                                    sampleIdsMap, studyMetadata,
                                    fileSampleIdsMap.get(fileIterator.getFileId()));
                            if (fileDoc != null) {
                                gapFileDocs.add(fileDoc);
                            }
                        }
                    }

                    if (!gapFileDocs.isEmpty()) {
                        String storageId = idConverter.buildStorageId(variant);
                        Bson filter = Filters.eq("_id", storageId);
                        List<Bson> updates = new ArrayList<>();
                        updates.add(Updates.pushEach(DocumentToVariantConverter.FILES_FIELD, gapFileDocs));
                        updates.add(MongoDBVariantSearchIndexUtils.getSetIndexUnknown(ts));
                        bulkQueries.add(filter);
                        bulkUpdates.add(Updates.combine(updates));
                        numUpdates++;
                    }

                    if (bulkQueries.size() >= BULK_SIZE) {
                        variantsCollection.update(bulkQueries, bulkUpdates, queryOptions);
                        bulkQueries.clear();
                        bulkUpdates.clear();
                    }

                    variant = getNextVariant(fileIterators, variant);
                }
                logger.info("chr " + chr + " , numVariants = " + numVariants
                        + " , numUpdates = " + numUpdates + " " + statusCount);
                chromosome = getNextChromosome(fileIterators);
            }

            // Flush remaining
            if (!bulkQueries.isEmpty()) {
                variantsCollection.update(bulkQueries, bulkUpdates, queryOptions);
                bulkQueries.clear();
                bulkUpdates.clear();
            }

            logger.info("Fill gaps finished! " + numVariants + " variants processed, "
                    + numUpdates + " updates written");
            logger.info("Time taken: " + TimeUtils.durationToString(stopWatch));
        } catch (IOException e) {
            throw new StorageEngineException("Error computing aggregation family operation", e);
        }
    }

    /**
     * Build a MongoDB file document from a gap-filled variant.
     */
    private Document buildFileDocument(int studyId, int fileId, Variant filledVariant,
                                       Set<Integer> missingSamples, Map<String, Integer> sampleIdsMap,
                                       StudyMetadata studyMetadata, List<Integer> fileSampleIds) {
        if (filledVariant == null || filledVariant.getStudies().isEmpty()) {
            return null;
        }

        StudyEntry filledStudy = filledVariant.getStudies().get(0);
        Document fileDoc = new Document(FILEID_FIELD, fileId)
                .append(STUDYID_FIELD, studyId);

        // Build mgt: genotype → [sampleIds]
        Document mgt = new Document();
        List<SampleEntry> samples = filledStudy.getSamples();
        List<String> orderedSampleNames = filledStudy.getOrderedSamplesName();

        int sampleIdx = 0;
        Map<String, List<Integer>> genotypeCodes = new HashMap<>();
        for (SampleEntry sampleEntry : samples) {
            String sampleName = orderedSampleNames.get(sampleIdx);
            sampleIdx++;
            String genotype = sampleEntry.getData().get(0);
            if (genotype == null) {
                genotype = ".";
            }
            Integer sampleId = sampleIdsMap.get(sampleName);
            if (sampleId != null) {
                genotypeCodes.computeIfAbsent(genotype, k -> new ArrayList<>()).add(sampleId);
            }
        }

        for (Map.Entry<String, List<Integer>> entry : genotypeCodes.entrySet()) {
            mgt.append(DocumentToSamplesConverter.genotypeToStorageType(entry.getKey()), entry.getValue());
        }
        fileDoc.append(FILE_GENOTYPE_FIELD, mgt);

        // Build sampleData for extra FORMAT fields (AD, DP, GQ, etc.)
        buildSampleData(fileDoc, filledStudy, studyMetadata, fileSampleIds);

        // Build secondary alternates if present
        if (!filledStudy.getSecondaryAlternates().isEmpty()) {
            List<Document> alternates = new ArrayList<>();
            for (org.opencb.biodata.models.variant.avro.AlternateCoordinate alt
                    : filledStudy.getSecondaryAlternates()) {
                Document altDoc = new Document();
                altDoc.put(ALTERNATES_CHR, alt.getChromosome() != null
                        ? alt.getChromosome() : filledVariant.getChromosome());
                altDoc.put(ALTERNATES_REF, alt.getReference() != null
                        ? alt.getReference() : filledVariant.getReference());
                altDoc.put(ALTERNATES_ALT, alt.getAlternate());
                altDoc.put(ALTERNATES_START, alt.getStart() != null
                        ? alt.getStart() : filledVariant.getStart());
                altDoc.put(ALTERNATES_END, alt.getEnd() != null
                        ? alt.getEnd() : filledVariant.getEnd());
                altDoc.put(ALTERNATES_TYPE, alt.getType() != null
                        ? alt.getType().toString() : filledVariant.getType().toString());
                alternates.add(altDoc);
            }
            fileDoc.append(ALTERNATES_FIELD, alternates);
        }

        // Build call and attrs from file data if present
        if (!filledStudy.getFiles().isEmpty()) {
            org.opencb.biodata.models.variant.avro.FileEntry fe =
                    filledStudy.getFiles().get(0);

            // Store original call (_ori)
            if (fe.getCall() != null) {
                fileDoc.append(ORI_FIELD,
                        new Document("s", fe.getCall().getVariantId())
                                .append("i", fe.getCall().getAlleleIndex()));
            }

            if (fe.getData() != null && !fe.getData().isEmpty()) {
                Document attrs = new Document();
                for (Map.Entry<String, String> attrEntry : fe.getData().entrySet()) {
                    String key = attrEntry.getKey();
                    if ("src".equals(key)) {
                        continue;
                    }
                    String stringValue = attrEntry.getValue();
                    Object value = stringValue;
                    try {
                        value = Integer.parseInt(stringValue);
                    } catch (NumberFormatException notAnInt) {
                        try {
                            value = Long.parseLong(stringValue);
                        } catch (NumberFormatException notALong) {
                            try {
                                value = Double.parseDouble(stringValue);
                            } catch (NumberFormatException notADouble) {
                                // leave as string
                            }
                        }
                    }
                    attrs.append(key, value);
                }
                if (!attrs.isEmpty()) {
                    fileDoc.append(ATTRIBUTES_FIELD, attrs);
                }
            }
        }

        return fileDoc;
    }

    /**
     * Encode extra FORMAT fields (AD, DP, GQ, etc.) as protobuf binary in the sampleData sub-document.
     * Values are ordered by file sample position (matching the decode order in DocumentToSamplesConverter).
     */
    private void buildSampleData(Document fileDoc, StudyEntry filledStudy,
                                  StudyMetadata studyMetadata, List<Integer> fileSampleIds) {
        List<String> extraFields = studyMetadata.getAttributes()
                .getAsStringList(VariantStorageOptions.EXTRA_FORMAT_FIELDS.key());
        if (extraFields.isEmpty()) {
            return;
        }
        List<String> extraFieldsType = studyMetadata.getAttributes()
                .getAsStringList(VariantStorageOptions.EXTRA_FORMAT_FIELDS_TYPE.key());
        boolean compressExtraParams = studyMetadata.getAttributes()
                .getBoolean(MongoDBVariantStorageOptions.EXTRA_GENOTYPE_FIELDS_COMPRESS.key(),
                        MongoDBVariantStorageOptions.EXTRA_GENOTYPE_FIELDS_COMPRESS.defaultValue());

        // Map filled variant's sample names to their position index
        Map<String, Integer> filledSamplePositions = new HashMap<>();
        List<String> filledSampleNames = filledStudy.getOrderedSamplesName();
        for (int i = 0; i < filledSampleNames.size(); i++) {
            filledSamplePositions.put(filledSampleNames.get(i), i);
        }

        Document dataFields = new Document();
        for (int i = 0; i < extraFields.size(); i++) {
            String extraField = extraFields.get(i);
            String extraFieldType = i < extraFieldsType.size() ? extraFieldsType.get(i) : "String";

            Integer formatIdx = filledStudy.getSampleDataKeyPosition(extraField);
            if (formatIdx == null) {
                // This extra field is not present in the filled variant
                continue;
            }

            VariantMongoDBProto.OtherFields.Builder builder = VariantMongoDBProto.OtherFields.newBuilder();

            // Iterate ALL samples in file-metadata order (decoder expects this ordering)
            for (Integer sampleId : fileSampleIds) {
                String sampleName = metadataManager.getSampleName(studyMetadata.getId(), sampleId);
                Integer filledIdx = filledSamplePositions.get(sampleName);
                String stringValue;
                if (filledIdx != null) {
                    stringValue = filledStudy.getSamples().get(filledIdx).getData().get(formatIdx);
                } else {
                    stringValue = null;
                }
                switch (extraFieldType) {
                    case "Integer":
                        builder.addIntValues(
                                DocumentToSamplesConverter.INTEGER_COMPLEX_TYPE_CONVERTER.convertToStorageType(stringValue));
                        break;
                    case "Float":
                        builder.addFloatValues(
                                DocumentToSamplesConverter.FLOAT_COMPLEX_TYPE_CONVERTER.convertToStorageType(stringValue));
                        break;
                    case "String":
                    default:
                        builder.addStringValues(stringValue != null ? stringValue : ".");
                        break;
                }
            }

            byte[] byteArray = builder.build().toByteArray();
            if (compressExtraParams && byteArray.length > 50) {
                try {
                    byteArray = CompressionUtils.compress(byteArray);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }
            dataFields.append(extraField.toLowerCase(), byteArray);
        }

        if (!dataFields.isEmpty()) {
            fileDoc.append(SAMPLE_DATA_FIELD, dataFields);
        }
    }

    private static String getNextChromosome(List<VcfFileVariantIterator> fileIterators) {
        for (VcfFileVariantIterator fileIterator : fileIterators) {
            String chromosome = fileIterator.getNextChromosome();
            if (chromosome != null) {
                return chromosome;
            }
        }
        return null;
    }

    private Variant getNextVariant(List<VcfFileVariantIterator> files, Variant prevVariant) {
        Variant nextVariant = null;
        for (VcfFileVariantIterator file : files) {
            Variant variant = file.getNextVariant(prevVariant);
            if (variant != null) {
                if (nextVariant == null) {
                    nextVariant = variant;
                } else if (SampleIndexSchema.INTRA_CHROMOSOME_VARIANT_COMPARATOR
                        .compare(variant, nextVariant) < 0) {
                    nextVariant = variant;
                }
            }
        }
        return nextVariant;
    }
}
