package org.opencb.opencga.storage.mongodb.variant.gaps;

import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Updates;
import org.apache.commons.lang3.time.StopWatch;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.VariantFileMetadata;
import org.opencb.biodata.models.variant.avro.FileEntry;
import org.opencb.biodata.tools.variant.VariantSorterTask;
import org.opencb.biodata.tools.variant.VariantVcfHtsjdkReader;
import org.opencb.commons.ProgressLogger;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.mongodb.MongoDBCollection;
import org.opencb.commons.io.DataReader;
import org.opencb.commons.run.Task;
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
import org.opencb.opencga.storage.mongodb.variant.converters.DocumentToVariantConverter;
import org.opencb.opencga.storage.mongodb.variant.converters.SampleToDocumentConverter;
import org.opencb.opencga.storage.mongodb.variant.converters.StudyEntryToDocumentConverter;
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
    /** Key in attrs for the overlapping status (V/R/G/M). */
    public static final String OVERLAPPING_STATUS_KEY = "_ovs";

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

            // Build sample name→id map and file→sampleNames list for converter
            Map<String, Integer> sampleIdsMap = new HashMap<>();
            Map<Integer, LinkedHashSet<String>> fileSampleNamesMap = new HashMap<>();
            for (URI inputFile : inputFiles) {
                File file = Paths.get(inputFile).toFile();
                String fileName = file.getName();
                int fileId = metadataManager.getFileId(studyMetadata.getId(), fileName);
                FileMetadata fileMetadata = metadataManager.getFileMetadata(studyMetadata.getId(), fileId);
                LinkedHashSet<String> fileSampleNames = new LinkedHashSet<>();
                for (Integer sampleId : fileMetadata.getSamples()) {
                    String sampleName = metadataManager.getSampleName(studyMetadata.getId(), sampleId);
                    sampleIdsMap.put(sampleName, sampleId);
                    fileSampleNames.add(sampleName);
                }
                fileSampleNamesMap.put(fileId, fileSampleNames);
            }

            // Create converters once for the entire gap-fill operation
            SampleToDocumentConverter samplesConverter =
                    new SampleToDocumentConverter(studyMetadata, sampleIdsMap);
            String defaultGt = studyMetadata.getAttributes()
                    .getAsStringList(MongoDBVariantStorageOptions.DEFAULT_GENOTYPE.key()).stream()
                    .findFirst().orElse("0/0");

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
                                    result.getFilledVariant(), result.getStatus(),
                                    samplesConverter, defaultGt,
                                    fileSampleNamesMap.get(fileIterator.getFileId()));
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
     *
     * <p>Delegates to {@link StudyEntryToDocumentConverter} for attrs/call/alternates and
     * {@link SampleToDocumentConverter} for mgt/sampleData/sfd, avoiding code duplication.</p>
     *
     * <p>Uses negative fileId for VARIANT and MULTI statuses, following the convention that
     * negative fid means "overlapped file" (the file has a different variant at this position).
     * The overlapping status is stored in a root-level field for debugging/validation.</p>
     */
    private Document buildFileDocument(int studyId, int fileId, Variant filledVariant,
                                       VariantOverlappingStatus status,
                                       SampleToDocumentConverter samplesConverter, String defaultGt,
                                       LinkedHashSet<String> fileSampleNames) {
        if (filledVariant == null || filledVariant.getStudies().isEmpty()) {
            return null;
        }

        StudyEntry filledStudy = filledVariant.getStudies().get(0);

        // Build base file document (attrs + call) using the shared converter
        FileEntry fileEntry = filledStudy.getFiles().isEmpty()
                ? new FileEntry(String.valueOf(fileId), null, Collections.emptyMap())
                : filledStudy.getFiles().get(0);
        // Ensure file entry has our fileId
        if (!String.valueOf(fileId).equals(fileEntry.getFileId())) {
            fileEntry = new FileEntry(String.valueOf(fileId), fileEntry.getCall(), fileEntry.getData());
        }
        Document fileDoc = StudyEntryToDocumentConverter.convertFileDocument(studyId, fileEntry, false);

        // Add secondary alternates using the shared converter
        if (!filledStudy.getSecondaryAlternates().isEmpty()) {
            fileDoc.append(ALTERNATES_FIELD,
                    StudyEntryToDocumentConverter.convertAlternates(filledVariant, filledStudy.getSecondaryAlternates()));
        }

        // Build a study entry with ALL file samples in metadata order for the samples converter.
        // Non-gap-filled samples get the default genotype (excluded from mgt) and null extra fields.
        StudyEntry orderedStudy = new StudyEntry(filledStudy.getStudyId());
        orderedStudy.setSampleDataKeys(new ArrayList<>(filledStudy.getSampleDataKeys()));
        Integer gtIdx = orderedStudy.getSampleDataKeyPosition("GT");
        int numFields = orderedStudy.getSampleDataKeys().size();
        List<String> filledSampleNames = filledStudy.getOrderedSamplesName();

        for (String sampleName : fileSampleNames) {
            int filledIdx = filledSampleNames.indexOf(sampleName);
            if (filledIdx >= 0) {
                // Gap-filled sample: copy real data
                orderedStudy.addSampleData(sampleName, new ArrayList<>(filledStudy.getSamples().get(filledIdx).getData()));
            } else {
                // Non-gap-filled sample: placeholder with default GT, null extra fields
                List<String> placeholder = new ArrayList<>(numFields);
                for (int i = 0; i < numFields; i++) {
                    placeholder.add(Objects.equals(i, gtIdx) ? defaultGt : null);
                }
                orderedStudy.addSampleData(sampleName, placeholder);
            }
        }

        // Use SampleToDocumentConverter for mgt + sampleData + sfd
        samplesConverter.convertToStorageType(orderedStudy, fileSampleNames, fileDoc);

        // Override fileId for VARIANT/MULTI (negative means "overlapped file")
        if (status == VariantOverlappingStatus.VARIANT || status == VariantOverlappingStatus.MULTI) {
            fileDoc.put(FILEID_FIELD, -fileId);
        }

        // Store overlapping status at root level of file document
        fileDoc.put(OVERLAPPING_STATUS_KEY, status.toString());

        return fileDoc;
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
