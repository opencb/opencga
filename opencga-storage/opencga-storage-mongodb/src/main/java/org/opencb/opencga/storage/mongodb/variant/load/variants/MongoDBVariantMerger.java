/*
 * Copyright 2015-2017 OpenCB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.opencb.opencga.storage.mongodb.variant.load.variants;

import com.mongodb.MongoExecutionTimeoutException;
import org.apache.commons.lang3.StringUtils;
import org.bson.BsonArray;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.Binary;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.AlternateCoordinate;
import org.opencb.biodata.models.variant.avro.FileEntry;
import org.opencb.biodata.models.variant.avro.VariantType;
import org.opencb.biodata.tools.variant.VariantNormalizer;
import org.opencb.biodata.tools.variant.merge.VariantMerger;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.commons.run.ParallelTaskRunner;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.mongodb.variant.MongoDBVariantStoragePipeline;
import org.opencb.opencga.storage.mongodb.variant.converters.DocumentToSamplesConverter;
import org.opencb.opencga.storage.mongodb.variant.converters.DocumentToStudyVariantEntryConverter;
import org.opencb.opencga.storage.mongodb.variant.converters.DocumentToVariantConverter;
import org.opencb.opencga.storage.mongodb.variant.converters.stage.StageDocumentToVariantConverter;
import org.opencb.opencga.storage.mongodb.variant.load.stage.MongoDBVariantStageLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

import static com.mongodb.client.model.Filters.*;
import static com.mongodb.client.model.Updates.*;
import static org.opencb.opencga.storage.mongodb.variant.MongoDBVariantStorageEngine.MongoDBVariantOptions.DEFAULT_GENOTYPE;
import static org.opencb.opencga.storage.mongodb.variant.converters.DocumentToSamplesConverter.UNKNOWN_GENOTYPE;
import static org.opencb.opencga.storage.mongodb.variant.converters.DocumentToStudyVariantEntryConverter.*;
import static org.opencb.opencga.storage.mongodb.variant.converters.DocumentToVariantConverter.IDS_FIELD;
import static org.opencb.opencga.storage.mongodb.variant.converters.DocumentToVariantConverter.STUDIES_FIELD;
import static org.opencb.opencga.storage.mongodb.variant.converters.stage.StageDocumentToVariantConverter.ID_FIELD;
import static org.opencb.opencga.storage.mongodb.variant.converters.stage.StageDocumentToVariantConverter.SECONDARY_ALTERNATES_FIELD;
import static org.opencb.opencga.storage.mongodb.variant.load.stage.MongoDBVariantStageLoader.STAGE_TO_VARIANT_CONVERTER;
import static org.opencb.opencga.storage.mongodb.variant.load.stage.MongoDBVariantStageLoader.VARIANT_CONVERTER_DEFAULT;

/**
 * Created on 07/04/16.
 *
 * Merges data from the Stage collection into the Variant collection.
 *
 * There are different situations depending on how the data is coming or depending on
 * the current variants configuration.
 *
 * ### Depending on the configuration of the variants
 *
 * Easy scenarios:
 *
 *                +---------------+------+
 *                |     Stage     | Vars |
 *                +-------+-------+------+
 *                | File1 | File2 | Vars |
 *  +-------------+-------+-------+------+
 *  | 1:100:A:C   | DATA  | DATA  | DATA |  <--- A1) Update variants with data from File1 and File2
 *  +-------------+-------+-------+------+
 *  | 1:125:A:C   | DATA  | DATA  | ---- |  <--- A2) New variant
 *  +-------------+-------+-------+------+
 *  | 1:150:G:T   | ----  | ----  | DATA |  <--- A3) Fill Gaps. If new study, skip.
 *  +-------------+-------+-------+------+
 *
 *  Some partial information from the stage collection. Fill gaps
 *  +-------------+-------+-------+------+
 *  | 1:200:A:C   | ----  | DATA  | DATA |  <--- B1) Update variants with data from File2 and missing from File1
 *  +-------------+-------+-------+------+
 *  | 1:225:A:C   | DATA  | ----  | ---- |  <--- B2) New variant with missing information from the File2
 *  +-------------+-------+-------+------+
 *  | 1:250:G:T   | ----  | ----  | DATA |  <--- B3) Missing information. Fill Gaps. If new study, skip.
 *  +-------------+-------+-------+------+
 *
 *  Overlapping variants
 *  +-------------+-------+-------+------+
 *  | 1:300:A:C   | ----  | DATA  | DATA |  <---
 *  +-------------+-------+-------+------+      |- C1) Simple merge variants
 *  | 1:300:A:T   | DATA  | ----  | DATA |  <---
 *  +=============+=======+=======+======+
 *  | 1:400:A:C   | DATA  | DATA  | DATA |  <---
 *  +-------------+-------+-------+------+      |- C2) Simple merge variants. File1 has duplicated information for this variants
 *  | 1:400:A:T   | DATA  | ----  | DATA |  <---   Ideally, variants for File1 have correct secondary alternate. Discard one variant.
 *  +=============+=======+=======+======+
 *  | 1:500:A:C   | ----  | ----  | DATA |  <---
 *  +-------------+-------+-------+------+      |- C3) New overlapped region. Fetch data from the database and merge.
 *  | 1:500:A:T   | DATA  | DATA  | ---- |  <---   Fill gaps for indexed files
 *  +=============+=======+=======+======+
 *  | 1:600:A:C   | ----  | ----  | DATA |  <---
 *  +-------------+-------+-------+------+      |- C4) New overlapped region. Fetch data from the database and merge.
 *  | 1:600:A:T   | DATA  | ----  | ---- |  <---   Fill gaps for indexed files and file2
 *  +=============+=======+=======+======+
 *  | 1:700:A:C   | ----  | ----  | DATA |  <---
 *  +-------------+-------+-------+------+      |- C5) Already existing overlapped region. No extra information. No overlapping variants.
 *  | 1:700:A:T   | ----  | ----  | DATA |  <---   Fill gaps for file1 and file2
 *  +=============+=======+=======+======+
 *
 *  Duplicated variants. Do not load duplicated variants.
 *  +-------------+-------+-------+------+
 *  | 1:200:A:C   | ----  | DATA* | DATA |  <--- D1) Duplicated variants in file 2. Fill gaps for file1 and file2.
 *  +-------------+-------+-------+------+           Skip if new study (nonInserted++)
 *  | 1:250:G:T   | ----  | DATA* | ---- |  <--- D2) Duplicated variants in file 2, missing for the rest.
 *  +-------------+-------+-------+------+           Skip variant! (nonInserted++)
 *  | 1:225:A:C   | DATA  | DATA* | DATA |  <--- D3) Duplicated variants in file 2. Fill gaps for file2 and insert file1.
 *  +-------------+-------+-------+------+
 *  | 1:225:A:C   | DATA  | DATA* | ---- |  <--- D4) Duplicated variants in file 2. Fill gaps for file2 and insert file1.
 *  +-------------+-------+-------+------+
 *  | 1:225:A:C   | DATA* | DATA* | ---- |  <--- D5) Duplicated variants in file 1 and 2. Fill gaps for file1 and file2.
 *  +-------------+-------+-------+------+           Skip if new study (nonInserted++)
 *
 *
 * ### Depending on how the data is split in files, and how the files are sent.
 *
 * This is specially important when filling gaps (for new or missing variants). Have to
 * select properly the "indexed files" for each region.
 *
 * So, in case of no filling gaps (i.e. default-chromosome is ?/?) none of this scenarios applies.
 * The only problem may be to load overlapping data, which may corrupt the database.
 * @see VariantStorageEngine.Options#LOAD_SPLIT_DATA
 *
 * The data can came split by chromosomes and/or by batches of samples. For the
 * next tables, columns are batches of samples, and the rows are different regions,
 * for example, chromosomes.
 * The cells represents a file. If the file is already merged on the database uses an X,
 * or if it's being loaded right now, an O.
 *
 * X = File loaded and merged
 * O = To be merged
 *
 * a) Merging one file having other merged files from a same sample set and chromosome.
 *        +---+---+---+
 *        |S1 |S2 |S3 |
 * +------+---+---+---+
 * | Chr1 | X | X |   |
 * +------+---+---+---+
 * | Chr2 | X | O |   |
 * +------+---+---+---+
 * | Chr3 | X |   |   |
 * +------+---+---+---+
 * Indexed files = {f_21}
 *
 * a.2) Merging different files from the same sample set.
 *      Must be aware when filling gaps (for new or missing variants).
 *      In this case, merge chromosome by chromosome in different calls.
 *        +---+---+---+
 *        |S1 |S2 |S3 |
 * +------+---+---+---+
 * | Chr1 | X | X | X |
 * +------+---+---+---+
 * | Chr2 | X | X | O |
 * +------+---+---+---+
 * | Chr3 | X | X | O |
 * +------+---+---+---+
 * Indexed files in Chr2 = {f_21, f_22}
 * Indexed files in Chr3 = {f_31, f_32}
 *
 * b) Merging one or more files from the same chromosome
 *    Having the indexed files in this region, no special consideration.
 *        +---+---+---+---+
 *        |S1 |S2 |S3 |S4 |
 * +------+---+---+---+---+
 * | Chr1 | X | X | X | X |
 * +------+---+---+---+---+
 * | Chr2 | X | X | O | O |
 * +------+---+---+---+---+
 * | Chr3 | X |   |   |   |
 * +------+---+---+---+---+
 * Indexed files = {f_21, f_22}
 *
 * c) Mix different regions and Sample sets
 *    Same situation as case a.2
 *        +---+---+---+
 *        |S1 |S2 |S3 |
 * +------+---+---+---+
 * | Chr1 | X | X | O |
 * +------+---+---+---+
 * | Chr2 | X | X |   |
 * +------+---+---+---+
 * | Chr3 | X | O |   |
 * +------+---+---+---+
 * Indexed files in Chr1 = {f_11, f_12}
 * Indexed files in Chr3 = {f_31}
 *
 * c) Mix different regions sizes. A single chromosome and a whole genome.
 *    Do not allow this!
 *        +---+---+---+
 *        |S1 |S2 |S3 |
 * +------+---+---+---+
 * | Chr1 | X | X | O |
 * +------+---+---+ O |
 * | Chr2 | X | O | O |
 * +------+---+---+ O |
 * | Chr3 | X | X | O |
 * +------+---+---+---+
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class MongoDBVariantMerger implements ParallelTaskRunner.Task<Document, MongoDBOperations> {

    private final VariantDBAdaptor dbAdaptor;

    /** Study to be merged. */
    private final Integer studyId;
    private final String studyIdStr;
    /** Files to be merged. */
    private final List<Integer> fileIds;
    /** Indexed files in the region that we are merging. */
    private final Set<Integer> indexedFiles;
    /**
     * Check overlapping variants.
     * Only needed when loading more than one file at the same time, or there were other loaded files in the same region
     **/
    private boolean checkOverlappings;
    private final DocumentToVariantConverter variantConverter;
    private final DocumentToStudyVariantEntryConverter studyConverter;
    private final StudyConfiguration studyConfiguration;
    private final boolean excludeGenotypes;
    private final boolean addUnknownGenotypes;

    // Variables that must be aware of concurrent modification
    private final Map<Integer, LinkedHashMap<String, Integer>> samplesPositionMap;
    private final List<Integer> indexedSamples;


    private final Logger logger = LoggerFactory.getLogger(MongoDBVariantMerger.class);
    private final VariantMerger variantMerger;
    private final List<String> format;
    private boolean resume;

    public MongoDBVariantMerger(VariantDBAdaptor dbAdaptor, StudyConfiguration studyConfiguration, List<Integer> fileIds,
                                Set<Integer> indexedFiles, boolean resume, boolean ignoreOverlapping) {
        this.dbAdaptor = Objects.requireNonNull(dbAdaptor);
        this.studyConfiguration = Objects.requireNonNull(studyConfiguration);
        this.fileIds = Objects.requireNonNull(fileIds);
        this.indexedFiles = Objects.requireNonNull(indexedFiles);

        excludeGenotypes = getExcludeGenotypes(studyConfiguration);
        format = buildFormat(studyConfiguration);
        indexedSamples = Collections.unmodifiableList(buildIndexedSamplesList(fileIds));
        studyId = studyConfiguration.getStudyId();
        studyIdStr = String.valueOf(studyId);
        addUnknownGenotypes = loadUnknownGenotypes(studyConfiguration);

        checkOverlappings = !ignoreOverlapping && (fileIds.size() > 1 || !indexedFiles.isEmpty());
        DocumentToSamplesConverter samplesConverter = new DocumentToSamplesConverter(this.studyConfiguration);
        studyConverter = new DocumentToStudyVariantEntryConverter(false, samplesConverter);
        variantConverter = new DocumentToVariantConverter(studyConverter, null);
        samplesPositionMap = new HashMap<>();

        variantMerger = new VariantMerger();
        variantMerger.configure(studyConfiguration.getVariantHeader());
        variantMerger.setExpectedFormats(format);
        this.resume = resume;
    }

    @Override
    public List<MongoDBOperations> apply(List<Document> batch) {
        try {
            return Collections.singletonList(merge(batch));
        } catch (Exception e) {
            if (batch.isEmpty()) {
                logger.error("Fail loading empty batch");
            } else {
                logger.error("Fail loading batch from " + batch.get(0).get("_id") + " to " + batch.get(batch.size() - 1).get("_id"));
            }
            throw e;
        }
    }

    public MongoDBOperations merge(List<Document> variants) {

        // Set of operations to be executed in the Database
        MongoDBOperations mongoDBOps = new MongoDBOperations();

        Variant previousVariant = null;
        Document previousDocument = null;
        int start = 0;
        int end = 0;
        String chromosome = null;
        List<Document> overlappedVariants = null;

        Iterator<Document> iterator = variants.iterator();
        // Get first valid variant
        while (iterator.hasNext()) {
            Document document = iterator.next();
            if (document.get(studyIdStr) != null) {
                Variant variant = STAGE_TO_VARIANT_CONVERTER.convertToDataModelType(document);
                if (variant.isSV()) {
                    // Directly process all Structural Variants
                    // Do never check if a SV overlaps with any other variant
                    processVariants(null, document, variant, mongoDBOps);
                } else {
                    previousDocument = document;
                    previousVariant = variant;

                    chromosome = previousVariant.getChromosome();
                    start = previousVariant.getStart();
                    end = getEnd(previousVariant);
                    break;
                }
            }
        }

        while (iterator.hasNext()) {
            Document document = iterator.next();
            Variant variant = STAGE_TO_VARIANT_CONVERTER.convertToDataModelType(document);
            Document study = document.get(studyIdStr, Document.class);
            if (study != null) {

                if (variant.isSV()) {
                    // Directly process all Structural Variants
                    // Do never check if a SV overlaps with any other variant
                    processVariants(null, document, variant, mongoDBOps);
                    continue;
                }

                if (checkOverlappings && variant.overlapWith(chromosome, start, end, true)) {
                    // If the variant overlaps with the last one, add to the overlappedVariants list.
                    // Do not process any variant yet!
                    if (overlappedVariants == null) {
                        overlappedVariants = new ArrayList<>();
                        overlappedVariants.add(previousDocument);
                    }
                    overlappedVariants.add(document);

                    // Take min start and max end
                    start = Math.min(start, variant.getStart());
                    end = Math.max(end, getEnd(variant));
                } else {
                    // If the current variant does not overlap with the previous variant, we can load the previous variant (or region)
                    processVariants(overlappedVariants, previousDocument, previousVariant, mongoDBOps);
                    overlappedVariants = null;

                    // Reset region
                    chromosome = variant.getChromosome();
                    start = variant.getStart();
                    end = getEnd(variant);

                }

                previousDocument = document;
                previousVariant = variant;
            }
        }

        // Process remaining variants
        processVariants(overlappedVariants, previousDocument, previousVariant, mongoDBOps);

//        // Execute MongoDB Operations
//        return executeMongoDBOperations(mongoDBOps);

        return mongoDBOps;
    }

    public void processVariants(List<Document> overlappedVariants, Document document, Variant variant,
                                MongoDBOperations mongoDBOps) {
        try {
            if (overlappedVariants != null) {
                for (Document overlappedVariant : overlappedVariants) {
                    if (alreadyProcessedStageDocument(overlappedVariant)) {
                        // Skip this batch if any of the documents is already processed
                        mongoDBOps.setMissingVariantsNoFillGaps(mongoDBOps.getMissingVariantsNoFillGaps() + overlappedVariants.size());
                        return;
                    }
                }
                processOverlappedVariants(overlappedVariants, mongoDBOps);
            } else if (document != null) {
                if (alreadyProcessedStageDocument(document)) {
                    mongoDBOps.setMissingVariantsNoFillGaps(mongoDBOps.getMissingVariantsNoFillGaps() + 1);
                    return;
                }
                processVariant(document, variant, mongoDBOps);
            }
        } catch (Exception e) {
            logger.error("Error processing variant " + variant, e);
            throw e;
        }
    }

    public boolean alreadyProcessedStageDocument(Document overlappedVariant) {
        Document study = overlappedVariant.get(studyIdStr, Document.class);
        for (Integer fileId : fileIds) {
            if (study.containsKey(fileId.toString())) {
                // If any of the files is null, the document is already processed.
                return study.get(fileId.toString()) == null;
            }
        }
        return false;
    }

    public Integer getEnd(Variant variant) {
//        if (variant.getType().equals(VariantType.SYMBOLIC) || variant.getType().equals(VariantType.NO_VARIATION)) {
//            return variant.getEnd();
//        } else {
//            return variant.getStart() + Math.max(variant.getReference().length() - 1, -1 /* 0 */);
//        }
        if (EnumSet.of(VariantType.SYMBOLIC, VariantType.CNV).contains(variant.getType())) {
            return variant.getStart();
        } else {
            return variant.getEnd();
        }
    }

    /**
     * Given a document from the stage collection, transforms the document into a set of MongoDB operations.
     *
     * It may be a new variant document in the database, a new study in the document, or just an update of an existing study variant.
     *
     * @param document          Document to load
     * @param emptyVar          Parsed empty variant of the document. Only chr, pos, ref, alt
     * @param mongoDBOps        Set of MongoDB operations to update
     */
    protected void processVariant(Document document, Variant emptyVar, MongoDBOperations mongoDBOps) {
        Document study = document.get(studyIdStr, Document.class);

        // New variant in the study.
        boolean newStudy = isNewStudy(study);
        // New variant in the collection if new variant and document size is 2 {_id, study}
        boolean newVariant = isNewVariant(document, newStudy);


        Set<String> ids = new HashSet<>();
        List<Document> fileDocuments = new LinkedList<>();
        List<Document> alternateDocuments = new LinkedList<>();
        Document gts = new Document();

        List<AlternateCoordinate> alternates = getAlternateCoordinatesFromStage(study);
        // Save the number of alternates in the original stage document. The list of alternates can be updated
        int alternatesFromStage = alternates.size();

        // Loop for each file that have to be merged
        int missing = 0;
        int skipped = 0;
        int duplicated = 0;
        for (Integer fileId : fileIds) {

            // Different actions if the file is present or missing in the document.
            if (study.containsKey(fileId.toString())) {
                //Duplicated documents are treated like missing. Increment the number of duplicated variants
                List<Object> duplicatedVariants = getListFromDocument(study, fileId.toString());
                if (duplicatedVariants.size() > 1) {
                    mongoDBOps.setNonInserted(mongoDBOps.getNonInserted() + duplicatedVariants.size());
                    if (addUnknownGenotypes) {
                        addSampleIdsGenotypes(gts, UNKNOWN_GENOTYPE, getSamplesInFile(fileId));
                    }
                    logDuplicatedVariant(emptyVar, duplicatedVariants.size(), fileId);
                    duplicated++;
                    continue;
                }

                Object file = duplicatedVariants.get(0);
                Variant variant;
                if (file instanceof Binary) {
                    variant = VARIANT_CONVERTER_DEFAULT.convertToDataModelType(((Binary) file));
                } else if (file instanceof Variant) {
                    variant = ((Variant) file);
                } else {
                    throw new IllegalStateException("");
                }
                if (MongoDBVariantStoragePipeline.SKIPPED_VARIANTS.contains(variant.getType())) {
                    mongoDBOps.setSkipped(mongoDBOps.getSkipped() + 1);
                    skipped++;
                    continue;
                }
                if (StringUtils.isNotEmpty(variant.getId()) && !variant.getId().equals(variant.toString())) {
                    ids.add(variant.getId());
                }
                if (variant.getNames() != null) {
                    ids.addAll(variant.getNames());
                }
                emptyVar.setType(variant.getType());
                emptyVar.setSv(variant.getSv());
                variant.getStudies().get(0).setSamplesPosition(getSamplesPosition(fileId));
                List<AlternateCoordinate> fileAlternates = variant.getStudies().get(0).getSecondaryAlternates();
                if (!alternates.isEmpty() && !alternates.equals(fileAlternates)) {
                    if (!fileAlternates.isEmpty()) {
                        // Create template variant with the required alternates.
                        Variant templateVariant = new Variant(
                                variant.getChromosome(),
                                variant.getStart(),
                                variant.getEnd(),
                                variant.getReference(),
                                variant.getAlternate());
                        StudyEntry studyEntry = new StudyEntry(studyIdStr, alternates, format);
                        studyEntry.setSamplesPosition(Collections.emptyMap());
                        templateVariant.addStudyEntry(studyEntry);

                        variant = variantMerger.merge(templateVariant, variant);
                        // Update the alternates, in case of increasing the number of alternates.
                        alternates = new ArrayList<>(variant.getStudies().get(0).getSecondaryAlternates());
                    } else {
                        // Add unused extra alternates
                        fileAlternates.addAll(alternates);
                    }
                } else if (alternates.isEmpty() && !fileAlternates.isEmpty()) {
                    alternates = new ArrayList<>(fileAlternates);
                }

                Document newDocument = studyConverter.convertToStorageType(variant, variant.getStudies().get(0));
                fileDocuments.add((Document) getListFromDocument(newDocument, FILES_FIELD).get(0));
                alternateDocuments = getListFromDocument(newDocument, ALTERNATES_FIELD);

                if (newDocument.containsKey(GENOTYPES_FIELD)) {
                    for (Map.Entry<String, Object> entry : newDocument.get(GENOTYPES_FIELD, Document.class).entrySet()) {
                        addSampleIdsGenotypes(gts, entry.getKey(), (List<Integer>) entry.getValue());
                    }
                }

            } else {
                if (addUnknownGenotypes) {
                    addSampleIdsGenotypes(gts, UNKNOWN_GENOTYPE, getSamplesInFile(fileId));
                }
                missing++;
            }

        }

        if (newStudy && addUnknownGenotypes) {
            //If it is a new variant for the study, add the already loaded samples as UNKNOWN
            addSampleIdsGenotypes(gts, UNKNOWN_GENOTYPE, getIndexedSamples());
        }

        addCleanStageOperations(document, mongoDBOps, newStudy, missing, skipped, duplicated);

        updateMongoDBOperations(emptyVar, new ArrayList<>(ids), fileDocuments, alternatesFromStage, alternateDocuments, gts,
                newStudy, newVariant, mongoDBOps);
    }

    protected void processOverlappedVariants(List<Document> overlappedVariants, MongoDBOperations mongoDBOps) {
        for (Document document : overlappedVariants) {
            try {
                processOverlappedVariants(document, overlappedVariants, mongoDBOps);
            } catch (Exception e) {
                Variant mainVariant = STAGE_TO_VARIANT_CONVERTER.convertToDataModelType(document);
                List<Variant> variants = overlappedVariants.stream()
                        .map(STAGE_TO_VARIANT_CONVERTER::convertToDataModelType)
                        .collect(Collectors.toList());
                logger.error("Error processing variant " + mainVariant + " in overlapped variants " + variants);
                throw e;
            }
        }
    }

    /**
     * Given a list of documents from the stage collection, and one variant from the list of documents,
     * merges into the main variant and transforms into a set of MongoDB operations.
     *
     * It may be a new variant document in the database, a new study in the document, or just an update of an existing study variant.
     *
     * @param mainDocument          Main document to add.
     * @param overlappedVariants    Overlapping documents from Stage collection.
     * @param mongoDBOps            Set of MongoDB operations to update
     */
    protected void processOverlappedVariants(Document mainDocument, List<Document> overlappedVariants, MongoDBOperations mongoDBOps) {

        Variant mainVariant = STAGE_TO_VARIANT_CONVERTER.convertToDataModelType(mainDocument);

        int variantsWithValidData = getVariantsWithValidData(mainVariant, overlappedVariants);

        Document study = mainDocument.get(studyIdStr, Document.class);

        // New variant in the study.
        boolean newStudy = isNewStudy(study);
        // New variant in the collection if new variant and document size is 2 {_id, study}
        boolean newVariant = isNewVariant(mainDocument, newStudy);

        // A variant counts as duplicated if is duplicated or missing for all the files.
        int duplicatedVariants = 0;
        List<String> duplicatedVariantsList = new ArrayList<>();
        int duplicatedFiles = 0;
        int missingFiles = 0;
        for (Integer fileId : fileIds) {
            List<Binary> files = getListFromDocument(study, fileId.toString());
            if (files == null || files.isEmpty()) {
                missingFiles++;
            } else if (files.size() > 1) {
                duplicatedVariants += files.size();
                duplicatedFiles++;
//                        // If there are more than one variant for this file, increment the number of nonInserted variants.
//                        // Duplicated variant
                logDuplicatedVariant(mainVariant, files.size(), fileId);
                for (Binary binary : files) {
                    Variant duplicatedVariant = VARIANT_CONVERTER_DEFAULT.convertToDataModelType(binary);
                    String call = duplicatedVariant.getStudies().get(0).getFiles().get(0).getCall();
                    if (call == null) {
                        call = duplicatedVariant.toString();
                    }
                    duplicatedVariantsList.add(call);
                }
            }
        }

        addCleanStageOperations(mainDocument, mongoDBOps, newStudy, missingFiles, 0, duplicatedFiles);

        // An overlapping variant will be considered missing if is missing or duplicated for all the files.
        final boolean missingOverlappingVariant;

        if (duplicatedFiles + missingFiles == fileIds.size()) {
            // C3.1), C4.1), C5), B3), D1), D2)
            missingOverlappingVariant = true;
            if (duplicatedFiles > 0) {
                // D1), D2)
                logger.error("Duplicated! " + mainVariant + " " + duplicatedVariantsList);
                mongoDBOps.setNonInserted(mongoDBOps.getNonInserted() + duplicatedVariants);
            }
            // No information for this variant
            if (newStudy) {
                // B3), D1), D2)
                return;
            }
            // else {
            //      Do not skip. Fill gaps.
            //      No new overlapped variants.
            // }

            if (variantsWithValidData != 0) {
                // Scenarios C3.1), C4.1)
                logger.debug("Missing overlapped variant! {}, {}", fileIds, mainVariant);
                mongoDBOps.setOverlappedVariants(mongoDBOps.getOverlappedVariants() + 1);
            }
            // else {
            //      If the files to be loaded where not present in the current variant, there is not overlapped variant.
            //      See scenario C5)
            // }
        } else {
            missingOverlappingVariant = false;
        }

        // Get list of already loaded secondary alternates
        List<AlternateCoordinate> loadedSecondaryAlternates = getAlternateCoordinatesFromStage(study);
        // Save the number of alternates in the original stage document. The list of alternates can be updated
        int alternatesFromStage = loadedSecondaryAlternates.size();

        // Merge documents
        Variant variant = mergeOverlappedVariants(mainVariant, overlappedVariants, loadedSecondaryAlternates);

        Document gts = new Document();
        List<Document> fileDocuments = new LinkedList<>();
        List<Document> alternateDocuments = null;
        StudyEntry studyEntry = variant.getStudies().get(0);

        // For all the files that are being indexed
        for (Integer fileId : fileIds) {
            FileEntry file = studyEntry.getFile(fileId.toString());
            if (file == null) {
                file = studyEntry.getFile(String.valueOf(-fileId));
            }
            if (file != null) {
                Document studyDocument = studyConverter.convertToStorageType(variant, studyEntry, file, getSampleNamesInFile(fileId));
                if (studyDocument.containsKey(GENOTYPES_FIELD)) {
                    studyDocument.get(GENOTYPES_FIELD, Document.class)
                            .forEach((gt, sampleIds) -> addSampleIdsGenotypes(gts, gt, (Collection<Integer>) sampleIds));
                }
                fileDocuments.addAll(getListFromDocument(studyDocument, FILES_FIELD));
                alternateDocuments = getListFromDocument(studyDocument, ALTERNATES_FIELD);
            } else if (addUnknownGenotypes) {
                addSampleIdsGenotypes(gts, UNKNOWN_GENOTYPE, getSamplesInFile(fileId));
            }
        }

        // For the rest of the files not indexed, only is this variant is new in this study,
        // add all the already indexed files information, if present in this variant.
        if (newStudy) {
            for (Integer fileId : indexedFiles) {
                FileEntry file = studyEntry.getFile(fileId.toString());
                if (file == null) {
                    file = studyEntry.getFile(String.valueOf(-fileId));
                }
                if (file != null) {
                    Document studyDocument = studyConverter.convertToStorageType(variant, studyEntry, file,
                            getSampleNamesInFile(fileId));
                    if (studyDocument.containsKey(GENOTYPES_FIELD)) {
                        studyDocument.get(GENOTYPES_FIELD, Document.class)
                                .forEach((gt, sampleIds) -> addSampleIdsGenotypes(gts, gt, (Collection<Integer>) sampleIds));
                    }
                    fileDocuments.addAll(getListFromDocument(studyDocument, FILES_FIELD));
                } else if (addUnknownGenotypes) {
                    addSampleIdsGenotypes(gts, UNKNOWN_GENOTYPE, getSamplesInFile(fileId));
                }
            }
        }
        updateMongoDBOperations(mainVariant, variant.getIds(), fileDocuments, alternatesFromStage, alternateDocuments, gts, newStudy,
                newVariant, mongoDBOps);

    }

    private void addCleanStageOperations(Document document, MongoDBOperations mongoDBOps, boolean newStudy, int missing,
                                         int skipped, int duplicated) {
        if (newStudy && duplicated > 0 && (missing + skipped + duplicated) == fileIds.size()) {
//            System.out.println("duplicated: document.getString(\"_id\") = " + document.getString("_id"));
            mongoDBOps.getDocumentsToCleanStudies().add(document.getString("_id"));
        } else {
            if (missing != fileIds.size()) {
                mongoDBOps.getDocumentsToCleanFiles().add(document.getString("_id"));
            } // else {
            //     logger.debug("Nothing to clean in variant " + document.getString("_id") + " , " + fileIds);
            // }
        }
    }

    private void logDuplicatedVariant(Variant variant, int numDuplicates, Integer fileId) {
        logger.warn("Found {} duplicated variants for file {} in variant {}.", numDuplicates, fileId, variant);
    }

    /**
     * Given a collection of documents from the stage collection, returns the number of documents (variants) with valid data.
     * i.e. : At least one file not duplicated with information
     *
     * @param mainVariant Main variant. Only valid data if overlaps with the main variant.
     * @param documents Variants from the stage collection
     * @return  Number of variants with valid data.
     */
    private int getVariantsWithValidData(Variant mainVariant, Collection<Document> documents) {
        int variantsWithValidData = 0;
        for (Document document : documents) {
            if (!mainVariant.overlapWith(STAGE_TO_VARIANT_CONVERTER.convertToDataModelType(document), true)) {
                continue;
            }
            Document study = document.get(studyIdStr, Document.class);
            boolean existingFiles = false;
            for (Integer fileId : fileIds) {
                List<Binary> files = getListFromDocument(study, fileId.toString());
                if (files != null && files.size() == 1) {
                    existingFiles = true;
                    break;
                }
            }
            if (existingFiles) {
                variantsWithValidData++;
            }
        }
        return variantsWithValidData;
    }

    /**
     * Given a list of overlapped documents from the stage collection, merge resolving the overlapping positions.
     *
     * If there are any conflict with overlapped positions, will try to select always the mainVariant.
     *
     * @see VariantMerger
     *
     * @param mainVariant           Main variant to resolve conflicts.
     * @param overlappedVariants    Overlapping documents from Stage collection.
     * @param loadedSecondaryAlternates Already loaded secondary alternates. Read from the STAGE collection
     * @return  For each document, its corresponding merged variant
     */
    protected Variant mergeOverlappedVariants(Variant mainVariant, List<Document> overlappedVariants,
                                              List<AlternateCoordinate> loadedSecondaryAlternates) {
//        System.out.println("--------------------------------");
//        System.out.println("Overlapped region = " + overlappedVariants
//                .stream()
//                .map(doc -> STRING_ID_CONVERTER.convertToDataModelType(doc.getString("_id")))
//                .collect(Collectors.toList()));

        // The overlapping region will be new if any of the variants is new for the study
        boolean newOverlappingRegion = false;
        // The overlapping region will be completely new if ALL the variants are new for the study
        boolean completelyNewOverlappingRegion = true;

        Map<Integer, List<Variant>> variantsPerFile = new HashMap<>();
        for (Integer fileId : fileIds) {
            variantsPerFile.put(fileId, new LinkedList<>());
        }

        Variant mainVariantNew = null;
        List<Variant> variants = new ArrayList<>(overlappedVariants.size());
        List<Boolean> newStudies = new ArrayList<>(overlappedVariants.size());

        // For each variant, create an empty variant that will be filled by the VariantMerger
        for (Document document : overlappedVariants) {
            Variant var = STAGE_TO_VARIANT_CONVERTER.convertToDataModelType(document);
            if (!mainVariant.overlapWith(var, true)) {
                // Skip those variants that do not overlap with the given main variant
                continue;
            }

            Document study = document.get(studyIdStr, Document.class);

            // New variant in the study.
            boolean newStudy = isNewStudy(study);
            newStudies.add(newStudy);
            // Its a new OverlappingRegion if at least one variant is new in this study
            newOverlappingRegion |= newStudy;
            // Its a completely new OverlappingRegion if all the variants are new in this study
            completelyNewOverlappingRegion &= newStudy;

            variants.add(var);

            if (sameVariant(var, mainVariant)) {
                mainVariantNew = var;
                StudyEntry se = new StudyEntry(studyId.toString(), new LinkedList<>(), format);
                se.setSamplesPosition(new HashMap<>());
                var.addStudyEntry(se);
            }
            HashSet<String> ids = new HashSet<>();
            for (Integer fileId : fileIds) {
                List<Binary> files = getListFromDocument(study, fileId.toString());
                if (files != null && files.size() == 1) {
                    // If there is only one variant for this file, add to the map variantsPerFile
                    Variant variant = VARIANT_CONVERTER_DEFAULT.convertToDataModelType(files.get(0));
                    variant.getStudies().get(0).setSamplesPosition(getSamplesPosition(fileId));
                    variantsPerFile.get(fileId).add(variant);
                    ids.addAll(variant.getIds());
                }
            }
            var.setIds(new ArrayList<>(ids));
        }

        if (mainVariantNew == null) {
            // This should never happen
            throw new IllegalStateException("Main variant was not one of the variants to merge");
        }

        // Check if the already loaded secondary alternates are different (in size or order) than the ones in the main variant
        if (!loadedSecondaryAlternates.isEmpty()
                && !mainVariantNew.getStudies().get(0).getSecondaryAlternates().equals(loadedSecondaryAlternates)) {
            // Replace the mainVariant with a template with the already loaded secondary alternates.
            // The VariantMerger will respect this order
            mainVariantNew = new Variant(
                    mainVariantNew.getChromosome(),
                    mainVariantNew.getStart(),
                    mainVariantNew.getEnd(),
                    mainVariantNew.getReference(),
                    mainVariantNew.getAlternate());
            StudyEntry se = new StudyEntry(studyIdStr, loadedSecondaryAlternates, format);
            se.setSamplesPosition(Collections.emptyMap());
            mainVariantNew.addStudyEntry(se);
        }

        List<Integer> overlappingFiles = new ArrayList<>();
        List<Variant> variantsToMerge = new LinkedList<>();
        for (Integer fileId : fileIds) {
            List<Variant> variantsInFile = variantsPerFile.get(fileId);
            switch (variantsInFile.size()) {
                case 0:
                    break;
                case 1:
                    variantsToMerge.add(variantsInFile.get(0));
                    if (!sameVariant(variantsInFile.get(0), mainVariant)) {
                        overlappingFiles.add(fileId);
                    }
                    break;
                default:
                    // If there are overlapping variants, select the mainVariant if possible.
                    Variant var = null;
                    for (Variant variant : variantsInFile) {
                        if (sameVariant(variant, mainVariant)) {
                            var = variant;
                        }
                    }
                    // If not found, get the first
                    if (var == null) {
                        var = variantsInFile.get(0);
                        overlappingFiles.add(fileId);
//                        logger.info("Variant " + mainVariant + " not found in " + variantsInFile);
                    }
                    variantsToMerge.add(var);


                    // Get the original call from the first variant
                    String call = var.getStudies().get(0).getFiles().get(0).getCall();
                    if (call != null) {
                        if (call.isEmpty()) {
                            call = null;
                        } else {
                            call = call.substring(0, call.lastIndexOf(':'));
                        }
                    }

                    // Do not prompt overlapping variants if genotypes are being excluded
                    if (!excludeGenotypes) {
                        boolean prompted = false;
                        for (int i = 1; i < variantsInFile.size(); i++) {
                            Variant auxVar = variantsInFile.get(i);
                            // Check if variants where part of the same multiallelic variant
                            String auxCall = auxVar.getStudies().get(0).getFiles().get(0).getCall();
                            if (!prompted && (auxCall == null || call == null || !auxCall.startsWith(call))) {
                                logger.warn("Overlapping variants in file {} : {}", fileId, variantsInFile);
                                prompted = true;
                            }
//                        // Those variants that do not overlap with the selected variant won't be inserted
//                        if (!auxVar.overlapWith(var, true)) {
//                            mongoDBOps.nonInserted++;
//                            logger.warn("Skipping overlapped variant " + auxVar);
//                        }
                        }
                    }
                    break;
            }
        }


        /*
         * If is a new overlapping region and there are some file already indexed
         * Fetch the information from the database regarding the loaded variants of this region.
         *
         *      +---+---+---+---+
         *      | A | B | C | D |
         * +----+---+---+---+---+
         * | V1 | X | X |   |   |
         * +----+---+---+---+---+
         * | V2 | X | X |   | X |
         * +----+---+---+---+---+
         * | V3 |   |   | X |   |
         * +----+---+---+---+---+
         *
         * - Files A and B are loaded
         * - Files C and D are being loaded
         * - Variants V1,V2,V3 are overlapping
         *
         * In order to merge the data properly, we need to get from the server the information about
         * the variants {V1, V2} for the files {A, B}.
         *
         * Because the variants {V1, V2} are already loaded, the information that we need is duplicated
         * in both variants, so we only need to get one of them.
         *
         */
        if (!completelyNewOverlappingRegion && newOverlappingRegion && !indexedFiles.isEmpty()) {
            int i = 0;
            for (Variant variant : variants) {
                // If the variant is not new in this study, query to the database for the loaded info.
                if (!newStudies.get(i)) {
                    QueryResult<Variant> queryResult = fetchVariant(variant);
                    if (queryResult.getResult().size() == 1 && queryResult.first().getStudies().size() == 1) {
                        // Check if overlapping variant. If so, invert!
                        for (FileEntry fileEntry : queryResult.first().getStudies().get(0).getFiles()) {
                            boolean empty = StringUtils.isEmpty(fileEntry.getCall());
                            if (empty && !sameVariant(mainVariant, queryResult.first())
                                    || !empty && !sameVariant(mainVariant, fileEntry.getCall())) {
                                markAsOverlapped(fileEntry);
                            } else {
                                markAsNonOverlapped(fileEntry);
                            }
                        }
                        variantsToMerge.add(queryResult.first());
                    } else {
                        if (queryResult.getResult().isEmpty()) {
                            throw new IllegalStateException("Variant " + variant + " not found!");
                        } else {
                            throw new IllegalStateException("Variant " + variant + " found wrong! : " + queryResult.getResult());
                        }
                    }
                    // Because the loaded variants were an overlapped region, all the information required is in every variant.
                    // Fetch only one variant
                    break;
                }
                i++;
            }
        }

        // Finally, merge variants
        variantMerger.merge(mainVariantNew, variantsToMerge);

        if (!overlappingFiles.isEmpty()) {
            for (FileEntry fileEntry : mainVariantNew.getStudies().get(0).getFiles()) {
                int fileId = Integer.parseInt(fileEntry.getFileId());
                if (overlappingFiles.contains(fileId)) {
                    markAsOverlapped(fileEntry);
                }
            }
        }

        return mainVariantNew;
    }

    private void markAsOverlapped(FileEntry fileEntry) {
        int fid = Integer.parseInt(fileEntry.getFileId());
        if (fid > 0) {
            fileEntry.setFileId(String.valueOf(-fid));
        }
    }

    private void markAsNonOverlapped(FileEntry fileEntry) {
        int fid = Integer.parseInt(fileEntry.getFileId());
        if (fid < 0) {
            fileEntry.setFileId(String.valueOf(-fid));
        }
    }

    /**
     * Reads the given variant from the 'variants' collection.
     *
     * It may happen that, 3s of default timeout, is not enough if there is a
     * lot of writes at the same time in the "variants" collection. Also add a
     * retry, just in case.
     * @param variant Variant to read
     * @return  Query result of the query
     */
    private QueryResult<Variant> fetchVariant(Variant variant) {
        QueryResult<Variant> queryResult = null;
        int maxNumFails = 2;
        int fails = 0;
        while (queryResult == null) {
            try {
                queryResult = dbAdaptor.get(new Query()
                                .append(VariantQueryParam.ID.key(), variant.toString())
                                .append(VariantQueryParam.UNKNOWN_GENOTYPE.key(), ".")
                                .append(VariantQueryParam.INCLUDE_STUDY.key(), studyId),
                        new QueryOptions(QueryOptions.TIMEOUT, 30_000));
            } catch (MongoExecutionTimeoutException e) {
                fails++;
                if (fails < maxNumFails) {
                    logger.warn("Got timeout exception reading variants. Retry!", e);
                } else {
                    throw e;
                }
            }
        }
        return queryResult;
    }

    /**
     * Transform the set of genotypes and file objects into a set of mongodb operations.
     *
     * @param emptyVar            Parsed empty variant of the document. Only chr, pos, ref, alt
     * @param ids                 Variant identifiers seen for this variant
     * @param fileDocuments       List of files to be updated
     * @param alternatesFromStage Number of alternates from the stage collection.
     *                            Alternates in stage will only be updated if there are new alternates.
     * @param secondaryAlternates SecondaryAlternates documents.
     * @param gts                 Set of genotypes to be updates
     * @param newStudy            If the variant is new for this study
     * @param newVariant          If the variant was never seen in the database
     * @param mongoDBOps          Set of MongoBD operations to update
     */
    protected void updateMongoDBOperations(Variant emptyVar, List<String> ids, List<Document> fileDocuments,
                                           int alternatesFromStage, List<Document> secondaryAlternates, Document gts,
                                           boolean newStudy, boolean newVariant, MongoDBOperations mongoDBOps) {
        final String id;

        if (!excludeGenotypes) {
            mongoDBOps.getGenotypes().addAll(gts.keySet());
        }

        if (newStudy) {
            // If there where no files and the study is new, do not add a new study.
            // It may happen if all the files in the variant where duplicated for this variant.
            if (!fileDocuments.isEmpty()) {
                Document studyDocument = new Document(STUDYID_FIELD, studyId)
                        .append(FILES_FIELD, fileDocuments);

                if (!excludeGenotypes) {
                    studyDocument.append(GENOTYPES_FIELD, gts);
                }

                if (secondaryAlternates != null && !secondaryAlternates.isEmpty()) {
                    studyDocument.append(ALTERNATES_FIELD, secondaryAlternates);
                }

                List<Bson> updates = new ArrayList<>();
                updates.add(push(STUDIES_FIELD, studyDocument));
                if (newVariant) {
                    Document variantDocument = variantConverter.convertToStorageType(emptyVar);
                    updates.add(addEachToSet(IDS_FIELD, ids));
                    for (Map.Entry<String, Object> entry : variantDocument.entrySet()) {
                        if (!entry.getKey().equals("_id") && !entry.getKey().equals(STUDIES_FIELD) && !entry.getKey().equals(IDS_FIELD)) {
                            Object value = entry.getValue();
                            if (value instanceof List) {
                                updates.add(setOnInsert(entry.getKey(), new BsonArray(((List) value))));
                            } else {
                                updates.add(setOnInsert(entry.getKey(), value));
                            }
                        }
                    }
                    mongoDBOps.getNewStudy().getVariants().add(variantDocument);
                    id = variantDocument.getString("_id");
                } else {
                    id = variantConverter.buildStorageId(emptyVar);
                }
                mongoDBOps.getNewStudy().getIds().add(id);
                mongoDBOps.getNewStudy().getQueries().add(eq("_id", id));
                mongoDBOps.getNewStudy().getUpdates().add(combine(updates));
            } else {
                id = null;
            }
        } else {
            id = variantConverter.buildStorageId(emptyVar);
            List<Bson> mergeUpdates = new LinkedList<>();
            if (!ids.isEmpty()) {
                mergeUpdates.add(addEachToSet(IDS_FIELD, ids));
            }

            if (!excludeGenotypes) {
                for (String gt : gts.keySet()) {
                    List sampleIds = getListFromDocument(gts, gt);
                    if (resume) {
                        mergeUpdates.add(addEachToSet(STUDIES_FIELD + ".$." + GENOTYPES_FIELD + '.' + gt, sampleIds));
                    } else {
                        mergeUpdates.add(pushEach(STUDIES_FIELD + ".$." + GENOTYPES_FIELD + '.' + gt, sampleIds));
                    }
                }
            }
            if (secondaryAlternates != null && !secondaryAlternates.isEmpty()) {
                mergeUpdates.add(addEachToSet(STUDIES_FIELD + ".$." + ALTERNATES_FIELD, secondaryAlternates));
            }

            if (!fileDocuments.isEmpty()) {
                mongoDBOps.getExistingStudy().getIds().add(id);
                mongoDBOps.getExistingStudy().getQueries().add(and(eq("_id", id),
                        eq(STUDIES_FIELD + '.' + STUDYID_FIELD, studyId)));

                if (resume) {
                    mergeUpdates.add(addEachToSet(STUDIES_FIELD + ".$." + FILES_FIELD, fileDocuments));
                } else {
                    mergeUpdates.add(pushEach(STUDIES_FIELD + ".$." + FILES_FIELD, fileDocuments));
                }
                mongoDBOps.getExistingStudy().getUpdates().add(combine(mergeUpdates));
            } else if (!mergeUpdates.isEmpty()) {
                // These files are not present in this variant. Increase the number of missing variants.
                mongoDBOps.setMissingVariants(mongoDBOps.getMissingVariants() + 1);
                mongoDBOps.getExistingStudy().getIds().add(id);
                mongoDBOps.getExistingStudy().getQueries().add(and(eq("_id", id),
                        eq(STUDIES_FIELD + '.' + STUDYID_FIELD, studyId)));
                mongoDBOps.getExistingStudy().getUpdates().add(combine(mergeUpdates));
            } else {
                mongoDBOps.setMissingVariantsNoFillGaps(mongoDBOps.getMissingVariantsNoFillGaps() + 1);
            }
        }

        if (secondaryAlternates != null && !secondaryAlternates.isEmpty()
                && id != null
                && alternatesFromStage != secondaryAlternates.size()) {


            // Secondary alternates loaded at stage must be empty or be a sublist of the new alternates to load.
            // <sid>.alts = { $exists : 0 }
            // <sid>.alts =
            ArrayList<Bson> filters = new ArrayList<>();
            filters.add(exists(studyIdStr + '.' + SECONDARY_ALTERNATES_FIELD, false));
            for (int i = 1; i <= secondaryAlternates.size(); i++) {
                filters.add(eq(studyIdStr + '.' + SECONDARY_ALTERNATES_FIELD, secondaryAlternates.subList(0, i)));
            }

            mongoDBOps.getSecondaryAlternates().getIds().add(id);
            mongoDBOps.getSecondaryAlternates().getQueries().add(and(eq(ID_FIELD, id), or(filters)));
            mongoDBOps.getSecondaryAlternates().getUpdates().add(set(studyIdStr + '.' + SECONDARY_ALTERNATES_FIELD, secondaryAlternates));
        }
    }

    private List<AlternateCoordinate> getAlternateCoordinatesFromStage(Document study) {
        List<AlternateCoordinate> alternates;
        if (study.containsKey(SECONDARY_ALTERNATES_FIELD)) {
            List<Document> alternatesDocuments = getListFromDocument(study, SECONDARY_ALTERNATES_FIELD);
            alternates = alternatesDocuments
                    .stream()
                    .map(DocumentToStudyVariantEntryConverter::convertToAlternateCoordinate)
                    .collect(Collectors.toList());
        } else {
            alternates = Collections.emptyList();
        }
        return alternates;
    }

    /**
     * Is a new variant for the study depending on the value of the field {@link MongoDBVariantStageLoader#NEW_STUDY_FIELD}.
     * @param study Study object
     * @return      If this is the first time that the variant has been seen in this study.
     */
    public static boolean isNewStudy(Document study) {
        return study.getBoolean(MongoDBVariantStageLoader.NEW_STUDY_FIELD, MongoDBVariantStageLoader.NEW_STUDY_DEFAULT);
    }

    public static boolean isNewVariant(Document document, boolean newStudy) {
        // If the document has only the study, _id, end, ref and alt fields.
        if (!newStudy || document.size() != 6) {
            for (Map.Entry<String, Object> entry : document.entrySet()) {
                if (!entry.getKey().equals(StageDocumentToVariantConverter.ID_FIELD)
                        && !entry.getKey().equals(StageDocumentToVariantConverter.END_FIELD)
                        && !entry.getKey().equals(StageDocumentToVariantConverter.REF_FIELD)
                        && !entry.getKey().equals(StageDocumentToVariantConverter.ALT_FIELD)
                        && !entry.getKey().equals(StageDocumentToVariantConverter.STUDY_FILE_FIELD)) {
                    if (!isNewStudy((Document) entry.getValue())) {
                        return false;
                    }
                }
            }
        }
        return true;
    }

    private boolean sameVariant(Variant variant, Variant other) {
        return variant.getChromosome().equals(other.getChromosome())
                && variant.getStart().equals(other.getStart())
                && variant.getReference().equals(other.getReference())
                && variant.getAlternate().equals(other.getAlternate());
    }


    private boolean sameVariant(Variant variant, String call) {
        String[] split = call.split(":", -1);
        List<VariantNormalizer.VariantKeyFields> normalized;
        if (variant.isSymbolic()) {
            normalized = new VariantNormalizer()
                    .normalizeSymbolic(Integer.parseInt(split[0]), variant.getEnd(), split[1], Arrays.asList(split[2].split(",")));
        } else {
            normalized = new VariantNormalizer()
                    .normalize(variant.getChromosome(), Integer.parseInt(split[0]), split[1], Arrays.asList(split[2].split(",")));
        }
        for (VariantNormalizer.VariantKeyFields variantKeyFields : normalized) {
            if (variantKeyFields.getStart() == variant.getStart()
                    && variantKeyFields.getReference().equals(variant.getReference())
                    && variantKeyFields.getAlternate().equals(variant.getAlternate())) {
                return true;
            }
        }
        return false;
    }

    protected void addSampleIdsGenotypes(Document gts, String genotype, Collection<Integer> sampleIds) {
        if (sampleIds.isEmpty()) {
            return;
        }
        if (gts.containsKey(genotype)) {
            getListFromDocument(gts, genotype).addAll(sampleIds);
        } else {
            gts.put(genotype, new LinkedList<>(sampleIds));
        }
    }

    @SuppressWarnings("unchecked")
    private <T> List<T> getListFromDocument(Document document, String key) {
        return document.get(key, List.class);
    }

    protected List<Integer> getIndexedSamples() {
        return indexedSamples;
    }

    private List<Integer> buildIndexedSamplesList(List<Integer> fileIds) {
        List<Integer> indexedSamples = new LinkedList<>(StudyConfiguration.getIndexedSamples(studyConfiguration).values());
        for (Integer fileId : fileIds) {
            indexedSamples.removeAll(getSamplesInFile(fileId));
        }
        indexedSamples.sort(Integer::compareTo);
        return indexedSamples;
    }

    protected LinkedHashSet<Integer> getSamplesInFile(Integer fileId) {
        return studyConfiguration.getSamplesInFiles().get(fileId);
    }

    protected LinkedHashSet<String> getSampleNamesInFile(Integer fileId) {
        LinkedHashSet<String> samples = new LinkedHashSet<>();
        getSamplesInFile(fileId).forEach(sampleId -> {
            samples.add(studyConfiguration.getSampleIds().inverse().get(sampleId));
        });
        return samples;
    }

    protected LinkedHashMap<String, Integer> getSamplesPosition(Integer fileId) {
        if (!samplesPositionMap.containsKey(fileId)) {
            synchronized (samplesPositionMap) {
                if (!samplesPositionMap.containsKey(fileId)) {
                    LinkedHashMap<String, Integer> samplesPosition = new LinkedHashMap<>();
                    for (Integer sampleId : studyConfiguration.getSamplesInFiles().get(fileId)) {
                        samplesPosition.put(studyConfiguration.getSampleIds().inverse().get(sampleId), samplesPosition.size());
                    }
                    samplesPositionMap.put(fileId, samplesPosition);
                }
            }
        }
        return samplesPositionMap.get(fileId);
    }

    public List<String> buildFormat(StudyConfiguration studyConfiguration) {
        List<String> format = new LinkedList<>();
        if (!excludeGenotypes) {
            format.add(VariantMerger.GT_KEY);
        }
        format.addAll(studyConfiguration.getAttributes().getAsStringList(VariantStorageEngine.Options.EXTRA_GENOTYPE_FIELDS.key()));
        return format;
    }

    public static boolean loadUnknownGenotypes(StudyConfiguration studyConfiguration) {
        Logger logger = LoggerFactory.getLogger(MongoDBVariantMerger.class);
        String defaultGenotype = studyConfiguration.getAttributes().getString(DEFAULT_GENOTYPE.key(), "");
        if (defaultGenotype.equals(DocumentToSamplesConverter.UNKNOWN_GENOTYPE)) {
            logger.debug("Do not need fill unknown genotype array. DefaultGenotype is UNKNOWN_GENOTYPE({}).",
                    DocumentToSamplesConverter.UNKNOWN_GENOTYPE);
            return false;
        } else if (getExcludeGenotypes(studyConfiguration)) {
            logger.debug("Do not need fill unknown genotype array. Excluding genotypes.");
            return false;
        } else {
            return true;
        }
    }

    public static boolean getExcludeGenotypes(StudyConfiguration studyConfiguration) {
        return studyConfiguration.getAttributes().getBoolean(VariantStorageEngine.Options.EXCLUDE_GENOTYPES.key(),
                VariantStorageEngine.Options.EXCLUDE_GENOTYPES.defaultValue());
    }
}
