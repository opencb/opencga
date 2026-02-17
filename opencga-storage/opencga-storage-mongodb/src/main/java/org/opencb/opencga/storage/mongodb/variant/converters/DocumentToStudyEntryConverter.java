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

package org.opencb.opencga.storage.mongodb.variant.converters;

import org.apache.commons.lang3.StringUtils;
import org.bson.Document;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.*;
import org.opencb.biodata.tools.variant.merge.VariantMerger;
import org.opencb.commons.datastore.mongodb.GenericDocumentComplexConverter;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;

import java.io.IOException;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import static org.opencb.opencga.storage.core.variant.adaptors.GenotypeClass.NA_GT_VALUE;
import static org.opencb.opencga.storage.core.variant.adaptors.GenotypeClass.UNKNOWN_GENOTYPE;

/**
 * @author Cristina Yenyxe Gonzalez Garcia <cyenyxe@ebi.ac.uk>
 */
public class DocumentToStudyEntryConverter {

    public static final String STUDYID_FIELD = "sid";
    //    public static final String FORMAT_FIELD = "fm";
    public static final String GENOTYPES_FIELD = "gt";

    public static final String FILES_FIELD = "files";
    public static final String FILEID_FIELD = "fid";
    public static final String SAMPLE_DATA_FIELD = "sampleData";
    /** Per-file genotype map for multi-file samples. Same shape as {@link #GENOTYPES_FIELD}: {@code {gt: [sampleId, ...]}}. */
    public static final String MULTI_FILE_GENOTYPE_FIELD = "mgt";
    public static final String ATTRIBUTES_FIELD = "attrs";
    public static final String ORI_FIELD = "_ori";

    public static final String ALTERNATES_FIELD = "alts";
    public static final String ALTERNATES_CHR = "chr";
    public static final String ALTERNATES_ALT = "alt";
    public static final String ALTERNATES_REF = "ref";
    public static final String ALTERNATES_START = "start";
    public static final String ALTERNATES_END = "end";
    public static final String ALTERNATES_TYPE = "type";

    private boolean includeSrc;
    private Map<Integer, List<Integer>> returnedFiles;
    private final Map<Integer, String> fileIds = new HashMap<>();

    //    private Integer fileId;
    private DocumentToSamplesConverter samplesConverter;
    private VariantStorageMetadataManager metadataManager = null;
    private final Map<Integer, String> studyIds = new HashMap<>();

    /**
     * Create a converter between VariantSourceEntry and Document entities when
     * there is no need to provide a list of samples or statistics.
     *
     * @param includeSrc If true, will include and gzip the "src" attribute in the Document
     */
    public DocumentToStudyEntryConverter(boolean includeSrc) {
        this.includeSrc = includeSrc;
        this.samplesConverter = null;
        this.returnedFiles = null;
    }


    /**
     * Create a converter from VariantSourceEntry to Document entities. A
     * samples converter and a statistics converter may be provided in case those
     * should be processed during the conversion.
     *
     * @param includeSrc       If true, will include and gzip the "src" attribute in the Document
     * @param samplesConverter The object used to convert the samples. If null, won't convert
     */
    public DocumentToStudyEntryConverter(boolean includeSrc, DocumentToSamplesConverter samplesConverter) {
        this(includeSrc);
        this.samplesConverter = samplesConverter;
    }

    /**
     * Create a converter from VariantSourceEntry to Document entities. A
     * samples converter and a statistics converter may be provided in case those
     * should be processed during the conversion.
     *
     * @param includeSrc       If true, will include and gzip the "src" attribute in the Document
     * @param returnedFiles    If present, reads the information of this files from FILES_FIELD
     * @param samplesConverter The object used to convert the samples. If null, won't convert
     */
    public DocumentToStudyEntryConverter(boolean includeSrc, Map<Integer, List<Integer>> returnedFiles,
                                         DocumentToSamplesConverter samplesConverter) {

        this(includeSrc);
        this.returnedFiles = returnedFiles;
        this.samplesConverter = samplesConverter;
    }


    public DocumentToStudyEntryConverter(boolean includeSrc, int studyId, int fileId,
                                         DocumentToSamplesConverter samplesConverter) {
        this(includeSrc, Collections.singletonMap(studyId, Collections.singletonList(fileId)), samplesConverter);
    }

    public void setMetadataManager(VariantStorageMetadataManager metadataManager) {
        this.metadataManager = metadataManager;
    }

    public void addStudyName(int studyId, String studyName) {
        this.studyIds.put(studyId, studyName);
    }

    /**
     * Convert a study sub-document to a {@link StudyEntry}.
     *
     * <p>When {@code variant} is provided and the study's files carry different per-file secondary
     * alternate lists, {@link VariantMerger} is used to unify the alternate lists and remap sample
     * genotype allele indices accordingly (same approach as HBaseToStudyEntryConverter).
     *
     * @param document BSON study sub-document
     * @param variant  The owning variant (used for VariantMerger construction); may be {@code null},
     *                 in which case a simple union is used without GT remapping.
     * @return Populated {@link StudyEntry}
     */
    public StudyEntry convertToDataModelType(Document document, Variant variant) {
        int studyId = ((Number) document.get(STUDYID_FIELD)).intValue();
        StudyEntry study = new StudyEntry(getStudyName(studyId));

        // Ordered map: fileId → per-file secondary alternates (only files that have any)
        Map<Integer, List<AlternateCoordinate>> fileIndexToAlts = new LinkedHashMap<>();

        if (document.containsKey(FILES_FIELD)) {
            List<FileEntry> files = new ArrayList<>(((List) document.get(FILES_FIELD)).size());
            // Files that are not in returnedFiles (e.g. because they were filtered out by the query) are collected here as "extra files"
            // with no attributes and only originalCall if available, to be added to the study if no other file is returned.
            List<FileEntry> extraFiles = new ArrayList<>();

            for (Document fileDocument : (List<Document>) document.get(FILES_FIELD)) {
                Integer fid = ((Number) fileDocument.get(FILEID_FIELD)).intValue();
                if (fid < 0) {
                    fid = -fid;
                }

                OriginalCall call = null;
                if (fileDocument.containsKey(ORI_FIELD)) {
                    Document ori = (Document) fileDocument.get(ORI_FIELD);
                    call = new OriginalCall(ori.getString("s"), ori.getInteger("i"));
                }
                if (returnedFiles != null && !returnedFiles.getOrDefault(studyId, Collections.emptyList()).contains(fid)) {
                    // Always return originalCall when context allele is missing
                    if (call != null) {
                        FileEntry fileEntry = new FileEntry(getFileName(studyId, fid), call, Collections.emptyMap());
                        extraFiles.add(fileEntry);
                    }
                    continue;
                }
                HashMap<String, String> attributes = new HashMap<>();
                FileEntry fileEntry = new FileEntry(getFileName(studyId, fid), call, attributes);
                int fileIndex = files.size();
                files.add(fileEntry);

                // Collect per-file secondary alternates
                List<Document> altDocs = (List<Document>) fileDocument.get(ALTERNATES_FIELD);
                if (altDocs != null && !altDocs.isEmpty()) {
                    List<AlternateCoordinate> alts = new ArrayList<>(altDocs.size());
                    for (Document altDoc : altDocs) {
                        alts.add(convertToAlternateCoordinate(altDoc));
                    }
                    fileIndexToAlts.put(fileIndex, alts);
                }

                // Attributes
                if (fileDocument.containsKey(ATTRIBUTES_FIELD)) {
                    Map<String, Object> attrs = ((Document) fileDocument.get(ATTRIBUTES_FIELD));
                    for (Map.Entry<String, Object> entry : attrs.entrySet()) {
                        // Unzip the "src" field, if available
                        if (entry.getKey().equals("src")) {
                            if (includeSrc) {
                                byte[] o = (byte[]) entry.getValue();
                                try {
                                    attributes.put(entry.getKey(), org.opencb.commons.utils.StringUtils.gunzip(o));
                                } catch (IOException ex) {
                                    Logger.getLogger(DocumentToStudyEntryConverter.class.getName()).log(Level.SEVERE, null, ex);
                                }
                            }
                        } else {
                            attributes.put(StringUtils.replace(entry.getKey(), GenericDocumentComplexConverter.TO_REPLACE_DOTS, "."),
                                    entry.getValue().toString());
                        }
                    }
                }
            }
            if (!files.isEmpty()) {
                study.setFiles(files);
            } else {
                study.setFiles(extraFiles);
            }
        }

        // Populate samples BEFORE secondary alternates so that we have sample GTs to remap.
        if (samplesConverter != null) {
            samplesConverter.convertToDataModelType(document, study, studyId);
        }

        // Merge per-file secondary alternates, remapping GT indices when files differ.
        addSecondaryAlternates(study, studyId, fileIndexToAlts, variant);

        return study;
    }

    /**
     * Set {@code study}'s secondary alternates, merging per-file alternate lists with
     * {@link VariantMerger} when files carry different sets.
     *
     * <p>When all files share the same alternates (the common single-file case) the method simply
     * calls {@code study.setSecondaryAlternates()} with no merger overhead. When files differ,
     * one {@link Variant} per distinct alternate set is constructed from the already-populated
     * sample data and {@link VariantMerger#merge} produces the unified list with remapped GT allele
     * indices, which are written back to the study's sample entries.
     *
     * <p>If {@code variant} is {@code null} or {@code metadataManager} is not set a simple
     * union-dedup fallback is used without GT remapping.
     *
     * @param studyEntry           The study to update with the merged secondary alternates
     * @param studyId         The study ID (used to resolve file and sample names from metadataManager)
     * @param fileIndexToAlts Map of file index to its list of secondary alternates (only for files that have any).
     *                        File index is the position of the file in {@code study.getFiles()}.
     * @param variant         The owning variant (used for VariantMerger construction);
     *                        may be {@code null}, in which case a simple union is used without GT remapping
     */
    private void addSecondaryAlternates(StudyEntry studyEntry, int studyId,
                                        Map<Integer, List<AlternateCoordinate>> fileIndexToAlts,
                                        Variant variant) {
        if (fileIndexToAlts.isEmpty()) {
            return;
        }

        // Group files by their alternate set (keyed by a canonical string for equality checks).
        LinkedHashMap<String, List<AlternateCoordinate>> altSetKeyToAlts = new LinkedHashMap<>();
        Map<Integer, String> fileIndexToAltSetKey = new HashMap<>();
        for (Map.Entry<Integer, List<AlternateCoordinate>> entry : fileIndexToAlts.entrySet()) {
            int fileIndex = entry.getKey();
            List<AlternateCoordinate> alts = entry.getValue();
            String key = alts.stream()
                    .map(a -> (a.getChromosome() == null ? "" : a.getChromosome()) + ":"
                            + (a.getStart() == null ? "" : a.getStart()) + ":"
                            + (a.getAlternate() == null ? "" : a.getAlternate()))
                    .collect(Collectors.joining("|"));
            altSetKeyToAlts.putIfAbsent(key, alts);
            fileIndexToAltSetKey.put(fileIndex, key);
        }

        if (altSetKeyToAlts.size() == 1) {
            // All files share the same secondary alternates — no merge needed.
            studyEntry.setSecondaryAlternates(altSetKeyToAlts.values().iterator().next());
            return;
        }

        // Multiple distinct alternate sets: merge or fallback.
        if (variant == null || metadataManager == null) {
            throw new IllegalStateException("Cannot merge secondary alternates: missing variant or metadata manager");
        }

        // Full merge with VariantMerger to remap genotype allele indices.
        VariantMerger variantMerger = new VariantMerger(false);
        variantMerger.setExpectedFormats(studyEntry.getSampleDataKeys());
        variantMerger.setStudyId("0");

        List<Variant> variants = new ArrayList<>(altSetKeyToAlts.size());
        // Samples whose GT was unknown/NA: temporarily replaced with "0/0" for the merger and
        // restored afterwards (same approach as HBaseToStudyEntryConverter).
        Map<String, String> specialGenotypes = new HashMap<>();

        for (Map.Entry<String, List<AlternateCoordinate>> entry : altSetKeyToAlts.entrySet()) {
            String altSetKey = entry.getKey();
            List<AlternateCoordinate> alts = entry.getValue();

            Variant perFileVariant = new Variant(
                    variant.getChromosome(), variant.getStart(),
                    variant.getEnd(), variant.getReference(), variant.getAlternate());
            StudyEntry perFileStudy = new StudyEntry("0");
            perFileStudy.setSecondaryAlternates(alts);
            perFileStudy.setSampleDataKeys(studyEntry.getSampleDataKeys());
            perFileStudy.setSamples(new ArrayList<>());

            // Add samples from all files that carry this alternate set.
            for (Map.Entry<Integer, String> fe : fileIndexToAltSetKey.entrySet()) {
                if (!fe.getValue().equals(altSetKey)) {
                    continue;
                }
                int fileIndex = fe.getKey();
                String fileName = studyEntry.getFiles().get(fileIndex).getFileId();
                int fileId = metadataManager.getFileIdOrFail(studyId, fileName);
                for (Integer sampleId : metadataManager.getSampleIdsFromFileId(studyId, fileId)) {
                    String sampleName = metadataManager.getSampleName(studyId, sampleId);
                    // Skip sampleEntries from other files
                    boolean foundSampleInIssues = false;
                    SampleEntry sampleEntry = studyEntry.getSample(sampleName);
                    if (sampleEntry == null) {
                        continue;
                    }
                    Integer thisFileIndex = sampleEntry.getFileIndex();
                    if (thisFileIndex != fileIndex) {
                        // This sample is not from this file. Search in issues.
                        for (IssueEntry issue : studyEntry.getIssues()) {
                            if (issue.getSample().getSampleId().equals(sampleName) && issue.getSample().getFileIndex().equals(fileIndex)) {
                                sampleEntry = issue.getSample();
                                foundSampleInIssues = true;
                            }
                        }
                        if (!foundSampleInIssues) {
                            // Sample not found. Skip this.
                            continue;
                        }
                    }
                    if (foundSampleInIssues) {
                        sampleName = sampleName + "_ISSUE+" + fileIndex;
                    }

                    List<String> data = new ArrayList<>(sampleEntry.getData());
                    // VariantMerger cannot handle unknown/NA GTs; use 0/0 as placeholder.
                    if (!data.isEmpty() && isUnknownOrNaGenotype(data.get(0))) {
                        specialGenotypes.put(sampleName, data.get(0));
                        data.set(0, "0/0");
                    }
                    perFileStudy.addSampleData(sampleName, data);
                }
            }

            perFileVariant.addStudyEntry(perFileStudy);
            variants.add(perFileVariant);
        }

        // Merge: the first variant acts as template; remaining variants provide additional alts.
        Variant mergedVariant = variantMerger.merge(variants.get(0), variants.subList(1, variants.size()));
        StudyEntry mergedStudy = mergedVariant.getStudies().get(0);

        // Apply remapped GTs back to the original study's sample entries.
        for (String sampleName : mergedStudy.getOrderedSamplesName()) {
            SampleEntry mergedSampleEntry = mergedStudy.getSample(sampleName);
            if (mergedSampleEntry == null) {
                continue;
            }
            if (specialGenotypes.containsKey(sampleName)) {
                mergedSampleEntry.getData().set(0, specialGenotypes.get(sampleName));
            }

            if (sampleName.contains("_ISSUE+")) {
                int idx1 = sampleName.lastIndexOf("_");
                int idx2 = sampleName.lastIndexOf("+");
                int fileIndex = Integer.parseInt(sampleName.substring(idx2 + 1));
                sampleName = sampleName.substring(0, idx1);

                for (IssueEntry issue : studyEntry.getIssues()) {
                    if (issue.getSample().getFileIndex().equals(fileIndex) && issue.getSample().getSampleId().equals(sampleName)) {
                        issue.getSample().setData(mergedSampleEntry.getData());
                        break;
                    }
                }
            } else {
                SampleEntry sampleEntry = studyEntry.getSample(sampleName);
                sampleEntry.setData(mergedSampleEntry.getData());
            }
        }
        for (FileEntry mergedFileEntry : mergedStudy.getFiles()) {
            studyEntry.getFile(mergedFileEntry.getFileId()).setData(mergedFileEntry.getData());
        }

        studyEntry.setSecondaryAlternates(mergedStudy.getSecondaryAlternates());
    }

    private static boolean isUnknownOrNaGenotype(String gt) {
        return gt == null || gt.isEmpty() || gt.equals(UNKNOWN_GENOTYPE) || gt.equals(NA_GT_VALUE);
    }

    public static AlternateCoordinate convertToAlternateCoordinate(Document alternateDocument) {
        VariantType variantType = null;
        String type = (String) alternateDocument.get(ALTERNATES_TYPE);

        if (type != null && !type.isEmpty()) {
            variantType = VariantType.valueOf(type);
        }

        return new AlternateCoordinate(
                (String) alternateDocument.get(ALTERNATES_CHR),
                (Integer) alternateDocument.get(ALTERNATES_START),
                (Integer) alternateDocument.get(ALTERNATES_END),
                (String) alternateDocument.get(ALTERNATES_REF),
                (String) alternateDocument.get(ALTERNATES_ALT),
                variantType);
    }

    public String getStudyName(int studyId) {
        return studyIds.computeIfAbsent(studyId, s -> {
            if (metadataManager == null) {
                return String.valueOf(studyId);
            } else {
                String studyName = metadataManager.getStudyName(studyId);
                if (studyName == null) {
                    return String.valueOf(studyId);
                } else {
                    return studyName;
                }
            }
        });
    }

    public String getFileName(int studyId, int fileId) {
        return fileIds.computeIfAbsent(fileId, f -> {
            if (metadataManager == null) {
                return Integer.toString(fileId);
            } else {
                String fileName = metadataManager.getFileName(studyId, fileId);
                if (fileName == null) {
                    return String.valueOf(fileId);
                } else {
                    return fileName;
                }
            }
        });
    }

    public DocumentToSamplesConverter getSamplesConverter() {
        return samplesConverter;
    }

    public void setIncludeSrc(boolean includeSrc) {
        this.includeSrc = includeSrc;
    }
}
