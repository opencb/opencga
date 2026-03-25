package org.opencb.opencga.catalog.utils;

import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.models.clinical.Disorder;
import org.opencb.biodata.models.clinical.Phenotype;
import org.opencb.biodata.models.core.SexOntologyTermAnnotation;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.core.models.clinical.ClinicalAnalysis;
import org.opencb.opencga.core.models.individual.Individual;
import org.opencb.opencga.core.models.individual.IndividualReferenceParam;
import org.opencb.opencga.core.models.individual.IndividualUpdateParams;
import org.opencb.opencga.core.models.panel.Panel;
import org.opencb.opencga.core.models.sample.Sample;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Utility class for parsing samplesheet files and registering samples, individuals,
 * and clinical analyses in the OpenCGA catalog.
 *
 * <p>Supports CSV and TSV formats with optional columns in any order:
 * file, sample, somatic, individual, gender/sex, father, mother, disorder, phenotype,
 * family, case, panel, proband. Header line may start with '#'.</p>
 */
public class SamplesheetUtils {

    private static final Logger LOGGER = LoggerFactory.getLogger(SamplesheetUtils.class);

    /**
     * Parse and validate a samplesheet, then create missing individuals, samples, and clinical analyses.
     *
     * <p>Follows a validate-then-create approach: all entries are parsed and validated
     * before any creation happens, to avoid partial state since there are no transactions.</p>
     *
     * @param catalogManager CatalogManager instance.
     * @param studyStr       Study identifier.
     * @param content        Samplesheet file content as string.
     * @param token          Authentication token.
     * @return Map with counts of created and existing entities.
     * @throws CatalogException if validation or creation fails.
     */
    public static Map<String, Integer> register(CatalogManager catalogManager, String studyStr, String content, String token)
            throws CatalogException {
        // Phase 1: Parse samplesheet
        List<SamplesheetEntry> entries = parse(content);
        if (entries.isEmpty()) {
            throw new CatalogException("Samplesheet is empty or has no valid entries");
        }
        LOGGER.info("Parsed {} entries from samplesheet", entries.size());

        // Phase 2: Validate everything before creating anything
        validate(catalogManager, studyStr, token, entries);

        // Phase 3: Create entities
        return create(catalogManager, studyStr, token, entries);
    }

    // -----------------------------------------------------------------------
    // Phase 1: Parse
    // -----------------------------------------------------------------------

    /**
     * Parse a samplesheet content string into a list of validated entries.
     *
     * @param content Samplesheet file content as string.
     * @return List of parsed samplesheet entries.
     * @throws CatalogException if the content is invalid.
     */
    public static List<SamplesheetEntry> parse(String content) throws CatalogException {
        if (StringUtils.isEmpty(content)) {
            throw new CatalogException("Samplesheet content is empty");
        }

        String[] lines = content.split("\\r?\\n");
        if (lines.length < 2) {
            throw new CatalogException("Samplesheet must have at least a header line and one data line");
        }

        // Detect separator from header line
        String headerLine = lines[0].trim();
        if (headerLine.startsWith("#")) {
            headerLine = headerLine.substring(1).trim();
        }
        String separator = headerLine.contains("\t") ? "\t" : ",";

        // Parse header — find column indices (case-insensitive)
        String[] headers = headerLine.split(separator);
        Map<String, Integer> headerMap = new LinkedHashMap<>();
        for (int i = 0; i < headers.length; i++) {
            headerMap.put(headers[i].trim().toLowerCase(), i);
        }

        if (!headerMap.containsKey("sample")) {
            throw new CatalogException("Samplesheet header must contain the 'sample' column. "
                    + "Found columns: " + headerMap.keySet());
        }

        // Resolve column indices — support 'gender' as alias for 'sex', 'phenotypes' as alias for 'phenotype'
        Integer sampleIdx = headerMap.get("sample");
        Integer individualIdx = headerMap.get("individual");
        Integer sexIdx = headerMap.containsKey("sex") ? headerMap.get("sex") : headerMap.get("gender");
        Integer somaticIdx = headerMap.get("somatic");
        Integer fatherIdx = headerMap.get("father");
        Integer motherIdx = headerMap.get("mother");
        Integer disorderIdx = headerMap.get("disorder");
        Integer familyIdx = headerMap.get("family");
        Integer fileIdx = headerMap.get("file");
        Integer caseIdx = headerMap.get("case");
        Integer panelIdx = headerMap.get("panel");
        Integer probandIdx = headerMap.get("proband");
        Integer phenotypeIdx = headerMap.containsKey("phenotype") ? headerMap.get("phenotype") : headerMap.get("phenotypes");

        // Parse data lines
        List<SamplesheetEntry> entries = new ArrayList<>();
        Set<String> seenSamples = new LinkedHashSet<>();

        for (int lineNum = 1; lineNum < lines.length; lineNum++) {
            String line = lines[lineNum].trim();
            if (line.isEmpty() || line.startsWith("#")) {
                continue;
            }

            String[] values = line.split(separator, -1);

            String sampleId = getColumnValue(values, sampleIdx);
            String rawIndividualId = getColumnValue(values, individualIdx);
            boolean explicitIndividual = StringUtils.isNotEmpty(rawIndividualId);

            if (StringUtils.isEmpty(sampleId)) {
                LOGGER.warn("Skipping samplesheet line {} — empty sample ID", lineNum + 1);
                continue;
            }

            // Individual ID defaults to sample ID
            String individualId = explicitIndividual ? rawIndividualId : sampleId;

            String sex = getColumnValue(values, sexIdx);
            if (StringUtils.isNotEmpty(sex)) {
                String sexUpper = sex.toUpperCase();
                if (!sexUpper.equals("MALE") && !sexUpper.equals("FEMALE") && !sexUpper.equals("UNKNOWN")) {
                    throw new CatalogException("Invalid sex/gender value '" + sex + "' at line " + (lineNum + 1)
                            + ". Valid values: male, female, unknown");
                }
            }

            if (seenSamples.contains(sampleId)) {
                throw new CatalogException("Duplicate sample ID '" + sampleId + "' at line " + (lineNum + 1));
            }
            seenSamples.add(sampleId);

            entries.add(new SamplesheetEntry(
                    sampleId, individualId, explicitIndividual, sex,
                    getColumnValue(values, somaticIdx),
                    getColumnValue(values, fatherIdx),
                    getColumnValue(values, motherIdx),
                    getColumnValue(values, disorderIdx),
                    getColumnValue(values, phenotypeIdx),
                    getColumnValue(values, familyIdx),
                    getColumnValue(values, fileIdx),
                    getColumnValue(values, caseIdx),
                    getColumnValue(values, panelIdx),
                    getColumnValue(values, probandIdx)));
        }

        return entries;
    }

    // -----------------------------------------------------------------------
    // Phase 2: Validate
    // -----------------------------------------------------------------------

    private static void validate(CatalogManager catalogManager, String studyStr, String token,
                                 List<SamplesheetEntry> entries) throws CatalogException {
        Set<String> allIndividualIds = new LinkedHashSet<>();
        Set<String> allFileIds = new LinkedHashSet<>();
        Set<String> allFamilyIds = new LinkedHashSet<>();
        Set<String> allPanelIds = new LinkedHashSet<>();
        Set<String> allFatherIds = new LinkedHashSet<>();
        Set<String> allMotherIds = new LinkedHashSet<>();

        for (SamplesheetEntry entry : entries) {
            allIndividualIds.add(entry.getIndividualId());
            if (StringUtils.isNotEmpty(entry.getFather())) {
                allFatherIds.add(entry.getFather());
            }
            if (StringUtils.isNotEmpty(entry.getMother())) {
                allMotherIds.add(entry.getMother());
            }
            if (StringUtils.isNotEmpty(entry.getFamily())) {
                allFamilyIds.add(entry.getFamily());
            }
            if (StringUtils.isNotEmpty(entry.getFile())) {
                allFileIds.add(entry.getFile());
            }
            if (StringUtils.isNotEmpty(entry.getPanel())) {
                allPanelIds.add(entry.getPanel());
            }
        }

        // Validate files exist
        for (String fileRef : allFileIds) {
            try {
                catalogManager.getFileManager().get(studyStr, fileRef, QueryOptions.empty(), token).first();
            } catch (CatalogException e) {
                throw new CatalogException("File '" + fileRef + "' referenced in samplesheet not found in catalog");
            }
        }

        // Validate families exist
        for (String familyId : allFamilyIds) {
            try {
                catalogManager.getFamilyManager().get(studyStr, familyId, QueryOptions.empty(), token).first();
            } catch (CatalogException e) {
                throw new CatalogException("Family '" + familyId + "' referenced in samplesheet not found in catalog");
            }
        }

        // Validate panels exist
        for (String panelId : allPanelIds) {
            try {
                catalogManager.getPanelManager().get(studyStr, panelId, QueryOptions.empty(), token).first();
            } catch (CatalogException e) {
                throw new CatalogException("Panel '" + panelId + "' referenced in samplesheet not found in catalog");
            }
        }

        // Validate father/mother IDs are either in the samplesheet or already exist in catalog
        for (String fatherId : allFatherIds) {
            if (!allIndividualIds.contains(fatherId)) {
                try {
                    catalogManager.getIndividualManager().get(studyStr, fatherId, QueryOptions.empty(), token);
                } catch (CatalogException e) {
                    throw new CatalogException("Father '" + fatherId
                            + "' referenced in samplesheet not found in catalog and not defined as an individual in the samplesheet");
                }
            }
        }
        for (String motherId : allMotherIds) {
            if (!allIndividualIds.contains(motherId)) {
                try {
                    catalogManager.getIndividualManager().get(studyStr, motherId, QueryOptions.empty(), token);
                } catch (CatalogException e) {
                    throw new CatalogException("Mother '" + motherId
                            + "' referenced in samplesheet not found in catalog and not defined as an individual in the samplesheet");
                }
            }
        }
    }

    // -----------------------------------------------------------------------
    // Phase 3: Create
    // -----------------------------------------------------------------------

    private static Map<String, Integer> create(CatalogManager catalogManager, String studyStr, String token,
                                                List<SamplesheetEntry> entries) throws CatalogException {
        int individualsCreated = 0;
        int samplesCreated = 0;
        int casesCreated = 0;
        int existingIndividualCount = 0;
        int existingSampleCount = 0;

        // Resolve existing samples and their individual links.
        // For each entry, determine the effective individual ID:
        //   1. Sample doesn't exist → use entry.individualId (defaults to sampleId)
        //   2. Sample exists with no individual → use entry.individualId
        //   3. Sample exists with individual → if entry has explicit individual and it differs → fail
        Map<String, String> effectiveIndividualMap = new LinkedHashMap<>(); // sampleId -> effective individualId
        Set<String> existingSampleIds = new LinkedHashSet<>();

        for (SamplesheetEntry entry : entries) {
            String sampleId = entry.getSampleId();
            String requestedIndId = entry.getIndividualId();
            boolean hasExplicitIndividual = entry.hasExplicitIndividual();

            Sample existingSample = null;
            try {
                existingSample = catalogManager.getSampleManager()
                        .get(studyStr, sampleId, QueryOptions.empty(), token).first();
                existingSampleIds.add(sampleId);
            } catch (CatalogException e) {
                // Sample does not exist
            }

            if (existingSample != null) {
                String existingIndId = existingSample.getIndividualId();
                if (StringUtils.isNotEmpty(existingIndId)) {
                    // Case 3: sample exists with individual
                    if (hasExplicitIndividual && !existingIndId.equals(requestedIndId)) {
                        throw new CatalogException("Sample '" + sampleId + "' is already linked to individual '"
                                + existingIndId + "', but samplesheet specifies individual '" + requestedIndId + "'");
                    }
                    effectiveIndividualMap.put(sampleId, existingIndId);
                } else {
                    // Case 2: sample exists with no individual
                    effectiveIndividualMap.put(sampleId, requestedIndId);
                }
            } else {
                // Case 1: sample doesn't exist
                effectiveIndividualMap.put(sampleId, requestedIndId);
            }
        }

        // First pass: create individuals without parent links
        Set<String> processedIndividuals = new LinkedHashSet<>();
        Set<String> existingIndividualIds = new LinkedHashSet<>();
        for (SamplesheetEntry entry : entries) {
            String indId = effectiveIndividualMap.get(entry.getSampleId());
            if (processedIndividuals.contains(indId) || existingIndividualIds.contains(indId)) {
                continue;
            }

            // Check if individual already exists
            boolean exists = false;
            try {
                catalogManager.getIndividualManager().get(studyStr, indId, QueryOptions.empty(), token);
                exists = true;
                existingIndividualIds.add(indId);
            } catch (CatalogException e) {
                // Does not exist
            }

            if (!exists) {
                Individual individual = new Individual().setId(indId);
                individual.setSex(entry.getSexOntologyTerm());
                if (StringUtils.isNotEmpty(entry.getDisorder())) {
                    individual.setDisorders(Collections.singletonList(
                            new Disorder(entry.getDisorder(), null, null, null, null, null, null)));
                }
                if (StringUtils.isNotEmpty(entry.getPhenotype())) {
                    individual.setPhenotypes(Collections.singletonList(
                            new Phenotype(entry.getPhenotype(), entry.getPhenotype(), "")));
                }
                catalogManager.getIndividualManager().create(studyStr, individual, QueryOptions.empty(), token);
                processedIndividuals.add(indId);
                individualsCreated++;
                LOGGER.info("Created individual '{}'", indId);
            }
        }

        // Second pass: update parent links for newly created individuals
        for (SamplesheetEntry entry : entries) {
            String indId = effectiveIndividualMap.get(entry.getSampleId());
            if (StringUtils.isNotEmpty(entry.getFather()) || StringUtils.isNotEmpty(entry.getMother())) {
                if (processedIndividuals.contains(indId)) {
                    IndividualUpdateParams updateParams = new IndividualUpdateParams();
                    if (StringUtils.isNotEmpty(entry.getFather())) {
                        updateParams.setFather(new IndividualReferenceParam(entry.getFather(), null));
                    }
                    if (StringUtils.isNotEmpty(entry.getMother())) {
                        updateParams.setMother(new IndividualReferenceParam(entry.getMother(), null));
                    }
                    catalogManager.getIndividualManager().update(studyStr, indId, updateParams, QueryOptions.empty(), token);
                    LOGGER.info("Updated individual '{}' with father='{}', mother='{}'",
                            indId, entry.getFather(), entry.getMother());
                }
            }
        }

        // Third pass: create samples linked to their individuals
        for (SamplesheetEntry entry : entries) {
            if (!existingSampleIds.contains(entry.getSampleId())) {
                String indId = effectiveIndividualMap.get(entry.getSampleId());
                Sample sample = new Sample()
                        .setId(entry.getSampleId())
                        .setIndividualId(indId)
                        .setSomatic(entry.isSomatic());
                catalogManager.getSampleManager().create(studyStr, sample, QueryOptions.empty(), token);
                samplesCreated++;
                LOGGER.info("Created sample '{}' linked to individual '{}'", entry.getSampleId(), indId);
            }
        }

        existingIndividualCount = existingIndividualIds.size();
        existingSampleCount = existingSampleIds.size();

        // Fourth pass: create ClinicalAnalysis for entries with 'case' column
        for (SamplesheetEntry entry : entries) {
            if (StringUtils.isNotEmpty(entry.getCaseId())) {
                boolean caseExists = false;
                try {
                    catalogManager.getClinicalAnalysisManager().get(studyStr, entry.getCaseId(),
                            QueryOptions.empty(), token).first();
                    caseExists = true;
                } catch (CatalogException e) {
                    // Does not exist
                }
                if (!caseExists) {
                    ClinicalAnalysis clinicalAnalysis = new ClinicalAnalysis()
                            .setId(entry.getCaseId())
                            .setType(ClinicalAnalysis.Type.SINGLE)
                            .setProband(new Individual().setId(effectiveIndividualMap.get(entry.getSampleId())));
                    if (StringUtils.isNotEmpty(entry.getPanel())) {
                        clinicalAnalysis.setPanels(Collections.singletonList(new Panel().setId(entry.getPanel())));
                    }
                    catalogManager.getClinicalAnalysisManager().create(studyStr, clinicalAnalysis,
                            true, QueryOptions.empty(), token);
                    casesCreated++;
                    LOGGER.info("Created clinical analysis '{}' with proband '{}'", entry.getCaseId(), entry.getIndividualId());
                }
            }
        }

        Map<String, Integer> result = new LinkedHashMap<>();
        result.put("individualsCreated", individualsCreated);
        result.put("samplesCreated", samplesCreated);
        result.put("casesCreated", casesCreated);
        result.put("individualsExisting", existingIndividualCount);
        result.put("samplesExisting", existingSampleCount);

        LOGGER.info("Samplesheet registration complete: {} individuals created, {} samples created, "
                        + "{} cases created, {} individuals existing, {} samples existing",
                individualsCreated, samplesCreated, casesCreated, existingIndividualCount, existingSampleCount);

        return result;
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    private static String getColumnValue(String[] values, Integer index) {
        if (index == null || index >= values.length) {
            return "";
        }
        return values[index].trim();
    }

    /**
     * Internal representation of a parsed samplesheet row.
     */
    public static class SamplesheetEntry {
        private final String sampleId;
        private final String individualId;
        private final boolean explicitIndividual;
        private final String sex;
        private final boolean somatic;
        private final String father;
        private final String mother;
        private final String disorder;
        private final String phenotype;
        private final String family;
        private final String file;
        private final String caseId;
        private final String panel;
        private final boolean proband;

        public SamplesheetEntry(String sampleId, String individualId, boolean explicitIndividual, String sex,
                                String somatic, String father, String mother, String disorder, String phenotype,
                                String family, String file, String caseId, String panel, String proband) {
            this.sampleId = sampleId;
            this.individualId = individualId;
            this.explicitIndividual = explicitIndividual;
            this.sex = StringUtils.isNotEmpty(sex) ? sex.toUpperCase() : "UNKNOWN";
            this.somatic = "true".equalsIgnoreCase(somatic);
            this.father = father;
            this.mother = mother;
            this.disorder = disorder;
            this.phenotype = phenotype;
            this.family = family;
            this.file = file;
            this.caseId = caseId;
            this.panel = panel;
            this.proband = "true".equalsIgnoreCase(proband);
        }

        public String getSampleId() {
            return sampleId;
        }

        public String getIndividualId() {
            return individualId;
        }

        public boolean hasExplicitIndividual() {
            return explicitIndividual;
        }

        public String getSex() {
            return sex;
        }

        public boolean isSomatic() {
            return somatic;
        }

        public String getFather() {
            return father;
        }

        public String getMother() {
            return mother;
        }

        public String getDisorder() {
            return disorder;
        }

        public String getPhenotype() {
            return phenotype;
        }

        public String getFamily() {
            return family;
        }

        public String getFile() {
            return file;
        }

        public String getCaseId() {
            return caseId;
        }

        public String getPanel() {
            return panel;
        }

        public boolean isProband() {
            return proband;
        }

        public SexOntologyTermAnnotation getSexOntologyTerm() {
            switch (sex) {
                case "MALE":
                    return SexOntologyTermAnnotation.initMale();
                case "FEMALE":
                    return SexOntologyTermAnnotation.initFemale();
                default:
                    return SexOntologyTermAnnotation.initUnknown();
            }
        }
    }
}
