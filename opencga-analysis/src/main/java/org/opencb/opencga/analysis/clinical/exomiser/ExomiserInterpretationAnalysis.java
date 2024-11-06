/*
 * Copyright 2015-2020 OpenCB
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

package org.opencb.opencga.analysis.clinical.exomiser;

import htsjdk.samtools.util.BufferedLineReader;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.models.clinical.ClinicalAnalyst;
import org.opencb.biodata.models.clinical.ClinicalProperty;
import org.opencb.biodata.models.clinical.interpretation.ClinicalVariant;
import org.opencb.biodata.models.clinical.interpretation.InterpretationMethod;
import org.opencb.biodata.models.clinical.interpretation.Software;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.exceptions.NonStandardCompliantSampleField;
import org.opencb.biodata.tools.variant.VariantNormalizer;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.ConfigurationUtils;
import org.opencb.opencga.analysis.clinical.InterpretationAnalysis;
import org.opencb.opencga.analysis.individual.qc.IndividualQcUtils;
import org.opencb.opencga.analysis.wrappers.exomiser.ExomiserWrapperAnalysis;
import org.opencb.opencga.analysis.wrappers.exomiser.ExomiserWrapperAnalysisExecutor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.core.common.GitRepositoryState;
import org.opencb.opencga.core.common.JacksonUtils;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.clinical.ClinicalAnalysis;
import org.opencb.opencga.core.models.clinical.Interpretation;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.individual.Individual;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.opencb.opencga.core.tools.annotations.Tool;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.core.variant.query.VariantQueryResult;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.util.*;

import static org.opencb.opencga.core.tools.OpenCgaToolExecutor.EXECUTOR_ID;

@Tool(id = ExomiserInterpretationAnalysis.ID, resource = Enums.Resource.CLINICAL)
public class ExomiserInterpretationAnalysis extends InterpretationAnalysis {

    public static final String ID = "interpretation-exomiser";
    public static final String DESCRIPTION = "Run exomiser interpretation analysis";

    private String studyId;
    private String clinicalAnalysisId;
    private String sampleId;
    private ClinicalAnalysis.Type clinicalAnalysisType;
    private String exomiserVersion;

    private ClinicalAnalysis clinicalAnalysis;

    @Override
    protected InterpretationMethod getInterpretationMethod() {
        return getInterpretationMethod(ID);
    }

    @Override
    protected void check() throws Exception {
        super.check();

        // Check study
        if (StringUtils.isEmpty(studyId)) {
            // Missing study
            throw new ToolException("Missing study ID");
        }

        // Check clinical analysis
        if (StringUtils.isEmpty(clinicalAnalysisId)) {
            throw new ToolException("Missing clinical analysis ID");
        }

        // Get clinical analysis to ckeck proband sample ID, family ID
        OpenCGAResult<ClinicalAnalysis> clinicalAnalysisQueryResult;
        try {
            clinicalAnalysisQueryResult = catalogManager.getClinicalAnalysisManager().get(studyId, clinicalAnalysisId, QueryOptions.empty(),
                    token);
        } catch (CatalogException e) {
            throw new ToolException(e);
        }
        if (clinicalAnalysisQueryResult.getNumResults() != 1) {
            throw new ToolException("Clinical analysis " + clinicalAnalysisId + " not found in study " + studyId);
        }

        clinicalAnalysis = clinicalAnalysisQueryResult.first();

        // Check sample from proband
        if (clinicalAnalysis.getProband() == null) {
            throw new ToolException("Missing proband in clinical analysis " + clinicalAnalysisId);
        }
        if (CollectionUtils.isEmpty(clinicalAnalysis.getProband().getSamples())) {
            throw new ToolException("Missing sample for proband " + clinicalAnalysis.getProband().getId() + " in clinical analysis "
                    + clinicalAnalysisId);
        }
        sampleId = clinicalAnalysis.getProband().getSamples().get(0).getId();

        // Check clinical analysis type
        if (clinicalAnalysis.getType() == ClinicalAnalysis.Type.FAMILY) {
            clinicalAnalysisType = ClinicalAnalysis.Type.FAMILY;
        } else {
            clinicalAnalysisType = ClinicalAnalysis.Type.SINGLE;
        }
        logger.info("The clinical analysis type is {}, so the Exomiser will be run in mode {}", clinicalAnalysis.getType(),
                clinicalAnalysisType);

        // Check exomiser version
        if (StringUtils.isEmpty(exomiserVersion)) {
            // Missing exomiser version use the default one
            exomiserVersion = ConfigurationUtils.getToolDefaultVersion(ExomiserWrapperAnalysis.ID, configuration);
            logger.warn("Missing exomiser version, using the default {}", exomiserVersion);
        }

        // Update executor params with OpenCGA home and session ID
        setUpStorageEngineExecutor(studyId);
    }

    @Override
    protected void run() throws ToolException {
        step(() -> {

            executorParams.put(EXECUTOR_ID, ExomiserWrapperAnalysisExecutor.ID);
            ExomiserWrapperAnalysisExecutor exomiserExecutor = getToolExecutor(ExomiserWrapperAnalysisExecutor.class)
                    .setStudyId(studyId)
                    .setSampleId(sampleId)
                    .setClinicalAnalysisType(clinicalAnalysisType)
                    .setExomiserVersion(exomiserVersion);

            exomiserExecutor.execute();

            saveInterpretation(studyId, clinicalAnalysis, exomiserExecutor.getDockerImageName(), exomiserExecutor.getDockerImageVersion());
        });
    }

    protected void saveInterpretation(String studyId, ClinicalAnalysis clinicalAnalysis, String dockerImage, String dockerImageVersion)
            throws ToolException, StorageEngineException,
            CatalogException, IOException {
        // Interpretation method
        InterpretationMethod method = new InterpretationMethod(getId(), GitRepositoryState.getInstance().getBuildVersion(),
                GitRepositoryState.getInstance().getCommitId(), Collections.singletonList(
                new Software()
                        .setName("Exomiser")
                        .setRepository("Docker: " + dockerImage)
                        .setVersion(dockerImageVersion)));

        // Analyst
        ClinicalAnalyst analyst = clinicalInterpretationManager.getAnalyst(studyId, token);

        List<ClinicalVariant> primaryFindings = getPrimaryFindings();

        org.opencb.biodata.models.clinical.interpretation.Interpretation interpretation = new Interpretation()
                .setPrimaryFindings(primaryFindings)
                .setSecondaryFindings(new ArrayList<>())
                .setAnalyst(analyst)
                .setClinicalAnalysisId(clinicalAnalysis.getId())
                .setCreationDate(TimeUtils.getTime())
                .setMethod(method);

        // Store interpretation analysis in DB
        try {
            catalogManager.getInterpretationManager().create(studyId, clinicalAnalysis.getId(), new Interpretation(interpretation),
                    ParamUtils.SaveInterpretationAs.SECONDARY, QueryOptions.empty(), token);
        } catch (CatalogException e) {
            throw new ToolException("Error saving interpretation into database", e);
        }

        // Save interpretation analysis in JSON file
        Path path = getOutDir().resolve(INTERPRETATION_FILENAME);
        try {
            JacksonUtils.getDefaultObjectMapper().writer().writeValue(path.toFile(), interpretation);
        } catch (IOException e) {
            throw new ToolException(e);
        }
    }

    private List<ClinicalVariant> getPrimaryFindings() throws IOException, StorageEngineException,
            CatalogException, ToolException {

        List<ClinicalVariant> primaryFindings = new ArrayList<>();

        // Prepare variant query
        List<String> sampleIds = new ArrayList<>();
        if (clinicalAnalysis.getType() == ClinicalAnalysis.Type.FAMILY && clinicalAnalysis.getFamily() != null
                && CollectionUtils.isNotEmpty(clinicalAnalysis.getFamily().getMembers())) {
            for (Individual member : clinicalAnalysis.getFamily().getMembers()) {
                Individual individual = IndividualQcUtils.getIndividualById(studyId, member.getId(), getCatalogManager(), getToken());
                if (CollectionUtils.isNotEmpty(individual.getSamples())) {
                    // Get first sample
                    sampleIds.add(individual.getSamples().get(0).getId());
                }
            }
        } else {
            sampleIds.add(clinicalAnalysis.getProband().getSamples().get(0).getId());
        }
        Query query = new Query(VariantQueryParam.STUDY.key(), studyId)
                .append(VariantQueryParam.INCLUDE_SAMPLE_ID.key(), true)
                .append(VariantQueryParam.INCLUDE_STUDY.key(), "all")
                .append(VariantQueryParam.SAMPLE.key(), StringUtils.join(sampleIds, ","));

        // Parse all Exomiser output file (TSV_VARIANT):
        //
        // 0    1   2           3               4   5       6                               7
        // RANK	ID	GENE_SYMBOL	ENTREZ_GENE_ID	MOI	P-VALUE	EXOMISER_GENE_COMBINED_SCORE	EXOMISER_GENE_PHENO_SCORE
        // 8                            9                       10                      11                  12      13      14      15
        // EXOMISER_GENE_VARIANT_SCORE	EXOMISER_VARIANT_SCORE	CONTRIBUTING_VARIANT	WHITELIST_VARIANT	VCF_ID	RS_ID	CONTIG	START
        // 16   17  18  19              20      21      22          23                  24      25
        // END	REF	ALT	CHANGE_LENGTH	QUAL	FILTER	GENOTYPE	FUNCTIONAL_CLASS	HGVS	EXOMISER_ACMG_CLASSIFICATION
        // 26                       27                          28                          29
        // EXOMISER_ACMG_EVIDENCE	EXOMISER_ACMG_DISEASE_ID	EXOMISER_ACMG_DISEASE_NAME	CLINVAR_ALLELE_ID
        // 30                               31                  32                      33
        // CLINVAR_PRIMARY_INTERPRETATION	CLINVAR_STAR_RATING	GENE_CONSTRAINT_LOEUF	GENE_CONSTRAINT_LOEUF_LOWER
        // 34                           35              36          37          38              39          40
        // GENE_CONSTRAINT_LOEUF_UPPER	MAX_FREQ_SOURCE	MAX_FREQ	ALL_FREQ	MAX_PATH_SOURCE	MAX_PATH	ALL_PATH

        VariantNormalizer normalizer = new VariantNormalizer();

        Map<String, List<String[]>> variantTsvMap = new HashMap<>();
        Map<String, String> normalizedToTsv = new HashMap<>();

        File variantTsvFile = getOutDir().resolve("exomiser_output.variants.tsv").toFile();
        if (variantTsvFile.exists()) {
            try (BufferedReader br = new BufferedLineReader(new FileInputStream(variantTsvFile))) {
                // Read and skip header line
                String line = br.readLine();
                // First line to be processed
                line = br.readLine();
                while (line != null) {
                    String[] fields = line.split("\t");
                    Variant variant = new Variant(fields[14], Integer.parseInt(fields[15]), Integer.parseInt(fields[16]), fields[17],
                            fields[18]);
                    try {
                        Variant normalized = normalizer.normalize(Collections.singletonList(variant), false).get(0);
                        String variantId = normalized.toStringSimple();
                        normalizedToTsv.put(variantId, variant.toStringSimple());
                        if (!variantTsvMap.containsKey(variantId)) {
                            variantTsvMap.put(variantId, new ArrayList<>());
                        }
                        variantTsvMap.get(variantId).add(fields);
                    } catch (NonStandardCompliantSampleField e) {
                        logger.warn("Skipping variant {}, it could not be normalized", variant.toStringSimple());
                    }

                    // Next line
                    line = br.readLine();
                }
            }
        }

        File exomiserJsonFile = getOutDir().resolve("exomiser_output.json").toFile();
        Map<String, Set<ExomiserTranscriptAnnotation>> variantTranscriptMap = getExomiserTranscriptAnnotationsFromFile(exomiserJsonFile);

        List<String> variantIds = new ArrayList<>(variantTsvMap.keySet());
        if (CollectionUtils.isNotEmpty(variantIds)) {
            query.put(VariantQueryParam.ID.key(), StringUtils.join(variantIds, ","));
            String jsonQuery = query.toJson();
            logger.info("Query (including all family samples): {}", jsonQuery);
            VariantQueryResult<Variant> variantResults = getVariantStorageManager().get(query, QueryOptions.empty(), getToken());

            if (variantResults != null && CollectionUtils.isNotEmpty(variantResults.getResults())) {
                // Exomiser clinical variant creator
                ExomiserClinicalVariantCreator clinicalVariantCreator = new ExomiserClinicalVariantCreator();

                // Convert variants to clinical variants
                for (Variant variant : variantResults.getResults()) {
                    ClinicalVariant clinicalVariant = clinicalVariantCreator.create(variant);
                    List<ExomiserTranscriptAnnotation> exomiserTranscripts = new ArrayList<>();
                    if (normalizedToTsv.containsKey(variant.toStringSimple())) {
                        if (variantTranscriptMap.containsKey(normalizedToTsv.get(variant.toStringSimple()))) {
                            exomiserTranscripts.addAll(variantTranscriptMap.get(normalizedToTsv.get(variant.toStringSimple())));
                        } else {
                            logger.warn("Variant {} (normalizedToTsv {}), not found in map variantTranscriptMap", variant.toStringSimple(),
                                    normalizedToTsv.get(variant.toStringSimple()));
                        }
                    } else {
                        logger.warn("Variant {} not found in map normalizedToTsv", variant.toStringSimple());
                    }
                    for (String[] fields : variantTsvMap.get(variant.toStringSimple())) {
                        ClinicalProperty.ModeOfInheritance moi = getModeOfInheritance(fields[4]);
                        Map<String, Object> attributes = getAttributesFromTsv(fields);

                        clinicalVariantCreator.addClinicalVariantEvidences(clinicalVariant, exomiserTranscripts, moi, attributes);
                    }
                    primaryFindings.add(clinicalVariant);
                }
            }
        }

        return primaryFindings;
    }

    private Map<String, Set<ExomiserTranscriptAnnotation>> getExomiserTranscriptAnnotationsFromFile(File file) throws IOException {
        Map<String, Set<ExomiserTranscriptAnnotation>> results = new HashMap<>();
        String content = org.apache.commons.io.FileUtils.readFileToString(file, Charset.defaultCharset());
        Object obj = JacksonUtils.getDefaultNonNullObjectMapper().readValue(content, Object.class);
        List<Object> list = (ArrayList<Object>) obj;
        for (Object item : list) {
            List<Map<String, Object>> geneScores = (ArrayList) ((Map) item).get("geneScores");
            for (Map<String, Object> geneScore : geneScores) {
                if (geneScore.containsKey("contributingVariants")) {
                    List<Map<String, Object>> contributingVariants = (ArrayList) geneScore.get("contributingVariants");
                    for (Map<String, Object> contributingVariant : contributingVariants) {
                        String variantId = contributingVariant.get("contigName") + ":" + contributingVariant.get("start") + ":"
                                + contributingVariant.get("ref") + ":" + contributingVariant.get("alt");
                        if (!results.containsKey(variantId)) {
                            results.put(variantId, new HashSet<>());
                        }
                        List<Map<String, Object>> transcriptAnnotations = (ArrayList) contributingVariant.get("transcriptAnnotations");
                        for (Map<String, Object> transcriptAnnotation : transcriptAnnotations) {
                            ExomiserTranscriptAnnotation exTranscriptAnnotation = new ExomiserTranscriptAnnotation();
                            if (transcriptAnnotation.containsKey("variantEffect")) {
                                exTranscriptAnnotation.setVariantEffect((String) transcriptAnnotation.get("variantEffect"));
                            }
                            if (transcriptAnnotation.containsKey("geneSymbol")) {
                                exTranscriptAnnotation.setGeneSymbol((String) transcriptAnnotation.get("geneSymbol"));
                            }
                            if (transcriptAnnotation.containsKey("accession")) {
                                exTranscriptAnnotation.setAccession((String) transcriptAnnotation.get("accession"));
                            }
                            if (transcriptAnnotation.containsKey("hgvsGenomic")) {
                                exTranscriptAnnotation.setHgvsGenomic((String) transcriptAnnotation.get("hgvsGenomic"));
                            }
                            if (transcriptAnnotation.containsKey("hgvsCdna")) {
                                exTranscriptAnnotation.setHgvsCdna((String) transcriptAnnotation.get("hgvsCdna"));
                            }
                            if (transcriptAnnotation.containsKey("hgvsProtein")) {
                                exTranscriptAnnotation.setHgvsProtein((String) transcriptAnnotation.get("hgvsProtein"));
                            }
                            if (transcriptAnnotation.containsKey("rankType")) {
                                exTranscriptAnnotation.setRankType((String) transcriptAnnotation.get("rankType"));
                            }
                            if (transcriptAnnotation.containsKey("rank")) {
                                exTranscriptAnnotation.setRank((Integer) transcriptAnnotation.get("rank"));
                            }
                            if (transcriptAnnotation.containsKey("rankTotal")) {
                                exTranscriptAnnotation.setRankTotal((Integer) transcriptAnnotation.get("rankTotal"));
                            }
                            results.get(variantId).add(exTranscriptAnnotation);
                        }
                    }
                }
            }
        }
        return results;
    }

    private ObjectMap getAttributesFromTsv(String[] fields) {
        ObjectMap exAttributes = new ObjectMap();

        // 0    1   2           3               4   5       6                               7
        // RANK	ID	GENE_SYMBOL	ENTREZ_GENE_ID	MOI	P-VALUE	EXOMISER_GENE_COMBINED_SCORE	EXOMISER_GENE_PHENO_SCORE
        if (fields.length >= 1) {
            exAttributes.put("RANK", fields[0]);
        }
        if (fields.length >= 6) {
            exAttributes.put("P-VALUE", fields[5]);
        }
        if (fields.length >= 7) {
            exAttributes.put("EXOMISER_GENE_COMBINED_SCORE", fields[6]);
        }
        if (fields.length >= 8) {
            exAttributes.put("EXOMISER_GENE_PHENO_SCORE", fields[7]);
        }

        // 8                            9                       10                      11                  12      13      14      15
        // EXOMISER_GENE_VARIANT_SCORE	EXOMISER_VARIANT_SCORE	CONTRIBUTING_VARIANT	WHITELIST_VARIANT	VCF_ID	RS_ID	CONTIG	START
        if (fields.length >= 9) {
            exAttributes.put("EXOMISER_GENE_VARIANT_SCORE", fields[8]);
        }
        if (fields.length >= 10) {
            exAttributes.put("EXOMISER_VARIANT_SCORE", fields[9]);
        }

        // 16   17  18  19              20      21      22          23                  24      25
        // END	REF	ALT	CHANGE_LENGTH	QUAL	FILTER	GENOTYPE	FUNCTIONAL_CLASS	HGVS	EXOMISER_ACMG_CLASSIFICATION
        if (fields.length >= 24) {
            exAttributes.put("FUNCTIONAL_CLASS", fields[23]);
        }
        if (fields.length >= 26) {
            exAttributes.put("EXOMISER_ACMG_CLASSIFICATION", fields[25]);
        }

        // 26                       27                          28                          29
        // EXOMISER_ACMG_EVIDENCE	EXOMISER_ACMG_DISEASE_ID	EXOMISER_ACMG_DISEASE_NAME	CLINVAR_ALLELE_ID
        if (fields.length >= 27) {
            exAttributes.put("EXOMISER_ACMG_EVIDENCE", fields[26]);
        }
        if (fields.length >= 28) {
            exAttributes.put("EXOMISER_ACMG_DISEASE_ID", fields[27]);
        }
        if (fields.length >= 29) {
            exAttributes.put("EXOMISER_ACMG_DISEASE_NAME", fields[28]);
        }
        if (fields.length >= 30) {
            exAttributes.put("CLINVAR_ALLELE_ID", fields[29]);
        }

        // 30                               31                  32                      33
        // CLINVAR_PRIMARY_INTERPRETATION	CLINVAR_STAR_RATING	GENE_CONSTRAINT_LOEUF	GENE_CONSTRAINT_LOEUF_LOWER
        if (fields.length >= 31) {
            exAttributes.put("CLINVAR_PRIMARY_INTERPRETATION", fields[30]);
        }
        if (fields.length >= 32) {
            exAttributes.put("CLINVAR_STAR_RATING", fields[31]);
        }
        if (fields.length >= 33) {
            exAttributes.put("GENE_CONSTRAINT_LOEUF", fields[32]);
        }
        if (fields.length >= 34) {
            exAttributes.put("GENE_CONSTRAINT_LOEUF_LOWER", fields[33]);
        }

        // 34                           35              36          37          38              39          40
        // GENE_CONSTRAINT_LOEUF_UPPER	MAX_FREQ_SOURCE	MAX_FREQ	ALL_FREQ	MAX_PATH_SOURCE	MAX_PATH	ALL_PATH
        if (fields.length >= 35) {
            exAttributes.put("GENE_CONSTRAINT_LOEUF_UPPER", fields[34]);
        }
        if (fields.length >= 39) {
            exAttributes.put("MAX_PATH_SOURCE", fields[38]);
        }
        if (fields.length >= 40) {
            exAttributes.put("MAX_PATH", fields[39]);
        }
        if (fields.length >= 41) {
            exAttributes.put("ALL_PATH", fields[40]);
        }

        return new ObjectMap("exomiser", exAttributes);
    }

    private ClinicalProperty.ModeOfInheritance getModeOfInheritance(String moi) {
        switch (moi) {
            case "AD":
                return ClinicalProperty.ModeOfInheritance.AUTOSOMAL_DOMINANT;
            case "AR":
                return ClinicalProperty.ModeOfInheritance.AUTOSOMAL_RECESSIVE;
            case "XD":
                return ClinicalProperty.ModeOfInheritance.X_LINKED_DOMINANT;
            case "XR":
                return ClinicalProperty.ModeOfInheritance.X_LINKED_RECESSIVE;
            case "MT":
                return ClinicalProperty.ModeOfInheritance.MITOCHONDRIAL;
            default:
                return ClinicalProperty.ModeOfInheritance.UNKNOWN;
        }
    }

    public String getStudyId() {
        return studyId;
    }

    public ExomiserInterpretationAnalysis setStudyId(String studyId) {
        this.studyId = studyId;
        return this;
    }

    public String getClinicalAnalysisId() {
        return clinicalAnalysisId;
    }

    public ExomiserInterpretationAnalysis setClinicalAnalysisId(String clinicalAnalysisId) {
        this.clinicalAnalysisId = clinicalAnalysisId;
        return this;
    }

    public String getExomiserVersion() {
        return exomiserVersion;
    }

    public ExomiserInterpretationAnalysis setExomiserVersion(String exomiserVersion) {
        this.exomiserVersion = exomiserVersion;
        return this;
    }
}
