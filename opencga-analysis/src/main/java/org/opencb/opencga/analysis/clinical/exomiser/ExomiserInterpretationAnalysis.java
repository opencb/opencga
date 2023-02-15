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
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.formats.pubmed.v233jaxb.B;
import org.opencb.biodata.formats.variant.io.VariantReader;
import org.opencb.biodata.models.clinical.ClinicalAcmg;
import org.opencb.biodata.models.clinical.ClinicalAnalyst;
import org.opencb.biodata.models.clinical.ClinicalProperty;
import org.opencb.biodata.models.clinical.interpretation.*;
import org.opencb.biodata.models.clinical.interpretation.exceptions.InterpretationAnalysisException;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.VariantFileMetadata;
import org.opencb.biodata.models.variant.avro.FileEntry;
import org.opencb.biodata.models.variant.avro.GeneCancerAssociation;
import org.opencb.biodata.models.variant.metadata.VariantStudyMetadata;
import org.opencb.biodata.tools.clinical.ClinicalVariantCreator;
import org.opencb.biodata.tools.clinical.DefaultClinicalVariantCreator;
import org.opencb.biodata.tools.pedigree.ModeOfInheritance;
import org.opencb.biodata.tools.variant.VariantNormalizer;
import org.opencb.biodata.tools.variant.VariantVcfHtsjdkReader;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
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
import org.opencb.opencga.core.response.VariantQueryResult;
import org.opencb.opencga.core.tools.annotations.Tool;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;

import java.io.*;
import java.nio.file.Path;
import java.util.*;

import static org.opencb.biodata.models.clinical.interpretation.VariantClassification.calculateAcmgClassification;
import static org.opencb.biodata.models.clinical.interpretation.VariantClassification.computeClinicalSignificance;
import static org.opencb.opencga.core.tools.OpenCgaToolExecutor.EXECUTOR_ID;

@Tool(id = ExomiserInterpretationAnalysis.ID, resource = Enums.Resource.CLINICAL)
public class ExomiserInterpretationAnalysis extends InterpretationAnalysis {

    public final static String ID = "interpretation-exomiser";
    public static final String DESCRIPTION = "Run exomiser interpretation analysis";

    private String studyId;
    private String clinicalAnalysisId;
    private String sampleId;

    private ClinicalAnalysis clinicalAnalysis;
    private Individual individual;

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
        } catch (
                CatalogException e) {
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

        // Check primary
//        checkPrimaryInterpretation(clinicalAnalysis);
//
//        // Check interpretation method
//        checkInterpretationMethod(getInterpretationMethod(ID).getName(), clinicalAnalysis);

        // Update executor params with OpenCGA home and session ID
        setUpStorageEngineExecutor(studyId);
    }

    @Override
    protected void run() throws ToolException {
        step(() -> {

            executorParams.put(EXECUTOR_ID, ExomiserWrapperAnalysisExecutor.ID);
            getToolExecutor(ExomiserWrapperAnalysisExecutor.class)
                    .setStudyId(studyId)
                    .setSampleId(sampleId)
                    .execute();

            saveInterpretation(studyId, clinicalAnalysis);
        });
    }

    protected void saveInterpretation(String studyId, ClinicalAnalysis clinicalAnalysis) throws ToolException, StorageEngineException,
            InterpretationAnalysisException, CatalogException, IOException {
        // Interpretation method
        InterpretationMethod method = new InterpretationMethod(getId(), GitRepositoryState.get().getBuildVersion(),
                GitRepositoryState.get().getCommitId(), Collections.singletonList(
                new Software()
                        .setName("Exomiser")
                        .setRepository("Docker: " + ExomiserWrapperAnalysisExecutor.DOCKER_IMAGE_NAME)
                        .setVersion(ExomiserWrapperAnalysisExecutor.DOCKER_IMAGE_VERSION)));

        // Analyst
        ClinicalAnalyst analyst = clinicalInterpretationManager.getAnalyst(token);

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

    private List<ClinicalVariant> getPrimaryFindings() throws InterpretationAnalysisException, IOException, StorageEngineException,
            CatalogException, ToolException {

        List<ClinicalVariant> primaryFindings = new ArrayList<>();

        // Prepare variant query
        List<String> sampleIds = new ArrayList<>();
        if (clinicalAnalysis.getFamily() != null && CollectionUtils.isNotEmpty(clinicalAnalysis.getFamily().getMembers())) {
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

        Map<String, List<String[]>> variantTsvMap = new HashMap<>();
        Map<String, ObjectMap> variantExomiserAttrMaps = new HashMap<>();

        File exomiserJsonFile = getOutDir().resolve("exomiser_output.json").toFile();
        Map<String, Set<String>> variantTranscriptMap = getTranscriptsFromJsonFile(exomiserJsonFile);

        File variantTsvFile = getOutDir().resolve("exomiser_output.variants.tsv").toFile();
        if (variantTsvFile.exists()) {
            try (BufferedReader br = new BufferedLineReader(new FileInputStream(variantTsvFile))) {
                // Read header line
                br.readLine();
                String line = br.readLine();
                while (line != null) {
                    String[] fields = line.split("\t");
                    Variant variant = new Variant(fields[14], Integer.parseInt(fields[15]), Integer.parseInt(fields[16]), fields[17],
                            fields[18]);
                    String variantId = variant.toStringSimple();
                    if (!variantTsvMap.containsKey(variantId)) {
                        variantTsvMap.put(variantId, new ArrayList<>());
                    }
                    variantTsvMap.get(variantId).add(fields);

                    // Next line
                    line = br.readLine();
                }
            }
        }

        List<String> variantIds = new ArrayList<>(variantTsvMap.keySet());
        if (CollectionUtils.isNotEmpty(variantIds)) {
            query.put(VariantQueryParam.ID.key(), StringUtils.join(variantIds, ","));
            logger.info("Query (including all family samples): {}", query.toJson());
            VariantQueryResult<Variant> variantResults = getVariantStorageManager().get(query, QueryOptions.empty(), getToken());

            if (variantResults != null && CollectionUtils.isNotEmpty(variantResults.getResults())) {
                // Exomiser clinical variant creator
                ExomiserClinicalVariantCreator clinicalVariantCreator = new ExomiserClinicalVariantCreator();

                // Convert variants to clinical variants
                for (Variant variant : variantResults.getResults()) {
                    ClinicalVariant clinicalVariant = clinicalVariantCreator.create(variant);
                    List<String> transcripts = new ArrayList<>(variantTranscriptMap.get(variant.toStringSimple()));
                    for (String[] fields : variantTsvMap.get(variant.toStringSimple())) {
                        ClinicalProperty.ModeOfInheritance moi = getModeOfInheritance(fields[4]);
                        Map<String, Object> attributes = getAttributesFromTsv(fields);

                        clinicalVariantCreator.addClinicalVariantEvidences(clinicalVariant, transcripts, moi, attributes);
                    }
                    primaryFindings.add(clinicalVariant);
                }
            }
        }

        return primaryFindings;
    }

    private Map<String, Set<String>> getTranscriptsFromJsonFile(File file) throws IOException {
        Map<String, Set<String>> results = new HashMap<>();
        String content = org.apache.commons.io.FileUtils.readFileToString(file);
        Object obj = JacksonUtils.getDefaultNonNullObjectMapper().readValue(content, Object.class);
        List<Object> list = (ArrayList<Object>) obj;
        System.out.println(list.size());
        for (Object item : list) {
            List<Map> geneScores = (ArrayList) ((Map) item).get("geneScores");
            for (Map geneScore : geneScores) {
                if (geneScore.containsKey("contributingVariants")) {
                    List<Map> contributingVariants = (ArrayList) geneScore.get("contributingVariants");
                    for (Map contributingVariant : contributingVariants) {
                        String variantId = contributingVariant.get("contigName") + ":" + contributingVariant.get("start") + ":"
                                + contributingVariant.get("ref") + ":" + contributingVariant.get("alt");
                        if (!results.containsKey(variantId)) {
                            results.put(variantId, new HashSet<>());
                        }
                        List<Map> transcriptAnnotations = (ArrayList) contributingVariant.get("transcriptAnnotations");
                        for (Map transcriptAnnotation : transcriptAnnotations) {
                            results.get(variantId).add((String) transcriptAnnotation.get("accession"));
                        }
                    }
                }
            }
        }
        return results;
    }

    private ObjectMap getAttributesFromTsv(String[] fields) {
        ObjectMap attributes = new ObjectMap();

        // 0    1   2           3               4   5       6                               7
        // RANK	ID	GENE_SYMBOL	ENTREZ_GENE_ID	MOI	P-VALUE	EXOMISER_GENE_COMBINED_SCORE	EXOMISER_GENE_PHENO_SCORE
        attributes.put("RANK", fields[0]);
        attributes.put("P-VALUE", fields[5]);
        attributes.put("EXOMISER_GENE_COMBINED_SCORE", fields[6]);
        attributes.put("EXOMISER_GENE_PHENO_SCORE", fields[7]);

        // 8                            9                       10                      11                  12      13      14      15
        // EXOMISER_GENE_VARIANT_SCORE	EXOMISER_VARIANT_SCORE	CONTRIBUTING_VARIANT	WHITELIST_VARIANT	VCF_ID	RS_ID	CONTIG	START
        attributes.put("EXOMISER_GENE_VARIANT_SCORE", fields[8]);
        attributes.put("EXOMISER_VARIANT_SCORE", fields[9]);

        // 16   17  18  19              20      21      22          23                  24      25
        // END	REF	ALT	CHANGE_LENGTH	QUAL	FILTER	GENOTYPE	FUNCTIONAL_CLASS	HGVS	EXOMISER_ACMG_CLASSIFICATION
        attributes.put("EXOMISER_ACMG_CLASSIFICATION", fields[25]);

        // 26                       27                          28                          29
        // EXOMISER_ACMG_EVIDENCE	EXOMISER_ACMG_DISEASE_ID	EXOMISER_ACMG_DISEASE_NAME	CLINVAR_ALLELE_ID
        attributes.put("EXOMISER_ACMG_EVIDENCE", fields[26]);
        attributes.put("EXOMISER_ACMG_DISEASE_ID", fields[27]);
        attributes.put("EXOMISER_ACMG_DISEASE_NAME", fields[28]);

        // 30                               31                  32                      33
        // CLINVAR_PRIMARY_INTERPRETATION	CLINVAR_STAR_RATING	GENE_CONSTRAINT_LOEUF	GENE_CONSTRAINT_LOEUF_LOWER
        // 34                           35              36          37          38              39          40
        // GENE_CONSTRAINT_LOEUF_UPPER	MAX_FREQ_SOURCE	MAX_FREQ	ALL_FREQ	MAX_PATH_SOURCE	MAX_PATH	ALL_PATH

        return attributes;
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
}
