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

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.formats.variant.io.VariantReader;
import org.opencb.biodata.models.clinical.ClinicalAnalyst;
import org.opencb.biodata.models.clinical.ClinicalProperty;
import org.opencb.biodata.models.clinical.interpretation.ClinicalVariant;
import org.opencb.biodata.models.clinical.interpretation.ClinicalVariantEvidence;
import org.opencb.biodata.models.clinical.interpretation.InterpretationMethod;
import org.opencb.biodata.models.clinical.interpretation.Software;
import org.opencb.biodata.models.clinical.interpretation.exceptions.InterpretationAnalysisException;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.VariantFileMetadata;
import org.opencb.biodata.models.variant.avro.FileEntry;
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
import org.opencb.opencga.analysis.wrappers.exomiser.ExomiserWrapperAnalysisExecutor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.utils.ParamUtils;
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

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.*;

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

    protected void saveInterpretation(String studyId, ClinicalAnalysis clinicalAnalysis) throws ToolException {
        // Interpretation method
        InterpretationMethod method = new InterpretationMethod(getId(), "", "",
                Collections.singletonList(new Software().setName(getId())));

        // Analyst
        ClinicalAnalyst analyst = clinicalInterpretationManager.getAnalyst(token);

        List<ClinicalVariant> primaryFindings;
        try {
            primaryFindings = getPrimaryFindings();
            for (ClinicalVariant primaryFinding : primaryFindings) {
                for (ClinicalVariantEvidence evidence : primaryFinding.getEvidences()) {
                    evidence.setInterpretationMethodName(method.getName());
                }
            }
        } catch (InterpretationAnalysisException | IOException | StorageEngineException | CatalogException e) {
            throw new ToolException("Error retrieving primary findings", e);
        }

        org.opencb.biodata.models.clinical.interpretation.Interpretation interpretation = new Interpretation()
                //.setId(getId() + "." + TimeUtils.getTimeMillis())
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
            CatalogException {
        Map<String, ClinicalVariant> cvMap = new HashMap<>();

        VariantNormalizer normalizer = new VariantNormalizer();

        // Prepare variant query
        List<String> sampleIds = new ArrayList<>();
        sampleIds.add(sampleId);
        if (clinicalAnalysis.getProband().getFather() != null) {
            sampleIds.add(clinicalAnalysis.getProband().getFather().getId());
        }
        if (clinicalAnalysis.getProband().getMother() != null) {
            sampleIds.add(clinicalAnalysis.getProband().getMother().getId());
        }
        Query query = new Query(VariantQueryParam.STUDY.key(), getStudyId())
                .append(VariantQueryParam.INCLUDE_SAMPLE_ID.key(), true)
                .append(VariantQueryParam.INCLUDE_STUDY.key(), "all")
                .append(VariantQueryParam.SAMPLE.key(), StringUtils.join(sampleIds, ","));

        // Parse all Exomiser output files (VCF)
        for (File file : getOutDir().toFile().listFiles()) {
            String filename = file.getName();
            if (filename.startsWith("exomiser_output") && filename.endsWith("vcf")) {
                Map<String, ObjectMap> variantExomiserAttrMaps = new HashMap<>();

                // Read variants from VCF file
                VariantStudyMetadata variantStudyMetadata = new VariantFileMetadata(filename, "").toVariantStudyMetadata(studyId);
                VariantReader reader = new VariantVcfHtsjdkReader(file.toPath(), variantStudyMetadata, normalizer);
                List<String> variantIds = new ArrayList<>();
                Iterator<Variant> iterator = reader.iterator();
                while (iterator.hasNext()) {
                    Variant variant = iterator.next();
                    String variantId = StringUtils.isEmpty(variant.getId()) ? variant.toStringSimple() : variant.getId();

                    // Get Exomiser attributes and put it into a map to further processing
                    variantExomiserAttrMaps.put(variantId, getExomiserAttributes(variant));

                    // Add variant into the list
                    variantIds.add(variantId);
                }
                if (CollectionUtils.isNotEmpty(variantIds)) {
                    query.put(VariantQueryParam.ID.key(), StringUtils.join(variantIds, ","));
                    logger.info("Query (including father/mother samples): {}", query.toJson());
                    VariantQueryResult<Variant> variantResults = getVariantStorageManager().get(query, QueryOptions.empty(), getToken());

                    if (variantResults != null && CollectionUtils.isNotEmpty(variantResults.getResults())) {
                        // Convert variants to clinical variants
                        ClinicalVariantCreator clinicalVariantCreator = getClinicalVariantCreator(filename);
                        List<ClinicalVariant> clinicalVariants = clinicalVariantCreator.create(variantResults.getResults());
                        if (CollectionUtils.isNotEmpty(clinicalVariants)) {
                            for (ClinicalVariant cv : clinicalVariants) {
                                String variantId = StringUtils.isEmpty(cv.getId()) ? cv.toStringSimple() : cv.getId();

                                // Add Exomiser attributes to the clinical variant evidences
                                if (variantExomiserAttrMaps.containsKey(variantId) && CollectionUtils.isNotEmpty(cv.getEvidences())) {
                                    for (ClinicalVariantEvidence cve : cv.getEvidences()) {
                                        if (cve.getAttributes() == null) {
                                            // Initialize map before populating
                                            cve.setAttributes(new HashMap<>());
                                        }
                                        cve.getAttributes().putAll(variantExomiserAttrMaps.get(cv.getId()));
                                    }
                                }

                                // Add clinical variant to the map (to group by mode of inheritance)
                                if (cvMap.containsKey(variantId)) {
                                    cvMap.get(variantId).getEvidences().addAll(cv.getEvidences());
                                } else {
                                    cvMap.put(variantId, cv);
                                }
                            }
                        }
                    }
                }

                // close
                reader.close();
            }
        }

        return new ArrayList<>(cvMap.values());
    }

    private ObjectMap getExomiserAttributes(Variant variant) {
        ObjectMap attributes = new ObjectMap();
        if (variant != null && CollectionUtils.isNotEmpty(variant.getStudies())) {
            StudyEntry studyEntry = variant.getStudies().get(0);
            if (CollectionUtils.isNotEmpty(studyEntry.getFiles())) {
                FileEntry fileEntry = studyEntry.getFiles().get(0);
                if (MapUtils.isNotEmpty(fileEntry.getData())) {
                    List<String> keys = Arrays.asList("ExGeneSCombi", "ExGeneSPheno", "ExGeneSVar", "ExVarScore");
                    for (String key : keys) {
                        if (fileEntry.getData().containsKey(key)) {
                            try {
                                attributes.put(key, Double.parseDouble(fileEntry.getData().get(key)));
                            } catch (NumberFormatException e) {
                                attributes.put(key, fileEntry.getData().get(key));
                            }
                        }
                    }
                }
            }
        }
        return attributes;
    }

    private ClinicalVariantCreator getClinicalVariantCreator(String filename) {
        ClinicalProperty.ModeOfInheritance moi;
        if (filename.endsWith("AD.vcf")) {
            moi = ClinicalProperty.ModeOfInheritance.AUTOSOMAL_DOMINANT;
        } else if (filename.endsWith("AR.vcf")) {
            moi = ClinicalProperty.ModeOfInheritance.AUTOSOMAL_RECESSIVE;
        } else if (filename.endsWith("XD.vcf")) {
            moi = ClinicalProperty.ModeOfInheritance.X_LINKED_DOMINANT;
        } else if (filename.endsWith("XR.vcf")) {
            moi = ClinicalProperty.ModeOfInheritance.X_LINKED_RECESSIVE;
        } else if (filename.endsWith("MT.vcf")) {
            moi = ClinicalProperty.ModeOfInheritance.MITOCHONDRIAL;
        } else {
            moi = ClinicalProperty.ModeOfInheritance.UNKNOWN;
        }

        return new DefaultClinicalVariantCreator(null, Collections.singletonList(moi), ClinicalProperty.Penetrance.COMPLETE,
                new ArrayList<>(), new ArrayList<>(ModeOfInheritance.proteinCoding), new ArrayList<>(ModeOfInheritance.extendedLof), true);
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
