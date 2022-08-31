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
import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.formats.variant.io.VariantReader;
import org.opencb.biodata.models.clinical.ClinicalAnalyst;
import org.opencb.biodata.models.clinical.ClinicalProperty;
import org.opencb.biodata.models.clinical.interpretation.ClinicalVariant;
import org.opencb.biodata.models.clinical.interpretation.ClinicalVariantEvidence;
import org.opencb.biodata.models.clinical.interpretation.InterpretationMethod;
import org.opencb.biodata.models.clinical.interpretation.Software;
import org.opencb.biodata.models.clinical.interpretation.exceptions.InterpretationAnalysisException;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.VariantFileMetadata;
import org.opencb.biodata.models.variant.metadata.VariantStudyMetadata;
import org.opencb.biodata.tools.clinical.ClinicalVariantCreator;
import org.opencb.biodata.tools.clinical.DefaultClinicalVariantCreator;
import org.opencb.biodata.tools.pedigree.ModeOfInheritance;
import org.opencb.biodata.tools.variant.VariantNormalizer;
import org.opencb.biodata.tools.variant.VariantVcfHtsjdkReader;
import org.opencb.cellbase.client.rest.CellBaseClient;
import org.opencb.cellbase.client.rest.VariantClient;
import org.opencb.cellbase.core.result.CellBaseDataResponse;
import org.opencb.cellbase.core.result.CellBaseDataResult;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.clinical.InterpretationAnalysis;
import org.opencb.opencga.analysis.tools.OpenCgaTool;
import org.opencb.opencga.analysis.wrappers.exomiser.ExomiserWrapperAnalysisExecutor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.core.common.JacksonUtils;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.clinical.ClinicalAnalysis;
import org.opencb.opencga.core.models.clinical.Interpretation;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.opencb.opencga.core.tools.annotations.Tool;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.slf4j.LoggerFactory;

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
        CellBaseClient cellBaseClient = getVariantStorageManager().getCellBaseUtils(studyId, token).getCellBaseClient();
        logger.info("{}: Annotating with Cellbase REST: {}", ID, cellBaseClient.getClientConfiguration());

        VariantClient variantClient = cellBaseClient.getVariantClient();

        for (File file : getOutDir().toFile().listFiles()) {
            String filename = file.getName();
            if (filename.startsWith("exomiser_output") && filename.endsWith("vcf")) {
                // Read variants from VCF file
                VariantStudyMetadata variantStudyMetadata = new VariantFileMetadata(filename, "").toVariantStudyMetadata(studyId);
                VariantReader reader = new VariantVcfHtsjdkReader(file.toPath(), variantStudyMetadata, normalizer);
                List<Variant> variants = new ArrayList<>();
                Iterator<Variant> iterator = reader.iterator();
                while (iterator.hasNext()) {
                    Variant variant = iterator.next();
                    if (StringUtils.isEmpty(variant.getId())) {
                        variant.setId(variant.toStringSimple());
                    }
                    variants.add(variant);
                }
                if (CollectionUtils.isNotEmpty(variants)) {
                    // Annotate variants
                    List<Variant> annotatedVariants = new ArrayList<>();
                    CellBaseDataResponse<Variant> cellBaseResponse = variantClient.annotate(variants, new QueryOptions("exclude",
                            "expression"), true);
                    if (cellBaseResponse != null && CollectionUtils.isNotEmpty(cellBaseResponse.getResponses())) {
                        for (CellBaseDataResult<Variant> response : cellBaseResponse.getResponses()) {
                            annotatedVariants.addAll(response.getResults());
                        }

                        // Convert annotated variant to clinical variants
                        ClinicalVariantCreator clinicalVariantCreator = getClinicalVariantCreator(filename);
                        List<ClinicalVariant> clinicalVariants = clinicalVariantCreator.create(annotatedVariants);
                        if (CollectionUtils.isNotEmpty(clinicalVariants)) {
                            for (ClinicalVariant clinicalVariant : clinicalVariants) {
                                if (!cvMap.containsKey(clinicalVariant.getId())) {
                                    cvMap.put(clinicalVariant.getId(), clinicalVariant);
                                } else {
                                    cvMap.get(clinicalVariant.getId()).getEvidences().addAll(clinicalVariant.getEvidences());
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

        return new DefaultClinicalVariantCreator(null, null, null,
                Collections.singletonList(moi), ClinicalProperty.Penetrance.COMPLETE, new ArrayList<>(),
                new ArrayList<>(ModeOfInheritance.proteinCoding), new ArrayList<>(ModeOfInheritance.extendedLof), true);
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
