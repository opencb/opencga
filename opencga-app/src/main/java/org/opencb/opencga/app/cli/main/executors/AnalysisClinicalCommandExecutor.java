package org.opencb.opencga.app.cli.main.executors;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.opencb.biodata.models.clinical.ClinicalDiscussion;
import org.opencb.biodata.models.clinical.interpretation.ClinicalVariant;
import org.opencb.biodata.models.clinical.interpretation.InterpretationMethod;
import org.opencb.commons.datastore.core.FacetField;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.utils.PrintUtils;
import org.opencb.opencga.app.cli.main.*;
import org.opencb.opencga.app.cli.main.executors.OpencgaCommandExecutor;
import org.opencb.opencga.app.cli.main.options.AnalysisClinicalCommandOptions;
import org.opencb.opencga.catalog.exceptions.CatalogAuthenticationException;
import org.opencb.opencga.catalog.utils.ParamUtils.AclAction;
import org.opencb.opencga.catalog.utils.ParamUtils.AddRemoveReplaceAction;
import org.opencb.opencga.catalog.utils.ParamUtils.BasicUpdateAction;
import org.opencb.opencga.catalog.utils.ParamUtils.CompleteUpdateAction;
import org.opencb.opencga.catalog.utils.ParamUtils.SaveInterpretationAs;
import org.opencb.opencga.catalog.utils.ParamUtils.UpdateAction;
import org.opencb.opencga.core.common.JacksonUtils;
import org.opencb.opencga.core.exceptions.ClientException;
import org.opencb.opencga.core.models.analysis.knockout.KnockoutByGeneSummary;
import org.opencb.opencga.core.models.analysis.knockout.KnockoutByIndividual;
import org.opencb.opencga.core.models.analysis.knockout.KnockoutByIndividualSummary;
import org.opencb.opencga.core.models.analysis.knockout.KnockoutByVariant;
import org.opencb.opencga.core.models.analysis.knockout.KnockoutByVariantSummary;
import org.opencb.opencga.core.models.analysis.knockout.RgaKnockoutByGene;
import org.opencb.opencga.core.models.clinical.ClinicalAnalysis;
import org.opencb.opencga.core.models.clinical.ClinicalAnalysisAclEntryList;
import org.opencb.opencga.core.models.clinical.ClinicalAnalysisAclUpdateParams;
import org.opencb.opencga.core.models.clinical.ClinicalAnalysisCreateParams;
import org.opencb.opencga.core.models.clinical.ClinicalAnalysisLoadParams;
import org.opencb.opencga.core.models.clinical.ClinicalAnalysisQualityControl;
import org.opencb.opencga.core.models.clinical.ClinicalAnalysisQualityControlUpdateParam;
import org.opencb.opencga.core.models.clinical.ClinicalAnalysisUpdateParams;
import org.opencb.opencga.core.models.clinical.ClinicalAnalystParam;
import org.opencb.opencga.core.models.clinical.ClinicalReport;
import org.opencb.opencga.core.models.clinical.ClinicalRequest;
import org.opencb.opencga.core.models.clinical.ClinicalResponsible;
import org.opencb.opencga.core.models.clinical.DisorderReferenceParam;
import org.opencb.opencga.core.models.clinical.ExomiserInterpretationAnalysisParams;
import org.opencb.opencga.core.models.clinical.FamilyParam;
import org.opencb.opencga.core.models.clinical.Interpretation;
import org.opencb.opencga.core.models.clinical.InterpretationCreateParams;
import org.opencb.opencga.core.models.clinical.InterpretationUpdateParams;
import org.opencb.opencga.core.models.clinical.PharmacogenomicsAlleleTyperParams;
import org.opencb.opencga.core.models.clinical.PharmacogenomicsAlleleTyperToolParams;
import org.opencb.opencga.core.models.clinical.PharmacogenomicsAnnotationAnalysisToolParams;
import org.opencb.opencga.core.models.clinical.PriorityParam;
import org.opencb.opencga.core.models.clinical.ProbandParam;
import org.opencb.opencga.core.models.clinical.RgaAnalysisParams;
import org.opencb.opencga.core.models.clinical.interpretation.RdInterpretationAnalysisToolParams;
import org.opencb.opencga.core.models.clinical.pharmacogenomics.AlleleTyperResult;
import org.opencb.opencga.core.models.clinical.pipeline.affy.AffyClinicalPipelineParams;
import org.opencb.opencga.core.models.clinical.pipeline.affy.AffyClinicalPipelineWrapperParams;
import org.opencb.opencga.core.models.clinical.pipeline.affy.AffyPipelineConfig;
import org.opencb.opencga.core.models.clinical.pipeline.genomics.GenomicsClinicalPipelineParams;
import org.opencb.opencga.core.models.clinical.pipeline.genomics.GenomicsClinicalPipelineWrapperParams;
import org.opencb.opencga.core.models.clinical.pipeline.genomics.GenomicsPipelineConfig;
import org.opencb.opencga.core.models.clinical.pipeline.prepare.PrepareClinicalPipelineParams;
import org.opencb.opencga.core.models.clinical.pipeline.prepare.PrepareClinicalPipelineWrapperParams;
import org.opencb.opencga.core.models.common.StatusParam;
import org.opencb.opencga.core.models.common.TsvAnnotationParams;
import org.opencb.opencga.core.models.job.Job;
import org.opencb.opencga.core.models.operations.variant.VariantIndexParams;
import org.opencb.opencga.core.models.sample.Sample;
import org.opencb.opencga.core.models.study.configuration.ClinicalAnalysisStudyConfiguration;
import org.opencb.opencga.core.models.study.configuration.ClinicalConsentAnnotationParam;
import org.opencb.opencga.core.models.study.configuration.ClinicalConsentConfiguration;
import org.opencb.opencga.core.models.study.configuration.ClinicalReportConfiguration;
import org.opencb.opencga.core.models.study.configuration.InterpretationStudyConfiguration;
import org.opencb.opencga.core.response.QueryType;
import org.opencb.opencga.core.response.RestResponse;


/*
* WARNING: AUTOGENERATED CODE
*
* This code was generated by a tool.
*
* Manual changes to this file may cause unexpected behavior in your application.
* Manual changes to this file will be overwritten if the code is regenerated.
*  
*/
/**
 * This class contains methods for the Analysis - Clinical command line.
 *    PATH: /{apiVersion}/analysis/clinical
 */
public class AnalysisClinicalCommandExecutor extends OpencgaCommandExecutor {

    public String categoryName = "clinical";
    public AnalysisClinicalCommandOptions analysisClinicalCommandOptions;

    public AnalysisClinicalCommandExecutor(AnalysisClinicalCommandOptions analysisClinicalCommandOptions) throws CatalogAuthenticationException {
        super(analysisClinicalCommandOptions.commonCommandOptions);
        this.analysisClinicalCommandOptions = analysisClinicalCommandOptions;
    }

    @Override
    public void execute() throws Exception {

        logger.debug("Executing Analysis - Clinical command line");

        String subCommandString = getParsedSubCommand(analysisClinicalCommandOptions.jCommander);

        RestResponse queryResponse = null;

        switch (subCommandString) {
            case "acl-update":
                queryResponse = updateAcl();
                break;
            case "aggregationstats":
                queryResponse = aggregationStats();
                break;
            case "annotation-sets-load":
                queryResponse = loadAnnotationSets();
                break;
            case "clinical-configuration-update":
                queryResponse = updateClinicalConfiguration();
                break;
            case "create":
                queryResponse = create();
                break;
            case "distinct":
                queryResponse = distinct();
                break;
            case "interpretation-aggregation-stats":
                queryResponse = aggregationStatsInterpretation();
                break;
            case "interpretation-distinct":
                queryResponse = distinctInterpretation();
                break;
            case "interpretation-search":
                queryResponse = searchInterpretation();
                break;
            case "interpretation-info":
                queryResponse = infoInterpretation();
                break;
            case "interpreter":
                queryResponse = interpreter();
                break;
            case "interpreter-exomiser-run":
                queryResponse = runInterpreterExomiser();
                break;
            case "interpreter-rd":
                queryResponse = rdInterpreter();
                break;
            case "interpreter-rd-run":
                queryResponse = runInterpreterRd();
                break;
            case "interpreter-run":
                queryResponse = runInterpreter();
                break;
            case "load":
                queryResponse = load();
                break;
            case "pharmacogenomics-allele-typer":
                queryResponse = alleleTyperPharmacogenomics();
                break;
            case "pharmacogenomics-allele-typer-run":
                queryResponse = runPharmacogenomicsAlleleTyper();
                break;
            case "pharmacogenomics-annotation-run":
                queryResponse = runPharmacogenomicsAnnotation();
                break;
            case "pipeline-affy-run":
                queryResponse = runPipelineAffy();
                break;
            case "pipeline-genomics-run":
                queryResponse = runPipelineGenomics();
                break;
            case "pipeline-prepare-run":
                queryResponse = runPipelinePrepare();
                break;
            case "rga-aggregation-stats":
                queryResponse = aggregationStatsRga();
                break;
            case "rga-gene-query":
                queryResponse = queryRgaGene();
                break;
            case "rga-gene-summary":
                queryResponse = summaryRgaGene();
                break;
            case "rga-index-run":
                queryResponse = runRgaIndex();
                break;
            case "rga-individual-query":
                queryResponse = queryRgaIndividual();
                break;
            case "rga-individual-summary":
                queryResponse = summaryRgaIndividual();
                break;
            case "rga-variant-query":
                queryResponse = queryRgaVariant();
                break;
            case "rga-variant-summary":
                queryResponse = summaryRgaVariant();
                break;
            case "search":
                queryResponse = search();
                break;
            case "variant-query":
                queryResponse = queryVariant();
                break;
            case "acl":
                queryResponse = acl();
                break;
            case "delete":
                queryResponse = delete();
                break;
            case "update":
                queryResponse = update();
                break;
            case "annotation-sets-annotations-update":
                queryResponse = updateAnnotationSetsAnnotations();
                break;
            case "info":
                queryResponse = info();
                break;
            case "interpretation-create":
                queryResponse = createInterpretation();
                break;
            case "interpretation-clear":
                queryResponse = clearInterpretation();
                break;
            case "interpretation-delete":
                queryResponse = deleteInterpretation();
                break;
            case "interpretation-revert":
                queryResponse = revertInterpretation();
                break;
            case "interpretation-update":
                queryResponse = updateInterpretation();
                break;
            case "report-update":
                queryResponse = updateReport();
                break;
            default:
                logger.error("Subcommand not valid");
                break;
        }

        createOutput(queryResponse);

    }

    private RestResponse<ClinicalAnalysisAclEntryList> updateAcl() throws Exception {
        logger.debug("Executing updateAcl in Analysis - Clinical command line");

        AnalysisClinicalCommandOptions.UpdateAclCommandOptions commandOptions = analysisClinicalCommandOptions.updateAclCommandOptions;

        ObjectMap queryParams = new ObjectMap();
        queryParams.putIfNotEmpty("study", commandOptions.study);
        queryParams.putIfNotNull("propagate", commandOptions.propagate);
        if (queryParams.get("study") == null && OpencgaMain.isShellMode()) {
            queryParams.putIfNotEmpty("study", sessionManager.getSession().getCurrentStudy());
        }


        ClinicalAnalysisAclUpdateParams clinicalAnalysisAclUpdateParams = null;
        if (commandOptions.jsonDataModel) {
            RestResponse<ClinicalAnalysisAclEntryList> res = new RestResponse<>();
            res.setType(QueryType.VOID);
            PrintUtils.println(getObjectAsJSON(categoryName,"/{apiVersion}/analysis/clinical/acl/{members}/update"));
            return res;
        } else if (commandOptions.jsonFile != null) {
            clinicalAnalysisAclUpdateParams = JacksonUtils.getDefaultObjectMapper()
                    .readValue(new java.io.File(commandOptions.jsonFile), ClinicalAnalysisAclUpdateParams.class);
        } else {
            ObjectMap beanParams = new ObjectMap();
            putNestedIfNotEmpty(beanParams, "permissions", commandOptions.permissions, true);
            putNestedIfNotEmpty(beanParams, "clinicalAnalysis", commandOptions.clinicalAnalysis, true);

            clinicalAnalysisAclUpdateParams = JacksonUtils.getDefaultObjectMapper().copy()
                    .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, true)
                    .readValue(beanParams.toJson(), ClinicalAnalysisAclUpdateParams.class);
        }
        return openCGAClient.getClinicalAnalysisClient().updateAcl(commandOptions.members, commandOptions.action, clinicalAnalysisAclUpdateParams, queryParams);
    }

    private RestResponse<FacetField> aggregationStats() throws Exception {
        logger.debug("Executing aggregationStats in Analysis - Clinical command line");

        AnalysisClinicalCommandOptions.AggregationStatsCommandOptions commandOptions = analysisClinicalCommandOptions.aggregationStatsCommandOptions;

        ObjectMap queryParams = new ObjectMap();
        queryParams.putIfNotEmpty("study", commandOptions.study);
        queryParams.putIfNotEmpty("id", commandOptions.id);
        queryParams.putIfNotEmpty("uuid", commandOptions.uuid);
        queryParams.putIfNotEmpty("type", commandOptions.type);
        queryParams.putIfNotEmpty("disorder", commandOptions.disorder);
        queryParams.putIfNotEmpty("files", commandOptions.files);
        queryParams.putIfNotEmpty("sample", commandOptions.sample);
        queryParams.putIfNotEmpty("individual", commandOptions.individual);
        queryParams.putIfNotEmpty("proband", commandOptions.proband);
        queryParams.putIfNotEmpty("probandSamples", commandOptions.probandSamples);
        queryParams.putIfNotEmpty("family", commandOptions.family);
        queryParams.putIfNotEmpty("familyMembers", commandOptions.familyMembers);
        queryParams.putIfNotEmpty("familyMemberSamples", commandOptions.familyMemberSamples);
        queryParams.putIfNotEmpty("panels", commandOptions.panels);
        queryParams.putIfNotNull("locked", commandOptions.locked);
        queryParams.putIfNotEmpty("analystId", commandOptions.analystId);
        queryParams.putIfNotEmpty("priority", commandOptions.priority);
        queryParams.putIfNotEmpty("flags", commandOptions.flags);
        queryParams.putIfNotEmpty("creationDate", commandOptions.creationDate);
        queryParams.putIfNotEmpty("modificationDate", commandOptions.modificationDate);
        queryParams.putIfNotEmpty("dueDate", commandOptions.dueDate);
        queryParams.putIfNotEmpty("qualityControlSummary", commandOptions.qualityControlSummary);
        queryParams.putIfNotEmpty("release", commandOptions.release);
        queryParams.putIfNotNull("snapshot", commandOptions.snapshot);
        queryParams.putIfNotEmpty("status", commandOptions.status);
        queryParams.putIfNotEmpty("internalStatus", commandOptions.internalStatus);
        queryParams.putIfNotEmpty("annotation", commandOptions.annotation);
        queryParams.putIfNotNull("deleted", commandOptions.deleted);
        queryParams.putIfNotEmpty("field", commandOptions.field);
        if (queryParams.get("study") == null && OpencgaMain.isShellMode()) {
            queryParams.putIfNotEmpty("study", sessionManager.getSession().getCurrentStudy());
        }

        return openCGAClient.getClinicalAnalysisClient().aggregationStats(queryParams);
    }

    private RestResponse<Job> loadAnnotationSets() throws Exception {
        logger.debug("Executing loadAnnotationSets in Analysis - Clinical command line");

        AnalysisClinicalCommandOptions.LoadAnnotationSetsCommandOptions commandOptions = analysisClinicalCommandOptions.loadAnnotationSetsCommandOptions;

        ObjectMap queryParams = new ObjectMap();
        queryParams.putIfNotEmpty("study", commandOptions.study);
        queryParams.putIfNotNull("parents", commandOptions.parents);
        queryParams.putIfNotEmpty("annotationSetId", commandOptions.annotationSetId);
        if (queryParams.get("study") == null && OpencgaMain.isShellMode()) {
            queryParams.putIfNotEmpty("study", sessionManager.getSession().getCurrentStudy());
        }


        TsvAnnotationParams tsvAnnotationParams = null;
        if (commandOptions.jsonDataModel) {
            RestResponse<Job> res = new RestResponse<>();
            res.setType(QueryType.VOID);
            PrintUtils.println(getObjectAsJSON(categoryName,"/{apiVersion}/analysis/clinical/annotationSets/load"));
            return res;
        } else if (commandOptions.jsonFile != null) {
            tsvAnnotationParams = JacksonUtils.getDefaultObjectMapper()
                    .readValue(new java.io.File(commandOptions.jsonFile), TsvAnnotationParams.class);
        } else {
            ObjectMap beanParams = new ObjectMap();
            putNestedIfNotEmpty(beanParams, "content", commandOptions.content, true);

            tsvAnnotationParams = JacksonUtils.getDefaultObjectMapper().copy()
                    .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, true)
                    .readValue(beanParams.toJson(), TsvAnnotationParams.class);
        }
        return openCGAClient.getClinicalAnalysisClient().loadAnnotationSets(commandOptions.variableSetId, commandOptions.path, tsvAnnotationParams, queryParams);
    }

    private RestResponse<ObjectMap> updateClinicalConfiguration() throws Exception {
        logger.debug("Executing updateClinicalConfiguration in Analysis - Clinical command line");

        AnalysisClinicalCommandOptions.UpdateClinicalConfigurationCommandOptions commandOptions = analysisClinicalCommandOptions.updateClinicalConfigurationCommandOptions;

        ObjectMap queryParams = new ObjectMap();
        queryParams.putIfNotEmpty("study", commandOptions.study);
        if (queryParams.get("study") == null && OpencgaMain.isShellMode()) {
            queryParams.putIfNotEmpty("study", sessionManager.getSession().getCurrentStudy());
        }


        ClinicalAnalysisStudyConfiguration clinicalAnalysisStudyConfiguration = null;
        if (commandOptions.jsonDataModel) {
            RestResponse<ObjectMap> res = new RestResponse<>();
            res.setType(QueryType.VOID);
            PrintUtils.println(getObjectAsJSON(categoryName,"/{apiVersion}/analysis/clinical/clinical/configuration/update"));
            return res;
        } else if (commandOptions.jsonFile != null) {
            clinicalAnalysisStudyConfiguration = JacksonUtils.getDefaultObjectMapper()
                    .readValue(new java.io.File(commandOptions.jsonFile), ClinicalAnalysisStudyConfiguration.class);
        } else {
            ObjectMap beanParams = new ObjectMap();
            putNestedMapIfNotEmpty(beanParams, "interpretation.defaultFilter", commandOptions.interpretationDefaultFilter, true);
            putNestedIfNotEmpty(beanParams, "report.title", commandOptions.reportTitle, true);
            putNestedIfNotEmpty(beanParams, "report.logo", commandOptions.reportLogo, true);
            putNestedMapIfNotEmpty(beanParams, "report.library", commandOptions.reportLibrary, true);

            clinicalAnalysisStudyConfiguration = JacksonUtils.getDefaultObjectMapper().copy()
                    .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, true)
                    .readValue(beanParams.toJson(), ClinicalAnalysisStudyConfiguration.class);
        }
        return openCGAClient.getClinicalAnalysisClient().updateClinicalConfiguration(clinicalAnalysisStudyConfiguration, queryParams);
    }

    private RestResponse<ClinicalAnalysis> create() throws Exception {
        logger.debug("Executing create in Analysis - Clinical command line");

        AnalysisClinicalCommandOptions.CreateCommandOptions commandOptions = analysisClinicalCommandOptions.createCommandOptions;

        ObjectMap queryParams = new ObjectMap();
        queryParams.putIfNotEmpty("include", commandOptions.include);
        queryParams.putIfNotEmpty("exclude", commandOptions.exclude);
        queryParams.putIfNotEmpty("study", commandOptions.study);
        queryParams.putIfNotNull("skipCreateDefaultInterpretation", commandOptions.skipCreateDefaultInterpretation);
        queryParams.putIfNotNull("includeResult", commandOptions.includeResult);
        if (queryParams.get("study") == null && OpencgaMain.isShellMode()) {
            queryParams.putIfNotEmpty("study", sessionManager.getSession().getCurrentStudy());
        }


        ClinicalAnalysisCreateParams clinicalAnalysisCreateParams = null;
        if (commandOptions.jsonDataModel) {
            RestResponse<ClinicalAnalysis> res = new RestResponse<>();
            res.setType(QueryType.VOID);
            PrintUtils.println(getObjectAsJSON(categoryName,"/{apiVersion}/analysis/clinical/create"));
            return res;
        } else if (commandOptions.jsonFile != null) {
            clinicalAnalysisCreateParams = JacksonUtils.getDefaultObjectMapper()
                    .readValue(new java.io.File(commandOptions.jsonFile), ClinicalAnalysisCreateParams.class);
        } else {
            ObjectMap beanParams = new ObjectMap();
            putNestedIfNotEmpty(beanParams, "id", commandOptions.id, true);
            putNestedIfNotEmpty(beanParams, "description", commandOptions.description, true);
            putNestedIfNotNull(beanParams, "type", commandOptions.type, true);
            putNestedIfNotEmpty(beanParams, "disorder.id", commandOptions.disorderId, true);
            putNestedIfNotEmpty(beanParams, "proband.id", commandOptions.probandId, true);
            putNestedIfNotEmpty(beanParams, "family.id", commandOptions.familyId, true);
            putNestedIfNotNull(beanParams, "panelLocked", commandOptions.panelLocked, true);
            putNestedIfNotEmpty(beanParams, "analyst.id", commandOptions.analystId, true);
            putNestedIfNotEmpty(beanParams, "report.overview", commandOptions.reportOverview, true);
            putNestedIfNotEmpty(beanParams, "report.recommendation", commandOptions.reportRecommendation, true);
            putNestedIfNotEmpty(beanParams, "report.methodology", commandOptions.reportMethodology, true);
            putNestedIfNotEmpty(beanParams, "report.limitations", commandOptions.reportLimitations, true);
            putNestedIfNotEmpty(beanParams, "report.experimentalProcedure", commandOptions.reportExperimentalProcedure, true);
            putNestedIfNotEmpty(beanParams, "report.date", commandOptions.reportDate, true);
            putNestedIfNotNull(beanParams, "report.images", commandOptions.reportImages, true);
            putNestedMapIfNotEmpty(beanParams, "report.attributes", commandOptions.reportAttributes, true);
            putNestedIfNotEmpty(beanParams, "request.id", commandOptions.requestId, true);
            putNestedIfNotEmpty(beanParams, "request.justification", commandOptions.requestJustification, true);
            putNestedIfNotEmpty(beanParams, "request.date", commandOptions.requestDate, true);
            putNestedMapIfNotEmpty(beanParams, "request.attributes", commandOptions.requestAttributes, true);
            putNestedIfNotEmpty(beanParams, "responsible.id", commandOptions.responsibleId, true);
            putNestedIfNotEmpty(beanParams, "responsible.name", commandOptions.responsibleName, true);
            putNestedIfNotEmpty(beanParams, "responsible.email", commandOptions.responsibleEmail, true);
            putNestedIfNotEmpty(beanParams, "responsible.organization", commandOptions.responsibleOrganization, true);
            putNestedIfNotEmpty(beanParams, "responsible.department", commandOptions.responsibleDepartment, true);
            putNestedIfNotEmpty(beanParams, "responsible.address", commandOptions.responsibleAddress, true);
            putNestedIfNotEmpty(beanParams, "responsible.city", commandOptions.responsibleCity, true);
            putNestedIfNotEmpty(beanParams, "responsible.postcode", commandOptions.responsiblePostcode, true);
            putNestedIfNotEmpty(beanParams, "interpretation.name", commandOptions.interpretationName, true);
            putNestedIfNotEmpty(beanParams, "interpretation.description", commandOptions.interpretationDescription, true);
            putNestedIfNotEmpty(beanParams, "interpretation.clinicalAnalysisId", commandOptions.interpretationClinicalAnalysisId, true);
            putNestedIfNotEmpty(beanParams, "interpretation.creationDate", commandOptions.interpretationCreationDate, true);
            putNestedIfNotEmpty(beanParams, "interpretation.modificationDate", commandOptions.interpretationModificationDate, true);
            putNestedIfNotNull(beanParams, "interpretation.locked", commandOptions.interpretationLocked, true);
            putNestedMapIfNotEmpty(beanParams, "interpretation.attributes", commandOptions.interpretationAttributes, true);
            putNestedIfNotNull(beanParams, "qualityControl.summary", commandOptions.qualityControlSummary, true);
            putNestedIfNotNull(beanParams, "qualityControl.comments", commandOptions.qualityControlComments, true);
            putNestedIfNotNull(beanParams, "qualityControl.files", commandOptions.qualityControlFiles, true);
            putNestedIfNotEmpty(beanParams, "creationDate", commandOptions.creationDate, true);
            putNestedIfNotEmpty(beanParams, "modificationDate", commandOptions.modificationDate, true);
            putNestedIfNotEmpty(beanParams, "dueDate", commandOptions.dueDate, true);
            putNestedIfNotEmpty(beanParams, "priority.id", commandOptions.priorityId, true);
            putNestedMapIfNotEmpty(beanParams, "attributes", commandOptions.attributes, true);
            putNestedIfNotEmpty(beanParams, "status.id", commandOptions.statusId, true);

            clinicalAnalysisCreateParams = JacksonUtils.getDefaultObjectMapper().copy()
                    .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, true)
                    .readValue(beanParams.toJson(), ClinicalAnalysisCreateParams.class);
        }
        return openCGAClient.getClinicalAnalysisClient().create(clinicalAnalysisCreateParams, queryParams);
    }

    private RestResponse<ObjectMap> distinct() throws Exception {
        logger.debug("Executing distinct in Analysis - Clinical command line");

        AnalysisClinicalCommandOptions.DistinctCommandOptions commandOptions = analysisClinicalCommandOptions.distinctCommandOptions;

        ObjectMap queryParams = new ObjectMap();
        queryParams.putIfNotEmpty("study", commandOptions.study);
        queryParams.putIfNotEmpty("id", commandOptions.id);
        queryParams.putIfNotEmpty("uuid", commandOptions.uuid);
        queryParams.putIfNotEmpty("type", commandOptions.type);
        queryParams.putIfNotEmpty("disorder", commandOptions.disorder);
        queryParams.putIfNotEmpty("files", commandOptions.files);
        queryParams.putIfNotEmpty("sample", commandOptions.sample);
        queryParams.putIfNotEmpty("individual", commandOptions.individual);
        queryParams.putIfNotEmpty("proband", commandOptions.proband);
        queryParams.putIfNotEmpty("probandSamples", commandOptions.probandSamples);
        queryParams.putIfNotEmpty("family", commandOptions.family);
        queryParams.putIfNotEmpty("familyMembers", commandOptions.familyMembers);
        queryParams.putIfNotEmpty("familyMemberSamples", commandOptions.familyMemberSamples);
        queryParams.putIfNotEmpty("panels", commandOptions.panels);
        queryParams.putIfNotNull("locked", commandOptions.locked);
        queryParams.putIfNotEmpty("analystId", commandOptions.analystId);
        queryParams.putIfNotEmpty("priority", commandOptions.priority);
        queryParams.putIfNotEmpty("flags", commandOptions.flags);
        queryParams.putIfNotEmpty("creationDate", commandOptions.creationDate);
        queryParams.putIfNotEmpty("modificationDate", commandOptions.modificationDate);
        queryParams.putIfNotEmpty("dueDate", commandOptions.dueDate);
        queryParams.putIfNotEmpty("qualityControlSummary", commandOptions.qualityControlSummary);
        queryParams.putIfNotEmpty("release", commandOptions.release);
        queryParams.putIfNotNull("snapshot", commandOptions.snapshot);
        queryParams.putIfNotEmpty("status", commandOptions.status);
        queryParams.putIfNotEmpty("internalStatus", commandOptions.internalStatus);
        queryParams.putIfNotEmpty("annotation", commandOptions.annotation);
        queryParams.putIfNotNull("deleted", commandOptions.deleted);
        if (queryParams.get("study") == null && OpencgaMain.isShellMode()) {
            queryParams.putIfNotEmpty("study", sessionManager.getSession().getCurrentStudy());
        }

        return openCGAClient.getClinicalAnalysisClient().distinct(commandOptions.field, queryParams);
    }

    private RestResponse<FacetField> aggregationStatsInterpretation() throws Exception {
        logger.debug("Executing aggregationStatsInterpretation in Analysis - Clinical command line");

        AnalysisClinicalCommandOptions.AggregationStatsInterpretationCommandOptions commandOptions = analysisClinicalCommandOptions.aggregationStatsInterpretationCommandOptions;

        ObjectMap queryParams = new ObjectMap();
        queryParams.putIfNotEmpty("study", commandOptions.study);
        queryParams.putIfNotEmpty("id", commandOptions.id);
        queryParams.putIfNotEmpty("uuid", commandOptions.uuid);
        queryParams.putIfNotEmpty("name", commandOptions.name);
        queryParams.putIfNotEmpty("clinicalAnalysisId", commandOptions.clinicalAnalysisId);
        queryParams.putIfNotEmpty("analystId", commandOptions.analystId);
        queryParams.putIfNotEmpty("methodName", commandOptions.methodName);
        queryParams.putIfNotEmpty("panels", commandOptions.panels);
        queryParams.putIfNotEmpty("primaryFindings", commandOptions.primaryFindings);
        queryParams.putIfNotEmpty("secondaryFindings", commandOptions.secondaryFindings);
        queryParams.putIfNotEmpty("creationDate", commandOptions.creationDate);
        queryParams.putIfNotEmpty("modificationDate", commandOptions.modificationDate);
        queryParams.putIfNotEmpty("status", commandOptions.status);
        queryParams.putIfNotEmpty("internalStatus", commandOptions.internalStatus);
        queryParams.putIfNotEmpty("release", commandOptions.release);
        queryParams.putIfNotEmpty("field", commandOptions.field);
        if (queryParams.get("study") == null && OpencgaMain.isShellMode()) {
            queryParams.putIfNotEmpty("study", sessionManager.getSession().getCurrentStudy());
        }

        return openCGAClient.getClinicalAnalysisClient().aggregationStatsInterpretation(queryParams);
    }

    private RestResponse<ObjectMap> distinctInterpretation() throws Exception {
        logger.debug("Executing distinctInterpretation in Analysis - Clinical command line");

        AnalysisClinicalCommandOptions.DistinctInterpretationCommandOptions commandOptions = analysisClinicalCommandOptions.distinctInterpretationCommandOptions;

        ObjectMap queryParams = new ObjectMap();
        queryParams.putIfNotEmpty("study", commandOptions.study);
        queryParams.putIfNotEmpty("id", commandOptions.id);
        queryParams.putIfNotEmpty("uuid", commandOptions.uuid);
        queryParams.putIfNotEmpty("name", commandOptions.name);
        queryParams.putIfNotEmpty("clinicalAnalysisId", commandOptions.clinicalAnalysisId);
        queryParams.putIfNotEmpty("analystId", commandOptions.analystId);
        queryParams.putIfNotEmpty("methodName", commandOptions.methodName);
        queryParams.putIfNotEmpty("panels", commandOptions.panels);
        queryParams.putIfNotEmpty("primaryFindings", commandOptions.primaryFindings);
        queryParams.putIfNotEmpty("secondaryFindings", commandOptions.secondaryFindings);
        queryParams.putIfNotEmpty("creationDate", commandOptions.creationDate);
        queryParams.putIfNotEmpty("modificationDate", commandOptions.modificationDate);
        queryParams.putIfNotEmpty("status", commandOptions.status);
        queryParams.putIfNotEmpty("internalStatus", commandOptions.internalStatus);
        queryParams.putIfNotEmpty("release", commandOptions.release);
        if (queryParams.get("study") == null && OpencgaMain.isShellMode()) {
            queryParams.putIfNotEmpty("study", sessionManager.getSession().getCurrentStudy());
        }

        return openCGAClient.getClinicalAnalysisClient().distinctInterpretation(commandOptions.field, queryParams);
    }

    private RestResponse<Interpretation> searchInterpretation() throws Exception {
        logger.debug("Executing searchInterpretation in Analysis - Clinical command line");

        AnalysisClinicalCommandOptions.SearchInterpretationCommandOptions commandOptions = analysisClinicalCommandOptions.searchInterpretationCommandOptions;

        ObjectMap queryParams = new ObjectMap();
        queryParams.putIfNotEmpty("include", commandOptions.include);
        queryParams.putIfNotEmpty("exclude", commandOptions.exclude);
        queryParams.putIfNotNull("limit", commandOptions.limit);
        queryParams.putIfNotNull("skip", commandOptions.skip);
        queryParams.putIfNotNull("sort", commandOptions.sort);
        queryParams.putIfNotEmpty("study", commandOptions.study);
        queryParams.putIfNotEmpty("id", commandOptions.id);
        queryParams.putIfNotEmpty("uuid", commandOptions.uuid);
        queryParams.putIfNotEmpty("name", commandOptions.name);
        queryParams.putIfNotEmpty("clinicalAnalysisId", commandOptions.clinicalAnalysisId);
        queryParams.putIfNotEmpty("analystId", commandOptions.analystId);
        queryParams.putIfNotEmpty("methodName", commandOptions.methodName);
        queryParams.putIfNotEmpty("panels", commandOptions.panels);
        queryParams.putIfNotEmpty("primaryFindings", commandOptions.primaryFindings);
        queryParams.putIfNotEmpty("secondaryFindings", commandOptions.secondaryFindings);
        queryParams.putIfNotEmpty("creationDate", commandOptions.creationDate);
        queryParams.putIfNotEmpty("modificationDate", commandOptions.modificationDate);
        queryParams.putIfNotEmpty("status", commandOptions.status);
        queryParams.putIfNotEmpty("internalStatus", commandOptions.internalStatus);
        queryParams.putIfNotEmpty("release", commandOptions.release);
        if (queryParams.get("study") == null && OpencgaMain.isShellMode()) {
            queryParams.putIfNotEmpty("study", sessionManager.getSession().getCurrentStudy());
        }

        return openCGAClient.getClinicalAnalysisClient().searchInterpretation(queryParams);
    }

    private RestResponse<Interpretation> infoInterpretation() throws Exception {
        logger.debug("Executing infoInterpretation in Analysis - Clinical command line");

        AnalysisClinicalCommandOptions.InfoInterpretationCommandOptions commandOptions = analysisClinicalCommandOptions.infoInterpretationCommandOptions;

        ObjectMap queryParams = new ObjectMap();
        queryParams.putIfNotEmpty("include", commandOptions.include);
        queryParams.putIfNotEmpty("exclude", commandOptions.exclude);
        queryParams.putIfNotEmpty("study", commandOptions.study);
        queryParams.putIfNotEmpty("version", commandOptions.version);
        queryParams.putIfNotNull("deleted", commandOptions.deleted);
        if (queryParams.get("study") == null && OpencgaMain.isShellMode()) {
            queryParams.putIfNotEmpty("study", sessionManager.getSession().getCurrentStudy());
        }

        return openCGAClient.getClinicalAnalysisClient().infoInterpretation(commandOptions.interpretations, queryParams);
    }

    private RestResponse<Interpretation> interpreter() throws Exception {
        logger.debug("Executing interpreter in Analysis - Clinical command line");

        AnalysisClinicalCommandOptions.InterpreterCommandOptions commandOptions = analysisClinicalCommandOptions.interpreterCommandOptions;

        ObjectMap queryParams = new ObjectMap();
        queryParams.putIfNotEmpty("clinicalAnalysisId", commandOptions.clinicalAnalysisId);
        queryParams.putIfNotEmpty("probandId", commandOptions.probandId);
        queryParams.putIfNotEmpty("familyId", commandOptions.familyId);
        queryParams.putIfNotEmpty("disorderId", commandOptions.disorderId);
        queryParams.putIfNotEmpty("panelIds", commandOptions.panelIds);
        queryParams.putIfNotEmpty("study", commandOptions.study);
        if (queryParams.get("study") == null && OpencgaMain.isShellMode()) {
            queryParams.putIfNotEmpty("study", sessionManager.getSession().getCurrentStudy());
        }

        return openCGAClient.getClinicalAnalysisClient().interpreter(commandOptions.configFile, queryParams);
    }

    private RestResponse<Job> runInterpreterExomiser() throws Exception {
        logger.debug("Executing runInterpreterExomiser in Analysis - Clinical command line");

        AnalysisClinicalCommandOptions.RunInterpreterExomiserCommandOptions commandOptions = analysisClinicalCommandOptions.runInterpreterExomiserCommandOptions;

        ObjectMap queryParams = new ObjectMap();
        queryParams.putIfNotEmpty("study", commandOptions.study);
        queryParams.putIfNotEmpty("jobId", commandOptions.jobId);
        queryParams.putIfNotEmpty("jobDescription", commandOptions.jobDescription);
        queryParams.putIfNotEmpty("jobDependsOn", commandOptions.jobDependsOn);
        queryParams.putIfNotEmpty("jobTags", commandOptions.jobTags);
        queryParams.putIfNotEmpty("jobScheduledStartTime", commandOptions.jobScheduledStartTime);
        queryParams.putIfNotEmpty("jobPriority", commandOptions.jobPriority);
        queryParams.putIfNotNull("jobDryRun", commandOptions.jobDryRun);
        if (queryParams.get("study") == null && OpencgaMain.isShellMode()) {
            queryParams.putIfNotEmpty("study", sessionManager.getSession().getCurrentStudy());
        }


        ExomiserInterpretationAnalysisParams exomiserInterpretationAnalysisParams = null;
        if (commandOptions.jsonDataModel) {
            RestResponse<Job> res = new RestResponse<>();
            res.setType(QueryType.VOID);
            PrintUtils.println(getObjectAsJSON(categoryName,"/{apiVersion}/analysis/clinical/interpreter/exomiser/run"));
            return res;
        } else if (commandOptions.jsonFile != null) {
            exomiserInterpretationAnalysisParams = JacksonUtils.getDefaultObjectMapper()
                    .readValue(new java.io.File(commandOptions.jsonFile), ExomiserInterpretationAnalysisParams.class);
        } else {
            ObjectMap beanParams = new ObjectMap();
            putNestedIfNotEmpty(beanParams, "clinicalAnalysis", commandOptions.clinicalAnalysis, true);
            putNestedIfNotEmpty(beanParams, "exomiserVersion", commandOptions.exomiserVersion, true);

            exomiserInterpretationAnalysisParams = JacksonUtils.getDefaultObjectMapper().copy()
                    .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, true)
                    .readValue(beanParams.toJson(), ExomiserInterpretationAnalysisParams.class);
        }
        return openCGAClient.getClinicalAnalysisClient().runInterpreterExomiser(exomiserInterpretationAnalysisParams, queryParams);
    }

    private RestResponse<Interpretation> rdInterpreter() throws Exception {
        logger.debug("Executing rdInterpreter in Analysis - Clinical command line");

        AnalysisClinicalCommandOptions.RdInterpreterCommandOptions commandOptions = analysisClinicalCommandOptions.rdInterpreterCommandOptions;

        ObjectMap queryParams = new ObjectMap();
        queryParams.putIfNotEmpty("clinicalAnalysisId", commandOptions.clinicalAnalysisId);
        queryParams.putIfNotEmpty("probandId", commandOptions.probandId);
        queryParams.putIfNotEmpty("familyId", commandOptions.familyId);
        queryParams.putIfNotEmpty("panelIds", commandOptions.panelIds);
        queryParams.putIfNotEmpty("disorderId", commandOptions.disorderId);
        queryParams.putIfNotEmpty("configFile", commandOptions.configFile);
        queryParams.putIfNotEmpty("study", commandOptions.study);
        if (queryParams.get("study") == null && OpencgaMain.isShellMode()) {
            queryParams.putIfNotEmpty("study", sessionManager.getSession().getCurrentStudy());
        }

        return openCGAClient.getClinicalAnalysisClient().rdInterpreter(queryParams);
    }

    private RestResponse<Job> runInterpreterRd() throws Exception {
        logger.debug("Executing runInterpreterRd in Analysis - Clinical command line");

        AnalysisClinicalCommandOptions.RunInterpreterRdCommandOptions commandOptions = analysisClinicalCommandOptions.runInterpreterRdCommandOptions;

        ObjectMap queryParams = new ObjectMap();
        queryParams.putIfNotEmpty("study", commandOptions.study);
        queryParams.putIfNotEmpty("jobId", commandOptions.jobId);
        queryParams.putIfNotEmpty("jobDescription", commandOptions.jobDescription);
        queryParams.putIfNotEmpty("jobDependsOn", commandOptions.jobDependsOn);
        queryParams.putIfNotEmpty("jobTags", commandOptions.jobTags);
        queryParams.putIfNotEmpty("jobScheduledStartTime", commandOptions.jobScheduledStartTime);
        queryParams.putIfNotEmpty("jobPriority", commandOptions.jobPriority);
        queryParams.putIfNotNull("jobDryRun", commandOptions.jobDryRun);
        if (queryParams.get("study") == null && OpencgaMain.isShellMode()) {
            queryParams.putIfNotEmpty("study", sessionManager.getSession().getCurrentStudy());
        }


        RdInterpretationAnalysisToolParams rdInterpretationAnalysisToolParams = null;
        if (commandOptions.jsonDataModel) {
            RestResponse<Job> res = new RestResponse<>();
            res.setType(QueryType.VOID);
            PrintUtils.println(getObjectAsJSON(categoryName,"/{apiVersion}/analysis/clinical/interpreter/rd/run"));
            return res;
        } else if (commandOptions.jsonFile != null) {
            rdInterpretationAnalysisToolParams = JacksonUtils.getDefaultObjectMapper()
                    .readValue(new java.io.File(commandOptions.jsonFile), RdInterpretationAnalysisToolParams.class);
        } else {
            ObjectMap beanParams = new ObjectMap();
            putNestedIfNotEmpty(beanParams, "name", commandOptions.name, true);
            putNestedIfNotEmpty(beanParams, "description", commandOptions.description, true);
            putNestedIfNotEmpty(beanParams, "clinicalAnalysisId", commandOptions.clinicalAnalysisId, true);
            putNestedIfNotNull(beanParams, "primary", commandOptions.primary, true);
            putNestedIfNotEmpty(beanParams, "configFile", commandOptions.configFile, true);
            putNestedIfNotEmpty(beanParams, "outdir", commandOptions.outdir, true);

            rdInterpretationAnalysisToolParams = JacksonUtils.getDefaultObjectMapper().copy()
                    .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, true)
                    .readValue(beanParams.toJson(), RdInterpretationAnalysisToolParams.class);
        }
        return openCGAClient.getClinicalAnalysisClient().runInterpreterRd(rdInterpretationAnalysisToolParams, queryParams);
    }

    private RestResponse<Job> runInterpreter() throws Exception {
        logger.debug("Executing runInterpreter in Analysis - Clinical command line");

        AnalysisClinicalCommandOptions.RunInterpreterCommandOptions commandOptions = analysisClinicalCommandOptions.runInterpreterCommandOptions;

        ObjectMap queryParams = new ObjectMap();
        queryParams.putIfNotEmpty("study", commandOptions.study);
        queryParams.putIfNotEmpty("jobId", commandOptions.jobId);
        queryParams.putIfNotEmpty("jobDescription", commandOptions.jobDescription);
        queryParams.putIfNotEmpty("jobDependsOn", commandOptions.jobDependsOn);
        queryParams.putIfNotEmpty("jobTags", commandOptions.jobTags);
        queryParams.putIfNotEmpty("jobScheduledStartTime", commandOptions.jobScheduledStartTime);
        queryParams.putIfNotEmpty("jobPriority", commandOptions.jobPriority);
        queryParams.putIfNotNull("jobDryRun", commandOptions.jobDryRun);
        if (queryParams.get("study") == null && OpencgaMain.isShellMode()) {
            queryParams.putIfNotEmpty("study", sessionManager.getSession().getCurrentStudy());
        }


        RdInterpretationAnalysisToolParams rdInterpretationAnalysisToolParams = null;
        if (commandOptions.jsonDataModel) {
            RestResponse<Job> res = new RestResponse<>();
            res.setType(QueryType.VOID);
            PrintUtils.println(getObjectAsJSON(categoryName,"/{apiVersion}/analysis/clinical/interpreter/run"));
            return res;
        } else if (commandOptions.jsonFile != null) {
            rdInterpretationAnalysisToolParams = JacksonUtils.getDefaultObjectMapper()
                    .readValue(new java.io.File(commandOptions.jsonFile), RdInterpretationAnalysisToolParams.class);
        } else {
            ObjectMap beanParams = new ObjectMap();
            putNestedIfNotEmpty(beanParams, "name", commandOptions.name, true);
            putNestedIfNotEmpty(beanParams, "description", commandOptions.description, true);
            putNestedIfNotEmpty(beanParams, "clinicalAnalysisId", commandOptions.clinicalAnalysisId, true);
            putNestedIfNotNull(beanParams, "primary", commandOptions.primary, true);
            putNestedIfNotEmpty(beanParams, "configFile", commandOptions.configFile, true);
            putNestedIfNotEmpty(beanParams, "outdir", commandOptions.outdir, true);

            rdInterpretationAnalysisToolParams = JacksonUtils.getDefaultObjectMapper().copy()
                    .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, true)
                    .readValue(beanParams.toJson(), RdInterpretationAnalysisToolParams.class);
        }
        return openCGAClient.getClinicalAnalysisClient().runInterpreter(rdInterpretationAnalysisToolParams, queryParams);
    }

    private RestResponse<Job> load() throws Exception {
        logger.debug("Executing load in Analysis - Clinical command line");

        AnalysisClinicalCommandOptions.LoadCommandOptions commandOptions = analysisClinicalCommandOptions.loadCommandOptions;

        ObjectMap queryParams = new ObjectMap();
        queryParams.putIfNotEmpty("study", commandOptions.study);
        queryParams.putIfNotEmpty("jobId", commandOptions.jobId);
        queryParams.putIfNotEmpty("jobDescription", commandOptions.jobDescription);
        queryParams.putIfNotEmpty("jobDependsOn", commandOptions.jobDependsOn);
        queryParams.putIfNotEmpty("jobTags", commandOptions.jobTags);
        queryParams.putIfNotEmpty("jobScheduledStartTime", commandOptions.jobScheduledStartTime);
        queryParams.putIfNotEmpty("jobPriority", commandOptions.jobPriority);
        queryParams.putIfNotNull("jobDryRun", commandOptions.jobDryRun);
        if (queryParams.get("study") == null && OpencgaMain.isShellMode()) {
            queryParams.putIfNotEmpty("study", sessionManager.getSession().getCurrentStudy());
        }


        ClinicalAnalysisLoadParams clinicalAnalysisLoadParams = null;
        if (commandOptions.jsonDataModel) {
            RestResponse<Job> res = new RestResponse<>();
            res.setType(QueryType.VOID);
            PrintUtils.println(getObjectAsJSON(categoryName,"/{apiVersion}/analysis/clinical/load"));
            return res;
        } else if (commandOptions.jsonFile != null) {
            clinicalAnalysisLoadParams = JacksonUtils.getDefaultObjectMapper()
                    .readValue(new java.io.File(commandOptions.jsonFile), ClinicalAnalysisLoadParams.class);
        } else {
            ObjectMap beanParams = new ObjectMap();
            putNestedIfNotEmpty(beanParams, "file", commandOptions.file, true);

            clinicalAnalysisLoadParams = JacksonUtils.getDefaultObjectMapper().copy()
                    .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, true)
                    .readValue(beanParams.toJson(), ClinicalAnalysisLoadParams.class);
        }
        return openCGAClient.getClinicalAnalysisClient().load(clinicalAnalysisLoadParams, queryParams);
    }

    private RestResponse<AlleleTyperResult> alleleTyperPharmacogenomics() throws Exception {
        logger.debug("Executing alleleTyperPharmacogenomics in Analysis - Clinical command line");

        AnalysisClinicalCommandOptions.AlleleTyperPharmacogenomicsCommandOptions commandOptions = analysisClinicalCommandOptions.alleleTyperPharmacogenomicsCommandOptions;

        ObjectMap queryParams = new ObjectMap();
        queryParams.putIfNotEmpty("study", commandOptions.study);
        if (queryParams.get("study") == null && OpencgaMain.isShellMode()) {
            queryParams.putIfNotEmpty("study", sessionManager.getSession().getCurrentStudy());
        }


        PharmacogenomicsAlleleTyperParams pharmacogenomicsAlleleTyperParams = null;
        if (commandOptions.jsonDataModel) {
            RestResponse<AlleleTyperResult> res = new RestResponse<>();
            res.setType(QueryType.VOID);
            PrintUtils.println(getObjectAsJSON(categoryName,"/{apiVersion}/analysis/clinical/pharmacogenomics/alleleTyper"));
            return res;
        } else if (commandOptions.jsonFile != null) {
            pharmacogenomicsAlleleTyperParams = JacksonUtils.getDefaultObjectMapper()
                    .readValue(new java.io.File(commandOptions.jsonFile), PharmacogenomicsAlleleTyperParams.class);
        } else {
            ObjectMap beanParams = new ObjectMap();
            putNestedIfNotEmpty(beanParams, "genotypingContent", commandOptions.genotypingContent, true);
            putNestedIfNotEmpty(beanParams, "translationContent", commandOptions.translationContent, true);

            pharmacogenomicsAlleleTyperParams = JacksonUtils.getDefaultObjectMapper().copy()
                    .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, true)
                    .readValue(beanParams.toJson(), PharmacogenomicsAlleleTyperParams.class);
        }
        return openCGAClient.getClinicalAnalysisClient().alleleTyperPharmacogenomics(pharmacogenomicsAlleleTyperParams, queryParams);
    }

    private RestResponse<Job> runPharmacogenomicsAlleleTyper() throws Exception {
        logger.debug("Executing runPharmacogenomicsAlleleTyper in Analysis - Clinical command line");

        AnalysisClinicalCommandOptions.RunPharmacogenomicsAlleleTyperCommandOptions commandOptions = analysisClinicalCommandOptions.runPharmacogenomicsAlleleTyperCommandOptions;

        ObjectMap queryParams = new ObjectMap();
        queryParams.putIfNotEmpty("study", commandOptions.study);
        queryParams.putIfNotEmpty("jobId", commandOptions.jobId);
        queryParams.putIfNotEmpty("jobDescription", commandOptions.jobDescription);
        queryParams.putIfNotEmpty("jobDependsOn", commandOptions.jobDependsOn);
        queryParams.putIfNotEmpty("jobTags", commandOptions.jobTags);
        queryParams.putIfNotEmpty("jobScheduledStartTime", commandOptions.jobScheduledStartTime);
        queryParams.putIfNotEmpty("jobPriority", commandOptions.jobPriority);
        queryParams.putIfNotNull("jobDryRun", commandOptions.jobDryRun);
        if (queryParams.get("study") == null && OpencgaMain.isShellMode()) {
            queryParams.putIfNotEmpty("study", sessionManager.getSession().getCurrentStudy());
        }


        PharmacogenomicsAlleleTyperToolParams pharmacogenomicsAlleleTyperToolParams = null;
        if (commandOptions.jsonDataModel) {
            RestResponse<Job> res = new RestResponse<>();
            res.setType(QueryType.VOID);
            PrintUtils.println(getObjectAsJSON(categoryName,"/{apiVersion}/analysis/clinical/pharmacogenomics/alleleTyper/run"));
            return res;
        } else if (commandOptions.jsonFile != null) {
            pharmacogenomicsAlleleTyperToolParams = JacksonUtils.getDefaultObjectMapper()
                    .readValue(new java.io.File(commandOptions.jsonFile), PharmacogenomicsAlleleTyperToolParams.class);
        } else {
            ObjectMap beanParams = new ObjectMap();
            putNestedIfNotEmpty(beanParams, "genotypingContent", commandOptions.genotypingContent, true);
            putNestedIfNotEmpty(beanParams, "translationContent", commandOptions.translationContent, true);
            putNestedIfNotNull(beanParams, "annotate", commandOptions.annotate, true);
            putNestedIfNotEmpty(beanParams, "outdir", commandOptions.outdir, true);

            pharmacogenomicsAlleleTyperToolParams = JacksonUtils.getDefaultObjectMapper().copy()
                    .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, true)
                    .readValue(beanParams.toJson(), PharmacogenomicsAlleleTyperToolParams.class);
        }
        return openCGAClient.getClinicalAnalysisClient().runPharmacogenomicsAlleleTyper(pharmacogenomicsAlleleTyperToolParams, queryParams);
    }

    private RestResponse<Job> runPharmacogenomicsAnnotation() throws Exception {
        logger.debug("Executing runPharmacogenomicsAnnotation in Analysis - Clinical command line");

        AnalysisClinicalCommandOptions.RunPharmacogenomicsAnnotationCommandOptions commandOptions = analysisClinicalCommandOptions.runPharmacogenomicsAnnotationCommandOptions;

        ObjectMap queryParams = new ObjectMap();
        queryParams.putIfNotEmpty("study", commandOptions.study);
        queryParams.putIfNotEmpty("jobId", commandOptions.jobId);
        queryParams.putIfNotEmpty("jobDescription", commandOptions.jobDescription);
        queryParams.putIfNotEmpty("jobDependsOn", commandOptions.jobDependsOn);
        queryParams.putIfNotEmpty("jobTags", commandOptions.jobTags);
        queryParams.putIfNotEmpty("jobScheduledStartTime", commandOptions.jobScheduledStartTime);
        queryParams.putIfNotEmpty("jobPriority", commandOptions.jobPriority);
        queryParams.putIfNotNull("jobDryRun", commandOptions.jobDryRun);
        if (queryParams.get("study") == null && OpencgaMain.isShellMode()) {
            queryParams.putIfNotEmpty("study", sessionManager.getSession().getCurrentStudy());
        }


        PharmacogenomicsAnnotationAnalysisToolParams pharmacogenomicsAnnotationAnalysisToolParams = null;
        if (commandOptions.jsonDataModel) {
            RestResponse<Job> res = new RestResponse<>();
            res.setType(QueryType.VOID);
            PrintUtils.println(getObjectAsJSON(categoryName,"/{apiVersion}/analysis/clinical/pharmacogenomics/annotation/run"));
            return res;
        } else if (commandOptions.jsonFile != null) {
            pharmacogenomicsAnnotationAnalysisToolParams = JacksonUtils.getDefaultObjectMapper()
                    .readValue(new java.io.File(commandOptions.jsonFile), PharmacogenomicsAnnotationAnalysisToolParams.class);
        } else {
            ObjectMap beanParams = new ObjectMap();
            putNestedIfNotEmpty(beanParams, "alleleTyperContent", commandOptions.alleleTyperContent, true);

            pharmacogenomicsAnnotationAnalysisToolParams = JacksonUtils.getDefaultObjectMapper().copy()
                    .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, true)
                    .readValue(beanParams.toJson(), PharmacogenomicsAnnotationAnalysisToolParams.class);
        }
        return openCGAClient.getClinicalAnalysisClient().runPharmacogenomicsAnnotation(pharmacogenomicsAnnotationAnalysisToolParams, queryParams);
    }

    private RestResponse<Job> runPipelineAffy() throws Exception {
        logger.debug("Executing runPipelineAffy in Analysis - Clinical command line");

        AnalysisClinicalCommandOptions.RunPipelineAffyCommandOptions commandOptions = analysisClinicalCommandOptions.runPipelineAffyCommandOptions;

        ObjectMap queryParams = new ObjectMap();
        queryParams.putIfNotEmpty("study", commandOptions.study);
        queryParams.putIfNotEmpty("jobId", commandOptions.jobId);
        queryParams.putIfNotEmpty("jobDescription", commandOptions.jobDescription);
        queryParams.putIfNotEmpty("jobDependsOn", commandOptions.jobDependsOn);
        queryParams.putIfNotEmpty("jobTags", commandOptions.jobTags);
        queryParams.putIfNotEmpty("jobScheduledStartTime", commandOptions.jobScheduledStartTime);
        queryParams.putIfNotEmpty("jobPriority", commandOptions.jobPriority);
        queryParams.putIfNotNull("jobDryRun", commandOptions.jobDryRun);
        if (queryParams.get("study") == null && OpencgaMain.isShellMode()) {
            queryParams.putIfNotEmpty("study", sessionManager.getSession().getCurrentStudy());
        }


        AffyClinicalPipelineWrapperParams affyClinicalPipelineWrapperParams = null;
        if (commandOptions.jsonDataModel) {
            RestResponse<Job> res = new RestResponse<>();
            res.setType(QueryType.VOID);
            PrintUtils.println(getObjectAsJSON(categoryName,"/{apiVersion}/analysis/clinical/pipeline/affy/run"));
            return res;
        } else if (commandOptions.jsonFile != null) {
            affyClinicalPipelineWrapperParams = JacksonUtils.getDefaultObjectMapper()
                    .readValue(new java.io.File(commandOptions.jsonFile), AffyClinicalPipelineWrapperParams.class);
        } else {
            ObjectMap beanParams = new ObjectMap();
            putNestedIfNotEmpty(beanParams, "pipelineParams.chip", commandOptions.pipelineParamsChip, true);
            putNestedIfNotEmpty(beanParams, "pipelineParams.indexDir", commandOptions.pipelineParamsIndexDir, true);
            putNestedIfNotEmpty(beanParams, "pipelineParams.dataDir", commandOptions.pipelineParamsDataDir, true);
            putNestedIfNotEmpty(beanParams, "pipelineParams.pipelineFile", commandOptions.pipelineParamsPipelineFile, true);
            putNestedIfNotEmpty(beanParams, "outdir", commandOptions.outdir, true);

            affyClinicalPipelineWrapperParams = JacksonUtils.getDefaultObjectMapper().copy()
                    .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, true)
                    .readValue(beanParams.toJson(), AffyClinicalPipelineWrapperParams.class);
        }
        return openCGAClient.getClinicalAnalysisClient().runPipelineAffy(affyClinicalPipelineWrapperParams, queryParams);
    }

    private RestResponse<Job> runPipelineGenomics() throws Exception {
        logger.debug("Executing runPipelineGenomics in Analysis - Clinical command line");

        AnalysisClinicalCommandOptions.RunPipelineGenomicsCommandOptions commandOptions = analysisClinicalCommandOptions.runPipelineGenomicsCommandOptions;

        ObjectMap queryParams = new ObjectMap();
        queryParams.putIfNotEmpty("study", commandOptions.study);
        queryParams.putIfNotEmpty("jobId", commandOptions.jobId);
        queryParams.putIfNotEmpty("jobDescription", commandOptions.jobDescription);
        queryParams.putIfNotEmpty("jobDependsOn", commandOptions.jobDependsOn);
        queryParams.putIfNotEmpty("jobTags", commandOptions.jobTags);
        queryParams.putIfNotEmpty("jobScheduledStartTime", commandOptions.jobScheduledStartTime);
        queryParams.putIfNotEmpty("jobPriority", commandOptions.jobPriority);
        queryParams.putIfNotNull("jobDryRun", commandOptions.jobDryRun);
        if (queryParams.get("study") == null && OpencgaMain.isShellMode()) {
            queryParams.putIfNotEmpty("study", sessionManager.getSession().getCurrentStudy());
        }


        GenomicsClinicalPipelineWrapperParams genomicsClinicalPipelineWrapperParams = null;
        if (commandOptions.jsonDataModel) {
            RestResponse<Job> res = new RestResponse<>();
            res.setType(QueryType.VOID);
            PrintUtils.println(getObjectAsJSON(categoryName,"/{apiVersion}/analysis/clinical/pipeline/genomics/run"));
            return res;
        } else if (commandOptions.jsonFile != null) {
            genomicsClinicalPipelineWrapperParams = JacksonUtils.getDefaultObjectMapper()
                    .readValue(new java.io.File(commandOptions.jsonFile), GenomicsClinicalPipelineWrapperParams.class);
        } else {
            ObjectMap beanParams = new ObjectMap();
            putNestedIfNotEmpty(beanParams, "pipelineParams.indexDir", commandOptions.pipelineParamsIndexDir, true);
            putNestedIfNotNull(beanParams, "pipelineParams.steps", commandOptions.pipelineParamsSteps, true);
            putNestedIfNotEmpty(beanParams, "pipelineParams.pipelineFile", commandOptions.pipelineParamsPipelineFile, true);
            putNestedIfNotNull(beanParams, "pipelineParams.samples", commandOptions.pipelineParamsSamples, true);
            putNestedIfNotEmpty(beanParams, "outdir", commandOptions.outdir, true);

            genomicsClinicalPipelineWrapperParams = JacksonUtils.getDefaultObjectMapper().copy()
                    .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, true)
                    .readValue(beanParams.toJson(), GenomicsClinicalPipelineWrapperParams.class);
        }
        return openCGAClient.getClinicalAnalysisClient().runPipelineGenomics(genomicsClinicalPipelineWrapperParams, queryParams);
    }

    private RestResponse<Job> runPipelinePrepare() throws Exception {
        logger.debug("Executing runPipelinePrepare in Analysis - Clinical command line");

        AnalysisClinicalCommandOptions.RunPipelinePrepareCommandOptions commandOptions = analysisClinicalCommandOptions.runPipelinePrepareCommandOptions;

        ObjectMap queryParams = new ObjectMap();
        queryParams.putIfNotEmpty("study", commandOptions.study);
        queryParams.putIfNotEmpty("jobId", commandOptions.jobId);
        queryParams.putIfNotEmpty("jobDescription", commandOptions.jobDescription);
        queryParams.putIfNotEmpty("jobDependsOn", commandOptions.jobDependsOn);
        queryParams.putIfNotEmpty("jobTags", commandOptions.jobTags);
        queryParams.putIfNotEmpty("jobScheduledStartTime", commandOptions.jobScheduledStartTime);
        queryParams.putIfNotEmpty("jobPriority", commandOptions.jobPriority);
        queryParams.putIfNotNull("jobDryRun", commandOptions.jobDryRun);
        if (queryParams.get("study") == null && OpencgaMain.isShellMode()) {
            queryParams.putIfNotEmpty("study", sessionManager.getSession().getCurrentStudy());
        }


        PrepareClinicalPipelineWrapperParams prepareClinicalPipelineWrapperParams = null;
        if (commandOptions.jsonDataModel) {
            RestResponse<Job> res = new RestResponse<>();
            res.setType(QueryType.VOID);
            PrintUtils.println(getObjectAsJSON(categoryName,"/{apiVersion}/analysis/clinical/pipeline/prepare/run"));
            return res;
        } else if (commandOptions.jsonFile != null) {
            prepareClinicalPipelineWrapperParams = JacksonUtils.getDefaultObjectMapper()
                    .readValue(new java.io.File(commandOptions.jsonFile), PrepareClinicalPipelineWrapperParams.class);
        } else {
            ObjectMap beanParams = new ObjectMap();
            putNestedIfNotEmpty(beanParams, "pipelineParams.referenceGenome", commandOptions.pipelineParamsReferenceGenome, true);
            putNestedIfNotNull(beanParams, "pipelineParams.indexes", commandOptions.pipelineParamsIndexes, true);
            putNestedIfNotEmpty(beanParams, "outdir", commandOptions.outdir, true);

            prepareClinicalPipelineWrapperParams = JacksonUtils.getDefaultObjectMapper().copy()
                    .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, true)
                    .readValue(beanParams.toJson(), PrepareClinicalPipelineWrapperParams.class);
        }
        return openCGAClient.getClinicalAnalysisClient().runPipelinePrepare(prepareClinicalPipelineWrapperParams, queryParams);
    }

    private RestResponse<FacetField> aggregationStatsRga() throws Exception {
        logger.debug("Executing aggregationStatsRga in Analysis - Clinical command line");

        AnalysisClinicalCommandOptions.AggregationStatsRgaCommandOptions commandOptions = analysisClinicalCommandOptions.aggregationStatsRgaCommandOptions;

        ObjectMap queryParams = new ObjectMap();
        queryParams.putIfNotNull("limit", commandOptions.limit);
        queryParams.putIfNotNull("skip", commandOptions.skip);
        queryParams.putIfNotEmpty("sampleId", commandOptions.sampleId);
        queryParams.putIfNotEmpty("individualId", commandOptions.individualId);
        queryParams.putIfNotEmpty("sex", commandOptions.sex);
        queryParams.putIfNotEmpty("phenotypes", commandOptions.phenotypes);
        queryParams.putIfNotEmpty("disorders", commandOptions.disorders);
        queryParams.putIfNotEmpty("numParents", commandOptions.numParents);
        queryParams.putIfNotEmpty("geneId", commandOptions.geneId);
        queryParams.putIfNotEmpty("geneName", commandOptions.geneName);
        queryParams.putIfNotEmpty("chromosome", commandOptions.chromosome);
        queryParams.putIfNotEmpty("start", commandOptions.start);
        queryParams.putIfNotEmpty("end", commandOptions.end);
        queryParams.putIfNotEmpty("transcriptId", commandOptions.transcriptId);
        queryParams.putIfNotEmpty("variants", commandOptions.variants);
        queryParams.putIfNotEmpty("dbSnps", commandOptions.dbSnps);
        queryParams.putIfNotEmpty("knockoutType", commandOptions.knockoutType);
        queryParams.putIfNotEmpty("filter", commandOptions.filter);
        queryParams.putIfNotEmpty("type", commandOptions.type);
        queryParams.putIfNotEmpty("clinicalSignificance", commandOptions.clinicalSignificance);
        queryParams.putIfNotEmpty("populationFrequency", commandOptions.populationFrequency);
        queryParams.putIfNotEmpty("consequenceType", commandOptions.consequenceType);
        queryParams.putIfNotEmpty("study", commandOptions.study);
        if (queryParams.get("study") == null && OpencgaMain.isShellMode()) {
            queryParams.putIfNotEmpty("study", sessionManager.getSession().getCurrentStudy());
        }

        return openCGAClient.getClinicalAnalysisClient().aggregationStatsRga(commandOptions.field, queryParams);
    }

    private RestResponse<RgaKnockoutByGene> queryRgaGene() throws Exception {
        logger.debug("Executing queryRgaGene in Analysis - Clinical command line");

        AnalysisClinicalCommandOptions.QueryRgaGeneCommandOptions commandOptions = analysisClinicalCommandOptions.queryRgaGeneCommandOptions;

        ObjectMap queryParams = new ObjectMap();
        queryParams.putIfNotEmpty("include", commandOptions.include);
        queryParams.putIfNotEmpty("exclude", commandOptions.exclude);
        queryParams.putIfNotNull("limit", commandOptions.limit);
        queryParams.putIfNotNull("skip", commandOptions.skip);
        queryParams.putIfNotNull("count", commandOptions.count);
        queryParams.putIfNotEmpty("includeIndividual", commandOptions.includeIndividual);
        queryParams.putIfNotNull("skipIndividual", commandOptions.skipIndividual);
        queryParams.putIfNotNull("limitIndividual", commandOptions.limitIndividual);
        queryParams.putIfNotEmpty("sampleId", commandOptions.sampleId);
        queryParams.putIfNotEmpty("individualId", commandOptions.individualId);
        queryParams.putIfNotEmpty("sex", commandOptions.sex);
        queryParams.putIfNotEmpty("phenotypes", commandOptions.phenotypes);
        queryParams.putIfNotEmpty("disorders", commandOptions.disorders);
        queryParams.putIfNotEmpty("numParents", commandOptions.numParents);
        queryParams.putIfNotEmpty("geneId", commandOptions.geneId);
        queryParams.putIfNotEmpty("geneName", commandOptions.geneName);
        queryParams.putIfNotEmpty("chromosome", commandOptions.chromosome);
        queryParams.putIfNotEmpty("start", commandOptions.start);
        queryParams.putIfNotEmpty("end", commandOptions.end);
        queryParams.putIfNotEmpty("transcriptId", commandOptions.transcriptId);
        queryParams.putIfNotEmpty("variants", commandOptions.variants);
        queryParams.putIfNotEmpty("dbSnps", commandOptions.dbSnps);
        queryParams.putIfNotEmpty("knockoutType", commandOptions.knockoutType);
        queryParams.putIfNotEmpty("filter", commandOptions.filter);
        queryParams.putIfNotEmpty("type", commandOptions.type);
        queryParams.putIfNotEmpty("clinicalSignificance", commandOptions.clinicalSignificance);
        queryParams.putIfNotEmpty("populationFrequency", commandOptions.populationFrequency);
        queryParams.putIfNotEmpty("consequenceType", commandOptions.consequenceType);
        queryParams.putIfNotEmpty("study", commandOptions.study);
        if (queryParams.get("study") == null && OpencgaMain.isShellMode()) {
            queryParams.putIfNotEmpty("study", sessionManager.getSession().getCurrentStudy());
        }

        return openCGAClient.getClinicalAnalysisClient().queryRgaGene(queryParams);
    }

    private RestResponse<KnockoutByGeneSummary> summaryRgaGene() throws Exception {
        logger.debug("Executing summaryRgaGene in Analysis - Clinical command line");

        AnalysisClinicalCommandOptions.SummaryRgaGeneCommandOptions commandOptions = analysisClinicalCommandOptions.summaryRgaGeneCommandOptions;

        ObjectMap queryParams = new ObjectMap();
        queryParams.putIfNotNull("limit", commandOptions.limit);
        queryParams.putIfNotNull("skip", commandOptions.skip);
        queryParams.putIfNotNull("count", commandOptions.count);
        queryParams.putIfNotEmpty("sampleId", commandOptions.sampleId);
        queryParams.putIfNotEmpty("individualId", commandOptions.individualId);
        queryParams.putIfNotEmpty("sex", commandOptions.sex);
        queryParams.putIfNotEmpty("phenotypes", commandOptions.phenotypes);
        queryParams.putIfNotEmpty("disorders", commandOptions.disorders);
        queryParams.putIfNotEmpty("numParents", commandOptions.numParents);
        queryParams.putIfNotEmpty("geneId", commandOptions.geneId);
        queryParams.putIfNotEmpty("geneName", commandOptions.geneName);
        queryParams.putIfNotEmpty("chromosome", commandOptions.chromosome);
        queryParams.putIfNotEmpty("start", commandOptions.start);
        queryParams.putIfNotEmpty("end", commandOptions.end);
        queryParams.putIfNotEmpty("transcriptId", commandOptions.transcriptId);
        queryParams.putIfNotEmpty("variants", commandOptions.variants);
        queryParams.putIfNotEmpty("dbSnps", commandOptions.dbSnps);
        queryParams.putIfNotEmpty("knockoutType", commandOptions.knockoutType);
        queryParams.putIfNotEmpty("filter", commandOptions.filter);
        queryParams.putIfNotEmpty("type", commandOptions.type);
        queryParams.putIfNotEmpty("clinicalSignificance", commandOptions.clinicalSignificance);
        queryParams.putIfNotEmpty("populationFrequency", commandOptions.populationFrequency);
        queryParams.putIfNotEmpty("consequenceType", commandOptions.consequenceType);
        queryParams.putIfNotEmpty("study", commandOptions.study);
        if (queryParams.get("study") == null && OpencgaMain.isShellMode()) {
            queryParams.putIfNotEmpty("study", sessionManager.getSession().getCurrentStudy());
        }

        return openCGAClient.getClinicalAnalysisClient().summaryRgaGene(queryParams);
    }

    private RestResponse<Job> runRgaIndex() throws Exception {
        logger.debug("Executing runRgaIndex in Analysis - Clinical command line");

        AnalysisClinicalCommandOptions.RunRgaIndexCommandOptions commandOptions = analysisClinicalCommandOptions.runRgaIndexCommandOptions;

        ObjectMap queryParams = new ObjectMap();
        queryParams.putIfNotEmpty("study", commandOptions.study);
        queryParams.putIfNotEmpty("jobId", commandOptions.jobId);
        queryParams.putIfNotEmpty("jobDescription", commandOptions.jobDescription);
        queryParams.putIfNotEmpty("jobDependsOn", commandOptions.jobDependsOn);
        queryParams.putIfNotEmpty("jobTags", commandOptions.jobTags);
        queryParams.putIfNotEmpty("jobScheduledStartTime", commandOptions.jobScheduledStartTime);
        queryParams.putIfNotEmpty("jobPriority", commandOptions.jobPriority);
        queryParams.putIfNotNull("jobDryRun", commandOptions.jobDryRun);
        queryParams.putIfNotNull("auxiliarIndex", commandOptions.auxiliarIndex);
        if (queryParams.get("study") == null && OpencgaMain.isShellMode()) {
            queryParams.putIfNotEmpty("study", sessionManager.getSession().getCurrentStudy());
        }


        RgaAnalysisParams rgaAnalysisParams = null;
        if (commandOptions.jsonDataModel) {
            RestResponse<Job> res = new RestResponse<>();
            res.setType(QueryType.VOID);
            PrintUtils.println(getObjectAsJSON(categoryName,"/{apiVersion}/analysis/clinical/rga/index/run"));
            return res;
        } else if (commandOptions.jsonFile != null) {
            rgaAnalysisParams = JacksonUtils.getDefaultObjectMapper()
                    .readValue(new java.io.File(commandOptions.jsonFile), RgaAnalysisParams.class);
        } else {
            ObjectMap beanParams = new ObjectMap();
            putNestedIfNotEmpty(beanParams, "file", commandOptions.file, true);

            rgaAnalysisParams = JacksonUtils.getDefaultObjectMapper().copy()
                    .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, true)
                    .readValue(beanParams.toJson(), RgaAnalysisParams.class);
        }
        return openCGAClient.getClinicalAnalysisClient().runRgaIndex(rgaAnalysisParams, queryParams);
    }

    private RestResponse<KnockoutByIndividual> queryRgaIndividual() throws Exception {
        logger.debug("Executing queryRgaIndividual in Analysis - Clinical command line");

        AnalysisClinicalCommandOptions.QueryRgaIndividualCommandOptions commandOptions = analysisClinicalCommandOptions.queryRgaIndividualCommandOptions;

        ObjectMap queryParams = new ObjectMap();
        queryParams.putIfNotEmpty("include", commandOptions.include);
        queryParams.putIfNotEmpty("exclude", commandOptions.exclude);
        queryParams.putIfNotNull("limit", commandOptions.limit);
        queryParams.putIfNotNull("skip", commandOptions.skip);
        queryParams.putIfNotNull("count", commandOptions.count);
        queryParams.putIfNotEmpty("sampleId", commandOptions.sampleId);
        queryParams.putIfNotEmpty("individualId", commandOptions.individualId);
        queryParams.putIfNotEmpty("sex", commandOptions.sex);
        queryParams.putIfNotEmpty("phenotypes", commandOptions.phenotypes);
        queryParams.putIfNotEmpty("disorders", commandOptions.disorders);
        queryParams.putIfNotEmpty("numParents", commandOptions.numParents);
        queryParams.putIfNotEmpty("geneId", commandOptions.geneId);
        queryParams.putIfNotEmpty("geneName", commandOptions.geneName);
        queryParams.putIfNotEmpty("chromosome", commandOptions.chromosome);
        queryParams.putIfNotEmpty("start", commandOptions.start);
        queryParams.putIfNotEmpty("end", commandOptions.end);
        queryParams.putIfNotEmpty("transcriptId", commandOptions.transcriptId);
        queryParams.putIfNotEmpty("variants", commandOptions.variants);
        queryParams.putIfNotEmpty("dbSnps", commandOptions.dbSnps);
        queryParams.putIfNotEmpty("knockoutType", commandOptions.knockoutType);
        queryParams.putIfNotEmpty("filter", commandOptions.filter);
        queryParams.putIfNotEmpty("type", commandOptions.type);
        queryParams.putIfNotEmpty("clinicalSignificance", commandOptions.clinicalSignificance);
        queryParams.putIfNotEmpty("populationFrequency", commandOptions.populationFrequency);
        queryParams.putIfNotEmpty("consequenceType", commandOptions.consequenceType);
        queryParams.putIfNotEmpty("study", commandOptions.study);
        if (queryParams.get("study") == null && OpencgaMain.isShellMode()) {
            queryParams.putIfNotEmpty("study", sessionManager.getSession().getCurrentStudy());
        }

        return openCGAClient.getClinicalAnalysisClient().queryRgaIndividual(queryParams);
    }

    private RestResponse<KnockoutByIndividualSummary> summaryRgaIndividual() throws Exception {
        logger.debug("Executing summaryRgaIndividual in Analysis - Clinical command line");

        AnalysisClinicalCommandOptions.SummaryRgaIndividualCommandOptions commandOptions = analysisClinicalCommandOptions.summaryRgaIndividualCommandOptions;

        ObjectMap queryParams = new ObjectMap();
        queryParams.putIfNotNull("limit", commandOptions.limit);
        queryParams.putIfNotNull("skip", commandOptions.skip);
        queryParams.putIfNotNull("count", commandOptions.count);
        queryParams.putIfNotEmpty("sampleId", commandOptions.sampleId);
        queryParams.putIfNotEmpty("individualId", commandOptions.individualId);
        queryParams.putIfNotEmpty("sex", commandOptions.sex);
        queryParams.putIfNotEmpty("phenotypes", commandOptions.phenotypes);
        queryParams.putIfNotEmpty("disorders", commandOptions.disorders);
        queryParams.putIfNotEmpty("numParents", commandOptions.numParents);
        queryParams.putIfNotEmpty("geneId", commandOptions.geneId);
        queryParams.putIfNotEmpty("geneName", commandOptions.geneName);
        queryParams.putIfNotEmpty("chromosome", commandOptions.chromosome);
        queryParams.putIfNotEmpty("start", commandOptions.start);
        queryParams.putIfNotEmpty("end", commandOptions.end);
        queryParams.putIfNotEmpty("transcriptId", commandOptions.transcriptId);
        queryParams.putIfNotEmpty("variants", commandOptions.variants);
        queryParams.putIfNotEmpty("dbSnps", commandOptions.dbSnps);
        queryParams.putIfNotEmpty("knockoutType", commandOptions.knockoutType);
        queryParams.putIfNotEmpty("filter", commandOptions.filter);
        queryParams.putIfNotEmpty("type", commandOptions.type);
        queryParams.putIfNotEmpty("clinicalSignificance", commandOptions.clinicalSignificance);
        queryParams.putIfNotEmpty("populationFrequency", commandOptions.populationFrequency);
        queryParams.putIfNotEmpty("consequenceType", commandOptions.consequenceType);
        queryParams.putIfNotEmpty("study", commandOptions.study);
        if (queryParams.get("study") == null && OpencgaMain.isShellMode()) {
            queryParams.putIfNotEmpty("study", sessionManager.getSession().getCurrentStudy());
        }

        return openCGAClient.getClinicalAnalysisClient().summaryRgaIndividual(queryParams);
    }

    private RestResponse<KnockoutByVariant> queryRgaVariant() throws Exception {
        logger.debug("Executing queryRgaVariant in Analysis - Clinical command line");

        AnalysisClinicalCommandOptions.QueryRgaVariantCommandOptions commandOptions = analysisClinicalCommandOptions.queryRgaVariantCommandOptions;

        ObjectMap queryParams = new ObjectMap();
        queryParams.putIfNotEmpty("include", commandOptions.include);
        queryParams.putIfNotEmpty("exclude", commandOptions.exclude);
        queryParams.putIfNotNull("limit", commandOptions.limit);
        queryParams.putIfNotNull("skip", commandOptions.skip);
        queryParams.putIfNotNull("count", commandOptions.count);
        queryParams.putIfNotEmpty("includeIndividual", commandOptions.includeIndividual);
        queryParams.putIfNotNull("skipIndividual", commandOptions.skipIndividual);
        queryParams.putIfNotNull("limitIndividual", commandOptions.limitIndividual);
        queryParams.putIfNotEmpty("sampleId", commandOptions.sampleId);
        queryParams.putIfNotEmpty("individualId", commandOptions.individualId);
        queryParams.putIfNotEmpty("sex", commandOptions.sex);
        queryParams.putIfNotEmpty("phenotypes", commandOptions.phenotypes);
        queryParams.putIfNotEmpty("disorders", commandOptions.disorders);
        queryParams.putIfNotEmpty("numParents", commandOptions.numParents);
        queryParams.putIfNotEmpty("geneId", commandOptions.geneId);
        queryParams.putIfNotEmpty("geneName", commandOptions.geneName);
        queryParams.putIfNotEmpty("chromosome", commandOptions.chromosome);
        queryParams.putIfNotEmpty("start", commandOptions.start);
        queryParams.putIfNotEmpty("end", commandOptions.end);
        queryParams.putIfNotEmpty("transcriptId", commandOptions.transcriptId);
        queryParams.putIfNotEmpty("variants", commandOptions.variants);
        queryParams.putIfNotEmpty("dbSnps", commandOptions.dbSnps);
        queryParams.putIfNotEmpty("knockoutType", commandOptions.knockoutType);
        queryParams.putIfNotEmpty("filter", commandOptions.filter);
        queryParams.putIfNotEmpty("type", commandOptions.type);
        queryParams.putIfNotEmpty("clinicalSignificance", commandOptions.clinicalSignificance);
        queryParams.putIfNotEmpty("populationFrequency", commandOptions.populationFrequency);
        queryParams.putIfNotEmpty("consequenceType", commandOptions.consequenceType);
        queryParams.putIfNotEmpty("study", commandOptions.study);
        if (queryParams.get("study") == null && OpencgaMain.isShellMode()) {
            queryParams.putIfNotEmpty("study", sessionManager.getSession().getCurrentStudy());
        }

        return openCGAClient.getClinicalAnalysisClient().queryRgaVariant(queryParams);
    }

    private RestResponse<KnockoutByVariantSummary> summaryRgaVariant() throws Exception {
        logger.debug("Executing summaryRgaVariant in Analysis - Clinical command line");

        AnalysisClinicalCommandOptions.SummaryRgaVariantCommandOptions commandOptions = analysisClinicalCommandOptions.summaryRgaVariantCommandOptions;

        ObjectMap queryParams = new ObjectMap();
        queryParams.putIfNotNull("limit", commandOptions.limit);
        queryParams.putIfNotNull("skip", commandOptions.skip);
        queryParams.putIfNotNull("count", commandOptions.count);
        queryParams.putIfNotEmpty("sampleId", commandOptions.sampleId);
        queryParams.putIfNotEmpty("individualId", commandOptions.individualId);
        queryParams.putIfNotEmpty("sex", commandOptions.sex);
        queryParams.putIfNotEmpty("phenotypes", commandOptions.phenotypes);
        queryParams.putIfNotEmpty("disorders", commandOptions.disorders);
        queryParams.putIfNotEmpty("numParents", commandOptions.numParents);
        queryParams.putIfNotEmpty("geneId", commandOptions.geneId);
        queryParams.putIfNotEmpty("geneName", commandOptions.geneName);
        queryParams.putIfNotEmpty("chromosome", commandOptions.chromosome);
        queryParams.putIfNotEmpty("start", commandOptions.start);
        queryParams.putIfNotEmpty("end", commandOptions.end);
        queryParams.putIfNotEmpty("transcriptId", commandOptions.transcriptId);
        queryParams.putIfNotEmpty("variants", commandOptions.variants);
        queryParams.putIfNotEmpty("dbSnps", commandOptions.dbSnps);
        queryParams.putIfNotEmpty("knockoutType", commandOptions.knockoutType);
        queryParams.putIfNotEmpty("filter", commandOptions.filter);
        queryParams.putIfNotEmpty("type", commandOptions.type);
        queryParams.putIfNotEmpty("clinicalSignificance", commandOptions.clinicalSignificance);
        queryParams.putIfNotEmpty("populationFrequency", commandOptions.populationFrequency);
        queryParams.putIfNotEmpty("consequenceType", commandOptions.consequenceType);
        queryParams.putIfNotEmpty("study", commandOptions.study);
        if (queryParams.get("study") == null && OpencgaMain.isShellMode()) {
            queryParams.putIfNotEmpty("study", sessionManager.getSession().getCurrentStudy());
        }

        return openCGAClient.getClinicalAnalysisClient().summaryRgaVariant(queryParams);
    }

    private RestResponse<ClinicalAnalysis> search() throws Exception {
        logger.debug("Executing search in Analysis - Clinical command line");

        AnalysisClinicalCommandOptions.SearchCommandOptions commandOptions = analysisClinicalCommandOptions.searchCommandOptions;

        ObjectMap queryParams = new ObjectMap();
        queryParams.putIfNotEmpty("include", commandOptions.include);
        queryParams.putIfNotEmpty("exclude", commandOptions.exclude);
        queryParams.putIfNotNull("limit", commandOptions.limit);
        queryParams.putIfNotNull("skip", commandOptions.skip);
        queryParams.putIfNotNull("count", commandOptions.count);
        queryParams.putIfNotNull("flattenAnnotations", commandOptions.flattenAnnotations);
        queryParams.putIfNotEmpty("study", commandOptions.study);
        queryParams.putIfNotEmpty("id", commandOptions.id);
        queryParams.putIfNotEmpty("uuid", commandOptions.uuid);
        queryParams.putIfNotEmpty("type", commandOptions.type);
        queryParams.putIfNotEmpty("disorder", commandOptions.disorder);
        queryParams.putIfNotEmpty("files", commandOptions.files);
        queryParams.putIfNotEmpty("sample", commandOptions.sample);
        queryParams.putIfNotEmpty("individual", commandOptions.individual);
        queryParams.putIfNotEmpty("proband", commandOptions.proband);
        queryParams.putIfNotEmpty("probandSamples", commandOptions.probandSamples);
        queryParams.putIfNotEmpty("family", commandOptions.family);
        queryParams.putIfNotEmpty("familyMembers", commandOptions.familyMembers);
        queryParams.putIfNotEmpty("familyMemberSamples", commandOptions.familyMemberSamples);
        queryParams.putIfNotEmpty("panels", commandOptions.panels);
        queryParams.putIfNotNull("locked", commandOptions.locked);
        queryParams.putIfNotEmpty("analystId", commandOptions.analystId);
        queryParams.putIfNotEmpty("priority", commandOptions.priority);
        queryParams.putIfNotEmpty("flags", commandOptions.flags);
        queryParams.putIfNotEmpty("creationDate", commandOptions.creationDate);
        queryParams.putIfNotEmpty("modificationDate", commandOptions.modificationDate);
        queryParams.putIfNotEmpty("dueDate", commandOptions.dueDate);
        queryParams.putIfNotEmpty("qualityControlSummary", commandOptions.qualityControlSummary);
        queryParams.putIfNotEmpty("release", commandOptions.release);
        queryParams.putIfNotNull("snapshot", commandOptions.snapshot);
        queryParams.putIfNotEmpty("status", commandOptions.status);
        queryParams.putIfNotEmpty("internalStatus", commandOptions.internalStatus);
        queryParams.putIfNotEmpty("annotation", commandOptions.annotation);
        queryParams.putIfNotNull("deleted", commandOptions.deleted);
        if (queryParams.get("study") == null && OpencgaMain.isShellMode()) {
            queryParams.putIfNotEmpty("study", sessionManager.getSession().getCurrentStudy());
        }

        return openCGAClient.getClinicalAnalysisClient().search(queryParams);
    }

    private RestResponse<ClinicalVariant> queryVariant() throws Exception {
        logger.debug("Executing queryVariant in Analysis - Clinical command line");

        AnalysisClinicalCommandOptions.QueryVariantCommandOptions commandOptions = analysisClinicalCommandOptions.queryVariantCommandOptions;

        ObjectMap queryParams = new ObjectMap();
        queryParams.putIfNotEmpty("include", commandOptions.include);
        queryParams.putIfNotEmpty("exclude", commandOptions.exclude);
        queryParams.putIfNotNull("limit", commandOptions.limit);
        queryParams.putIfNotNull("skip", commandOptions.skip);
        queryParams.putIfNotNull("count", commandOptions.count);
        queryParams.putIfNotNull("approximateCount", commandOptions.approximateCount);
        queryParams.putIfNotNull("approximateCountSamplingSize", commandOptions.approximateCountSamplingSize);
        queryParams.putIfNotEmpty("savedFilter", commandOptions.savedFilter);
        queryParams.putIfNotEmpty("includeInterpretation", commandOptions.includeInterpretation);
        queryParams.putIfNotEmpty("id", commandOptions.id);
        queryParams.putIfNotEmpty("region", commandOptions.region);
        queryParams.putIfNotEmpty("type", commandOptions.type);
        queryParams.putIfNotEmpty("study", commandOptions.study);
        queryParams.putIfNotEmpty("file", commandOptions.file);
        queryParams.putIfNotEmpty("filter", commandOptions.filter);
        queryParams.putIfNotEmpty("qual", commandOptions.qual);
        queryParams.putIfNotEmpty("fileData", commandOptions.fileData);
        queryParams.putIfNotEmpty("sample", commandOptions.sample);
        queryParams.putIfNotEmpty("sampleData", commandOptions.sampleData);
        queryParams.putIfNotEmpty("sampleAnnotation", commandOptions.sampleAnnotation);
        queryParams.putIfNotEmpty("cohort", commandOptions.cohort);
        queryParams.putIfNotEmpty("cohortStatsRef", commandOptions.cohortStatsRef);
        queryParams.putIfNotEmpty("cohortStatsAlt", commandOptions.cohortStatsAlt);
        queryParams.putIfNotEmpty("cohortStatsMaf", commandOptions.cohortStatsMaf);
        queryParams.putIfNotEmpty("cohortStatsMgf", commandOptions.cohortStatsMgf);
        queryParams.putIfNotEmpty("cohortStatsPass", commandOptions.cohortStatsPass);
        queryParams.putIfNotEmpty("missingAlleles", commandOptions.missingAlleles);
        queryParams.putIfNotEmpty("missingGenotypes", commandOptions.missingGenotypes);
        queryParams.putIfNotEmpty("score", commandOptions.score);
        queryParams.putIfNotEmpty("family", commandOptions.family);
        queryParams.putIfNotEmpty("familyDisorder", commandOptions.familyDisorder);
        queryParams.putIfNotEmpty("familySegregation", commandOptions.familySegregation);
        queryParams.putIfNotEmpty("familyMembers", commandOptions.familyMembers);
        queryParams.putIfNotEmpty("familyProband", commandOptions.familyProband);
        queryParams.putIfNotEmpty("gene", commandOptions.gene);
        queryParams.putIfNotEmpty("ct", commandOptions.ct);
        queryParams.putIfNotEmpty("xref", commandOptions.xref);
        queryParams.putIfNotEmpty("biotype", commandOptions.biotype);
        queryParams.putIfNotEmpty("proteinSubstitution", commandOptions.proteinSubstitution);
        queryParams.putIfNotEmpty("conservation", commandOptions.conservation);
        queryParams.putIfNotEmpty("populationFrequencyAlt", commandOptions.populationFrequencyAlt);
        queryParams.putIfNotEmpty("populationFrequencyRef", commandOptions.populationFrequencyRef);
        queryParams.putIfNotEmpty("populationFrequencyMaf", commandOptions.populationFrequencyMaf);
        queryParams.putIfNotEmpty("transcriptFlag", commandOptions.transcriptFlag);
        queryParams.putIfNotEmpty("geneTraitId", commandOptions.geneTraitId);
        queryParams.putIfNotEmpty("go", commandOptions.go);
        queryParams.putIfNotEmpty("expression", commandOptions.expression);
        queryParams.putIfNotEmpty("proteinKeyword", commandOptions.proteinKeyword);
        queryParams.putIfNotEmpty("drug", commandOptions.drug);
        queryParams.putIfNotEmpty("functionalScore", commandOptions.functionalScore);
        queryParams.putIfNotEmpty("clinical", commandOptions.clinical);
        queryParams.putIfNotEmpty("clinicalSignificance", commandOptions.clinicalSignificance);
        queryParams.putIfNotNull("clinicalConfirmedStatus", commandOptions.clinicalConfirmedStatus);
        queryParams.putIfNotEmpty("customAnnotation", commandOptions.customAnnotation);
        queryParams.putIfNotEmpty("panel", commandOptions.panel);
        queryParams.putIfNotEmpty("panelModeOfInheritance", commandOptions.panelModeOfInheritance);
        queryParams.putIfNotEmpty("panelConfidence", commandOptions.panelConfidence);
        queryParams.putIfNotEmpty("panelRoleInCancer", commandOptions.panelRoleInCancer);
        queryParams.putIfNotEmpty("panelFeatureType", commandOptions.panelFeatureType);
        queryParams.putIfNotNull("panelIntersection", commandOptions.panelIntersection);
        queryParams.putIfNotEmpty("source", commandOptions.source);
        queryParams.putIfNotEmpty("trait", commandOptions.trait);
        if (queryParams.get("study") == null && OpencgaMain.isShellMode()) {
            queryParams.putIfNotEmpty("study", sessionManager.getSession().getCurrentStudy());
        }

        return openCGAClient.getClinicalAnalysisClient().queryVariant(queryParams);
    }

    private RestResponse<ClinicalAnalysisAclEntryList> acl() throws Exception {
        logger.debug("Executing acl in Analysis - Clinical command line");

        AnalysisClinicalCommandOptions.AclCommandOptions commandOptions = analysisClinicalCommandOptions.aclCommandOptions;

        ObjectMap queryParams = new ObjectMap();
        queryParams.putIfNotEmpty("study", commandOptions.study);
        queryParams.putIfNotEmpty("member", commandOptions.member);
        queryParams.putIfNotNull("silent", commandOptions.silent);
        if (queryParams.get("study") == null && OpencgaMain.isShellMode()) {
            queryParams.putIfNotEmpty("study", sessionManager.getSession().getCurrentStudy());
        }

        return openCGAClient.getClinicalAnalysisClient().acl(commandOptions.clinicalAnalyses, queryParams);
    }

    private RestResponse<ClinicalAnalysis> delete() throws Exception {
        logger.debug("Executing delete in Analysis - Clinical command line");

        AnalysisClinicalCommandOptions.DeleteCommandOptions commandOptions = analysisClinicalCommandOptions.deleteCommandOptions;

        ObjectMap queryParams = new ObjectMap();
        queryParams.putIfNotEmpty("study", commandOptions.study);
        queryParams.putIfNotNull("force", commandOptions.force);
        if (queryParams.get("study") == null && OpencgaMain.isShellMode()) {
            queryParams.putIfNotEmpty("study", sessionManager.getSession().getCurrentStudy());
        }

        return openCGAClient.getClinicalAnalysisClient().delete(commandOptions.clinicalAnalyses, queryParams);
    }

    private RestResponse<ClinicalAnalysis> update() throws Exception {
        logger.debug("Executing update in Analysis - Clinical command line");

        AnalysisClinicalCommandOptions.UpdateCommandOptions commandOptions = analysisClinicalCommandOptions.updateCommandOptions;

        ObjectMap queryParams = new ObjectMap();
        queryParams.putIfNotEmpty("include", commandOptions.include);
        queryParams.putIfNotEmpty("exclude", commandOptions.exclude);
        queryParams.putIfNotEmpty("study", commandOptions.study);
        queryParams.putIfNotNull("analystsAction", commandOptions.analystsAction);
        queryParams.putIfNotNull("reportedFilesAction", commandOptions.reportedFilesAction);
        queryParams.putIfNotNull("annotationSetsAction", commandOptions.annotationSetsAction);
        queryParams.putIfNotNull("includeResult", commandOptions.includeResult);
        if (queryParams.get("study") == null && OpencgaMain.isShellMode()) {
            queryParams.putIfNotEmpty("study", sessionManager.getSession().getCurrentStudy());
        }


        ClinicalAnalysisUpdateParams clinicalAnalysisUpdateParams = null;
        if (commandOptions.jsonDataModel) {
            RestResponse<ClinicalAnalysis> res = new RestResponse<>();
            res.setType(QueryType.VOID);
            PrintUtils.println(getObjectAsJSON(categoryName,"/{apiVersion}/analysis/clinical/{clinicalAnalyses}/update"));
            return res;
        } else if (commandOptions.jsonFile != null) {
            clinicalAnalysisUpdateParams = JacksonUtils.getDefaultObjectMapper()
                    .readValue(new java.io.File(commandOptions.jsonFile), ClinicalAnalysisUpdateParams.class);
        } else {
            ObjectMap beanParams = new ObjectMap();
            putNestedIfNotEmpty(beanParams, "id", commandOptions.id, true);
            putNestedIfNotEmpty(beanParams, "description", commandOptions.description, true);
            putNestedIfNotNull(beanParams, "type", commandOptions.type, true);
            putNestedIfNotEmpty(beanParams, "disorder.id", commandOptions.disorderId, true);
            putNestedIfNotNull(beanParams, "panelLocked", commandOptions.panelLocked, true);
            putNestedIfNotEmpty(beanParams, "proband.id", commandOptions.probandId, true);
            putNestedIfNotEmpty(beanParams, "family.id", commandOptions.familyId, true);
            putNestedIfNotNull(beanParams, "locked", commandOptions.locked, true);
            putNestedIfNotEmpty(beanParams, "analyst.id", commandOptions.analystId, true);
            putNestedIfNotEmpty(beanParams, "report.overview", commandOptions.reportOverview, true);
            putNestedIfNotEmpty(beanParams, "report.recommendation", commandOptions.reportRecommendation, true);
            putNestedIfNotEmpty(beanParams, "report.methodology", commandOptions.reportMethodology, true);
            putNestedIfNotEmpty(beanParams, "report.limitations", commandOptions.reportLimitations, true);
            putNestedIfNotEmpty(beanParams, "report.experimentalProcedure", commandOptions.reportExperimentalProcedure, true);
            putNestedIfNotEmpty(beanParams, "report.date", commandOptions.reportDate, true);
            putNestedIfNotNull(beanParams, "report.images", commandOptions.reportImages, true);
            putNestedMapIfNotEmpty(beanParams, "report.attributes", commandOptions.reportAttributes, true);
            putNestedIfNotEmpty(beanParams, "request.id", commandOptions.requestId, true);
            putNestedIfNotEmpty(beanParams, "request.justification", commandOptions.requestJustification, true);
            putNestedIfNotEmpty(beanParams, "request.date", commandOptions.requestDate, true);
            putNestedMapIfNotEmpty(beanParams, "request.attributes", commandOptions.requestAttributes, true);
            putNestedIfNotEmpty(beanParams, "responsible.id", commandOptions.responsibleId, true);
            putNestedIfNotEmpty(beanParams, "responsible.name", commandOptions.responsibleName, true);
            putNestedIfNotEmpty(beanParams, "responsible.email", commandOptions.responsibleEmail, true);
            putNestedIfNotEmpty(beanParams, "responsible.organization", commandOptions.responsibleOrganization, true);
            putNestedIfNotEmpty(beanParams, "responsible.department", commandOptions.responsibleDepartment, true);
            putNestedIfNotEmpty(beanParams, "responsible.address", commandOptions.responsibleAddress, true);
            putNestedIfNotEmpty(beanParams, "responsible.city", commandOptions.responsibleCity, true);
            putNestedIfNotEmpty(beanParams, "responsible.postcode", commandOptions.responsiblePostcode, true);
            putNestedIfNotNull(beanParams, "qualityControl.summary", commandOptions.qualityControlSummary, true);
            putNestedIfNotNull(beanParams, "qualityControl.comments", commandOptions.qualityControlComments, true);
            putNestedIfNotNull(beanParams, "qualityControl.files", commandOptions.qualityControlFiles, true);
            putNestedIfNotEmpty(beanParams, "creationDate", commandOptions.creationDate, true);
            putNestedIfNotEmpty(beanParams, "modificationDate", commandOptions.modificationDate, true);
            putNestedIfNotEmpty(beanParams, "dueDate", commandOptions.dueDate, true);
            putNestedIfNotEmpty(beanParams, "priority.id", commandOptions.priorityId, true);
            putNestedMapIfNotEmpty(beanParams, "attributes", commandOptions.attributes, true);
            putNestedIfNotEmpty(beanParams, "status.id", commandOptions.statusId, true);
            putNestedIfNotNull(beanParams, "panelLock", commandOptions.panelLock, true);

            clinicalAnalysisUpdateParams = JacksonUtils.getDefaultObjectMapper().copy()
                    .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, true)
                    .readValue(beanParams.toJson(), ClinicalAnalysisUpdateParams.class);
        }
        return openCGAClient.getClinicalAnalysisClient().update(commandOptions.clinicalAnalyses, clinicalAnalysisUpdateParams, queryParams);
    }

    private RestResponse<Sample> updateAnnotationSetsAnnotations() throws Exception {
        logger.debug("Executing updateAnnotationSetsAnnotations in Analysis - Clinical command line");

        AnalysisClinicalCommandOptions.UpdateAnnotationSetsAnnotationsCommandOptions commandOptions = analysisClinicalCommandOptions.updateAnnotationSetsAnnotationsCommandOptions;

        ObjectMap queryParams = new ObjectMap();
        queryParams.putIfNotEmpty("study", commandOptions.study);
        queryParams.putIfNotNull("action", commandOptions.action);
        if (queryParams.get("study") == null && OpencgaMain.isShellMode()) {
            queryParams.putIfNotEmpty("study", sessionManager.getSession().getCurrentStudy());
        }


        ObjectMap objectMap = null;
        if (commandOptions.jsonDataModel) {
            RestResponse<Sample> res = new RestResponse<>();
            res.setType(QueryType.VOID);
            PrintUtils.println(getObjectAsJSON(categoryName,"/{apiVersion}/analysis/clinical/{clinicalAnalysis}/annotationSets/{annotationSet}/annotations/update"));
            return res;
        } else if (commandOptions.jsonFile != null) {
            objectMap = JacksonUtils.getDefaultObjectMapper()
                    .readValue(new java.io.File(commandOptions.jsonFile), ObjectMap.class);
        }
        return openCGAClient.getClinicalAnalysisClient().updateAnnotationSetsAnnotations(commandOptions.clinicalAnalysis, commandOptions.annotationSet, objectMap, queryParams);
    }

    private RestResponse<ClinicalAnalysis> info() throws Exception {
        logger.debug("Executing info in Analysis - Clinical command line");

        AnalysisClinicalCommandOptions.InfoCommandOptions commandOptions = analysisClinicalCommandOptions.infoCommandOptions;

        ObjectMap queryParams = new ObjectMap();
        queryParams.putIfNotEmpty("include", commandOptions.include);
        queryParams.putIfNotEmpty("exclude", commandOptions.exclude);
        queryParams.putIfNotNull("flattenAnnotations", commandOptions.flattenAnnotations);
        queryParams.putIfNotEmpty("study", commandOptions.study);
        queryParams.putIfNotEmpty("version", commandOptions.version);
        queryParams.putIfNotNull("deleted", commandOptions.deleted);
        if (queryParams.get("study") == null && OpencgaMain.isShellMode()) {
            queryParams.putIfNotEmpty("study", sessionManager.getSession().getCurrentStudy());
        }

        return openCGAClient.getClinicalAnalysisClient().info(commandOptions.clinicalAnalysis, queryParams);
    }

    private RestResponse<Interpretation> createInterpretation() throws Exception {
        logger.debug("Executing createInterpretation in Analysis - Clinical command line");

        AnalysisClinicalCommandOptions.CreateInterpretationCommandOptions commandOptions = analysisClinicalCommandOptions.createInterpretationCommandOptions;

        ObjectMap queryParams = new ObjectMap();
        queryParams.putIfNotEmpty("include", commandOptions.include);
        queryParams.putIfNotEmpty("exclude", commandOptions.exclude);
        queryParams.putIfNotEmpty("study", commandOptions.study);
        queryParams.putIfNotNull("setAs", commandOptions.setAs);
        queryParams.putIfNotNull("includeResult", commandOptions.includeResult);
        if (queryParams.get("study") == null && OpencgaMain.isShellMode()) {
            queryParams.putIfNotEmpty("study", sessionManager.getSession().getCurrentStudy());
        }


        InterpretationCreateParams interpretationCreateParams = null;
        if (commandOptions.jsonDataModel) {
            RestResponse<Interpretation> res = new RestResponse<>();
            res.setType(QueryType.VOID);
            PrintUtils.println(getObjectAsJSON(categoryName,"/{apiVersion}/analysis/clinical/{clinicalAnalysis}/interpretation/create"));
            return res;
        } else if (commandOptions.jsonFile != null) {
            interpretationCreateParams = JacksonUtils.getDefaultObjectMapper()
                    .readValue(new java.io.File(commandOptions.jsonFile), InterpretationCreateParams.class);
        } else {
            ObjectMap beanParams = new ObjectMap();
            putNestedIfNotEmpty(beanParams, "name", commandOptions.name, true);
            putNestedIfNotEmpty(beanParams, "description", commandOptions.description, true);
            putNestedIfNotEmpty(beanParams, "clinicalAnalysisId", commandOptions.clinicalAnalysisId, true);
            putNestedIfNotEmpty(beanParams, "creationDate", commandOptions.creationDate, true);
            putNestedIfNotEmpty(beanParams, "modificationDate", commandOptions.modificationDate, true);
            putNestedIfNotEmpty(beanParams, "analyst.id", commandOptions.analystId, true);
            putNestedIfNotEmpty(beanParams, "method.name", commandOptions.methodName, true);
            putNestedIfNotEmpty(beanParams, "method.version", commandOptions.methodVersion, true);
            putNestedIfNotEmpty(beanParams, "method.commit", commandOptions.methodCommit, true);
            putNestedIfNotNull(beanParams, "locked", commandOptions.locked, true);
            putNestedIfNotEmpty(beanParams, "status.id", commandOptions.statusId, true);
            putNestedMapIfNotEmpty(beanParams, "attributes", commandOptions.attributes, true);

            interpretationCreateParams = JacksonUtils.getDefaultObjectMapper().copy()
                    .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, true)
                    .readValue(beanParams.toJson(), InterpretationCreateParams.class);
        }
        return openCGAClient.getClinicalAnalysisClient().createInterpretation(commandOptions.clinicalAnalysis, interpretationCreateParams, queryParams);
    }

    private RestResponse<Interpretation> clearInterpretation() throws Exception {
        logger.debug("Executing clearInterpretation in Analysis - Clinical command line");

        AnalysisClinicalCommandOptions.ClearInterpretationCommandOptions commandOptions = analysisClinicalCommandOptions.clearInterpretationCommandOptions;

        ObjectMap queryParams = new ObjectMap();
        queryParams.putIfNotEmpty("study", commandOptions.study);
        if (queryParams.get("study") == null && OpencgaMain.isShellMode()) {
            queryParams.putIfNotEmpty("study", sessionManager.getSession().getCurrentStudy());
        }

        return openCGAClient.getClinicalAnalysisClient().clearInterpretation(commandOptions.clinicalAnalysis, commandOptions.interpretations, queryParams);
    }

    private RestResponse<Interpretation> deleteInterpretation() throws Exception {
        logger.debug("Executing deleteInterpretation in Analysis - Clinical command line");

        AnalysisClinicalCommandOptions.DeleteInterpretationCommandOptions commandOptions = analysisClinicalCommandOptions.deleteInterpretationCommandOptions;

        ObjectMap queryParams = new ObjectMap();
        queryParams.putIfNotEmpty("study", commandOptions.study);
        queryParams.putIfNotEmpty("setAsPrimary", commandOptions.setAsPrimary);
        if (queryParams.get("study") == null && OpencgaMain.isShellMode()) {
            queryParams.putIfNotEmpty("study", sessionManager.getSession().getCurrentStudy());
        }

        return openCGAClient.getClinicalAnalysisClient().deleteInterpretation(commandOptions.clinicalAnalysis, commandOptions.interpretations, queryParams);
    }

    private RestResponse<Interpretation> revertInterpretation() throws Exception {
        logger.debug("Executing revertInterpretation in Analysis - Clinical command line");

        AnalysisClinicalCommandOptions.RevertInterpretationCommandOptions commandOptions = analysisClinicalCommandOptions.revertInterpretationCommandOptions;

        ObjectMap queryParams = new ObjectMap();
        queryParams.putIfNotEmpty("study", commandOptions.study);
        if (queryParams.get("study") == null && OpencgaMain.isShellMode()) {
            queryParams.putIfNotEmpty("study", sessionManager.getSession().getCurrentStudy());
        }

        return openCGAClient.getClinicalAnalysisClient().revertInterpretation(commandOptions.clinicalAnalysis, commandOptions.interpretation, commandOptions.version, queryParams);
    }

    private RestResponse<Interpretation> updateInterpretation() throws Exception {
        logger.debug("Executing updateInterpretation in Analysis - Clinical command line");

        AnalysisClinicalCommandOptions.UpdateInterpretationCommandOptions commandOptions = analysisClinicalCommandOptions.updateInterpretationCommandOptions;

        ObjectMap queryParams = new ObjectMap();
        queryParams.putIfNotEmpty("include", commandOptions.include);
        queryParams.putIfNotEmpty("exclude", commandOptions.exclude);
        queryParams.putIfNotEmpty("study", commandOptions.study);
        queryParams.putIfNotNull("setAs", commandOptions.setAs);
        queryParams.putIfNotNull("includeResult", commandOptions.includeResult);
        if (queryParams.get("study") == null && OpencgaMain.isShellMode()) {
            queryParams.putIfNotEmpty("study", sessionManager.getSession().getCurrentStudy());
        }


        InterpretationUpdateParams interpretationUpdateParams = null;
        if (commandOptions.jsonDataModel) {
            RestResponse<Interpretation> res = new RestResponse<>();
            res.setType(QueryType.VOID);
            PrintUtils.println(getObjectAsJSON(categoryName,"/{apiVersion}/analysis/clinical/{clinicalAnalysis}/interpretation/{interpretation}/update"));
            return res;
        } else if (commandOptions.jsonFile != null) {
            interpretationUpdateParams = JacksonUtils.getDefaultObjectMapper()
                    .readValue(new java.io.File(commandOptions.jsonFile), InterpretationUpdateParams.class);
        } else {
            ObjectMap beanParams = new ObjectMap();
            putNestedIfNotEmpty(beanParams, "name", commandOptions.name, true);
            putNestedIfNotEmpty(beanParams, "description", commandOptions.description, true);
            putNestedIfNotEmpty(beanParams, "analyst.id", commandOptions.analystId, true);
            putNestedIfNotEmpty(beanParams, "method.name", commandOptions.methodName, true);
            putNestedIfNotEmpty(beanParams, "method.version", commandOptions.methodVersion, true);
            putNestedIfNotEmpty(beanParams, "method.commit", commandOptions.methodCommit, true);
            putNestedIfNotEmpty(beanParams, "creationDate", commandOptions.creationDate, true);
            putNestedIfNotEmpty(beanParams, "modificationDate", commandOptions.modificationDate, true);
            putNestedIfNotEmpty(beanParams, "status.id", commandOptions.statusId, true);
            putNestedIfNotNull(beanParams, "locked", commandOptions.locked, true);
            putNestedMapIfNotEmpty(beanParams, "attributes", commandOptions.attributes, true);

            interpretationUpdateParams = JacksonUtils.getDefaultObjectMapper().copy()
                    .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, true)
                    .readValue(beanParams.toJson(), InterpretationUpdateParams.class);
        }
        return openCGAClient.getClinicalAnalysisClient().updateInterpretation(commandOptions.clinicalAnalysis, commandOptions.interpretation, interpretationUpdateParams, queryParams);
    }

    private RestResponse<ClinicalReport> updateReport() throws Exception {
        logger.debug("Executing updateReport in Analysis - Clinical command line");

        AnalysisClinicalCommandOptions.UpdateReportCommandOptions commandOptions = analysisClinicalCommandOptions.updateReportCommandOptions;

        ObjectMap queryParams = new ObjectMap();
        queryParams.putIfNotEmpty("include", commandOptions.include);
        queryParams.putIfNotEmpty("exclude", commandOptions.exclude);
        queryParams.putIfNotEmpty("study", commandOptions.study);
        queryParams.putIfNotNull("signaturesAction", commandOptions.signaturesAction);
        queryParams.putIfNotNull("referencesAction", commandOptions.referencesAction);
        queryParams.putIfNotNull("includeResult", commandOptions.includeResult);
        if (queryParams.get("study") == null && OpencgaMain.isShellMode()) {
            queryParams.putIfNotEmpty("study", sessionManager.getSession().getCurrentStudy());
        }


        ClinicalReport clinicalReport = null;
        if (commandOptions.jsonDataModel) {
            RestResponse<ClinicalReport> res = new RestResponse<>();
            res.setType(QueryType.VOID);
            PrintUtils.println(getObjectAsJSON(categoryName,"/{apiVersion}/analysis/clinical/{clinicalAnalysis}/report/update"));
            return res;
        } else if (commandOptions.jsonFile != null) {
            clinicalReport = JacksonUtils.getDefaultObjectMapper()
                    .readValue(new java.io.File(commandOptions.jsonFile), ClinicalReport.class);
        } else {
            ObjectMap beanParams = new ObjectMap();
            putNestedIfNotEmpty(beanParams, "overview", commandOptions.overview, true);
            putNestedIfNotEmpty(beanParams, "discussion.author", commandOptions.discussionAuthor, true);
            putNestedIfNotEmpty(beanParams, "discussion.date", commandOptions.discussionDate, true);
            putNestedIfNotEmpty(beanParams, "discussion.text", commandOptions.discussionText, true);
            putNestedIfNotEmpty(beanParams, "recommendation", commandOptions.recommendation, true);
            putNestedIfNotEmpty(beanParams, "methodology", commandOptions.methodology, true);
            putNestedIfNotEmpty(beanParams, "limitations", commandOptions.limitations, true);
            putNestedIfNotEmpty(beanParams, "experimentalProcedure", commandOptions.experimentalProcedure, true);
            putNestedIfNotEmpty(beanParams, "conclusion.author", commandOptions.conclusionAuthor, true);
            putNestedIfNotEmpty(beanParams, "conclusion.date", commandOptions.conclusionDate, true);
            putNestedIfNotEmpty(beanParams, "conclusion.text", commandOptions.conclusionText, true);
            putNestedIfNotEmpty(beanParams, "date", commandOptions.date, true);
            putNestedIfNotNull(beanParams, "images", commandOptions.images, true);
            putNestedMapIfNotEmpty(beanParams, "attributes", commandOptions.attributes, true);

            clinicalReport = JacksonUtils.getDefaultObjectMapper().copy()
                    .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, true)
                    .readValue(beanParams.toJson(), ClinicalReport.class);
        }
        return openCGAClient.getClinicalAnalysisClient().updateReport(commandOptions.clinicalAnalysis, clinicalReport, queryParams);
    }
}