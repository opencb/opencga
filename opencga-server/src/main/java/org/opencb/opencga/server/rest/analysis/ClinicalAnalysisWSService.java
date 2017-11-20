package org.opencb.opencga.server.rest.analysis;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import org.opencb.commons.datastore.core.QueryResponse;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.core.exception.VersionException;
import org.opencb.opencga.core.models.Job;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.*;
import javax.ws.rs.core.*;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Path("/{version}/analysis/clinical")
@Produces(MediaType.APPLICATION_JSON)
@Api(value = "Analysis - Clinical", position = 4, description = "Methods for working with Clinical Analysis")
public class ClinicalAnalysisWSService extends AnalysisWSService {

    public ClinicalAnalysisWSService(@Context UriInfo uriInfo, @Context HttpServletRequest httpServletRequest, @Context HttpHeaders httpHeaders)
            throws IOException, VersionException {
        super(uriInfo, httpServletRequest, httpHeaders);
    }

    public ClinicalAnalysisWSService(String version, @Context UriInfo uriInfo, @Context HttpServletRequest httpServletRequest, @Context HttpHeaders httpHeaders)
            throws IOException, VersionException {
        super(version, uriInfo, httpServletRequest, httpHeaders);
    }

    @GET
    @Path("/query")
    @ApiOperation(value = "TEAM interpretation analysis", position = 14, response = QueryResponse.class)
    public Response query(@ApiParam(value = "Comma separated list of interpretation IDs") @QueryParam(value = "interpretation") String fileIdStrOld,
                         @ApiParam(value = "Clinical analysis ID") @QueryParam("clinicalAnalysis") String clinicalAnalysis,
                         @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias") @QueryParam("study") String studyStr) {

        return Response.ok().build();
    }

    @GET
    @Path("/prioritization")
    @ApiOperation(value = "Prioritization analysis", position = 14, response = QueryResponse.class)
    public Response prioritization(@ApiParam(value = "Clinical Analysis Id") @QueryParam("clinicalAnalysisId") String clinicalAnalysisId,
                                   @ApiParam(value = "List of variant ids") @QueryParam("ids") String ids,
                                   @ApiParam(value = "List of regions: {chr}:{start}-{end}") @QueryParam("region") String region,
                                   @ApiParam(value = "List of chromosomes") @QueryParam("chromosome") String chromosome,
                                   @ApiParam(value = "List of genes") @QueryParam("gene") String gene,
                                   @ApiParam(value = "Variant type: [SNV, MNV, INDEL, SV, CNV]") @QueryParam("type") String type,
                                   @ApiParam(value = "Reference allele") @QueryParam("reference") String reference,
                                   @ApiParam(value = "Main alternate allele") @QueryParam("alternate") String alternate,
                                   @ApiParam(value = "", required = true) @QueryParam("study") String studyStr,
                                   @ApiParam(value = "", required = true) @QueryParam("outDir") String outDir,
                                   @ApiParam(value = "Description of prioritization") @QueryParam("description") String description,
                                   @ApiParam(value = "List of studies to be returned") @QueryParam("returnedStudies") String returnedStudies,
                                   @ApiParam(value = "List of samples to be returned") @QueryParam("returnedSamples") String returnedSamples,
                                   @ApiParam(value = "List of files to be returned.") @QueryParam("returnedFiles") String returnedFiles,
                                   @ApiParam(value = "Variants in specific files") @QueryParam("files") String files,
                                   @ApiParam(value = "Minor Allele Frequency: [{study:}]{cohort}[<|>|<=|>=]{number}") @QueryParam("maf") String maf,
                                   @ApiParam(value = "Minor Genotype Frequency: [{study:}]{cohort}[<|>|<=|>=]{number}") @QueryParam("mgf") String mgf,
                                   @ApiParam(value = "Number of missing alleles: [{study:}]{cohort}[<|>|<=|>=]{number}") @QueryParam("missingAlleles") String missingAlleles,
                                   @ApiParam(value = "Number of missing genotypes: [{study:}]{cohort}[<|>|<=|>=]{number}") @QueryParam("missingGenotypes") String missingGenotypes,
                                   @ApiParam(value = "Specify if the variant annotation must exists.") @QueryParam("annotationExists") boolean annotationExists,
                                   @ApiParam(value = "Samples with a specific genotype: {samp_1}:{gt_1}(,{gt_n})*(;{samp_n}:{gt_1}(,{gt_n})*)* e.g. HG0097:0/0;HG0098:0/1,1/1") @QueryParam("genotype") String genotype,
                                   @ApiParam(value = "List of FORMAT names from Samples Data to include in the output. e.g: DP,AD. Accepts") @QueryParam("include-format") String includeFormat,
                                   @ApiParam(value = "Include genotypes, apart of other formats defined with include-format") @QueryParam("include-genotype") String includeGenotype,
                                   @ApiParam(value = "Selects some samples using metadata information from Catalog. e.g. age>20;ontologies=hpo:123,hpo:456;name=smith") @QueryParam("sampleFilter") String sampleFilter,
                                   @ApiParam(value = "Consequence type SO term list. e.g. missense_variant,stop_lost or SO:0001583,SO:0001578") @QueryParam("annot-ct") String annot_ct,
                                   @ApiParam(value = "XRef") @QueryParam("annot-xref") String annot_xref,
                                   @ApiParam(value = "Biotype") @QueryParam("annot-biotype") String annot_biotype,
                                   @ApiParam(value = "Polyphen, protein substitution score. [<|>|<=|>=]{number} or [~=|=|]{description} e.g. <=0.9 , =benign") @QueryParam("polyphen") String polyphen,
                                   @ApiParam(value = "Sift, protein substitution score. [<|>|<=|>=]{number} or [~=|=|]{description} e.g. >0.1 , ~=tolerant") @QueryParam("sift") String sift,
                                   @ApiParam(value = "Conservation score: {conservation_score}[<|>|<=|>=]{number} e.g. phastCons>0.5,phylop<0.1,gerp>0.1") @QueryParam("conservation") String conservation,
                                   @ApiParam(value = "Population minor allele frequency: {study}:{population}[<|>|<=|>=]{number}") @QueryParam("annot-population-maf") String annotPopulationMaf,
                                   @ApiParam(value = "Alternate Population Frequency: {study}:{population}[<|>|<=|>=]{number}") @QueryParam("alternate_frequency") String alternate_frequency,
                                   @ApiParam(value = "Reference Population Frequency: {study}:{population}[<|>|<=|>=]{number}") @QueryParam("reference_frequency") String reference_frequency,
                                   @ApiParam(value = "List of transcript annotation flags. e.g. CCDS, basic, cds_end_NF, mRNA_end_NF, cds_start_NF, mRNA_start_NF, seleno") @QueryParam("annot-transcription-flags") String transcriptionFlags,
                                   @ApiParam(value = "List of gene trait association id. e.g. \"umls:C0007222\" , \"OMIM:269600\"") @QueryParam("annot-gene-trait-id") String geneTraitId,
                                   @ApiParam(value = "List of gene trait association names. e.g. \"Cardiovascular Diseases\"") @QueryParam("annot-gene-trait-name") String geneTraitName,
                                   @ApiParam(value = "List of HPO terms. e.g. \"HP:0000545\"") @QueryParam("annot-hpo") String hpo,
                                   @ApiParam(value = "List of GO (Genome Ontology) terms. e.g. \"GO:0002020\"") @QueryParam("annot-go") String go,
                                   @ApiParam(value = "List of tissues of interest. e.g. \"tongue\"") @QueryParam("annot-expression") String expression,
                                   @ApiParam(value = "List of protein variant annotation keywords") @QueryParam("annot-protein-keywords") String proteinKeyword,
                                   @ApiParam(value = "List of drug names") @QueryParam("annot-drug") String drug,
                                   @ApiParam(value = "Perform a full-text search on a list of traits") @QueryParam("traits") String traits,
                                   @ApiParam(value = "Functional score: {functional_score}[<|>|<=|>=]{number} e.g. cadd_scaled>5.2 , cadd_raw<=0.3") @QueryParam("annot-functional-score") String functional,

                                   @ApiParam(value = "Returned genotype for unknown genotypes. Common values: [0/0, 0|0, ./.]") @QueryParam("unknownGenotype") String unknownGenotype,
                                   @ApiParam(value = "Returns the samples metadata group by study. Sample names will appear in the same order as their corresponding genotypes.", required = false) @QueryParam("samplesMetadata") boolean samplesMetadata,
                                   @ApiParam(value = "Sort the results", required = false) @QueryParam("sort") boolean sort,
                                   @ApiParam(value = "Group variants by: [ct, gene, ensemblGene]", required = false) @DefaultValue("") @QueryParam("groupBy") String groupBy,
                                   @ApiParam(value = "Calculate histogram. Requires one region.", required = false) @DefaultValue("false") @QueryParam("histogram") boolean histogram,
                                   @ApiParam(value = "Histogram interval size", required = false) @DefaultValue("2000") @QueryParam("interval") int interval,
                                   @ApiParam(value = "Fetch summary data from Solr", required = false) @QueryParam("summary") boolean summary,
                                   @ApiParam(value = "Merge results", required = false) @DefaultValue("false") @QueryParam("merge") boolean merge) {

        Map<String,String> objectMap = new HashMap<>();
        for (Map.Entry<String, List<String>> e : uriInfo.getQueryParameters().entrySet()) {
            objectMap.put(e.getKey(), String.valueOf(e.getValue()));
        }

        try {
            QueryResult<Job> queryResult = catalogManager.getJobManager()
                    .create(studyStr, "prioritization", description, "opencga-analysis", "clinical prioritization", outDir, objectMap, sessionId);
            return createOkResponse(queryResult);
        } catch (CatalogException e) {
            return createErrorResponse(e);
        }

    }

    @GET
    @Path("/tiering")
    @ApiOperation(value = "GEL Tiering interpretation analysis", position = 14, response = QueryResponse.class)
    public Response tiering(@ApiParam(value = "(DEPRECATED) Comma separated list of file ids (files or directories)", hidden = true)
                         @QueryParam(value = "fileId") String fileIdStrOld,
                         @ApiParam(value = "Comma separated list of file ids (files or directories)", required = true)
                         @QueryParam(value = "file") String fileIdStr,
                         // Study id is not ingested by the analysis index command line. No longer needed.
                         @ApiParam(value = "(DEPRECATED) Study id", hidden = true) @QueryParam("studyId") String studyStrOld,
                         @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias")
                         @QueryParam("study") String studyStr) {

        return Response.ok().build();
    }

    @GET
    @Path("/team")
    @ApiOperation(value = "TEAM interpretation analysis", position = 14, response = QueryResponse.class)
    public Response team(@ApiParam(value = "(DEPRECATED) Comma separated list of file ids (files or directories)", hidden = true)
                          @QueryParam(value = "fileId") String fileIdStrOld,
                          @ApiParam(value = "Comma separated list of file ids (files or directories)", required = true)
                          @QueryParam(value = "file") String fileIdStr,
                          // Study id is not ingested by the analysis index command line. No longer needed.
                          @ApiParam(value = "(DEPRECATED) Study id", hidden = true) @QueryParam("studyId") String studyStrOld,
                          @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias")
                          @QueryParam("study") String studyStr) {

        return Response.ok().build();
    }

}
