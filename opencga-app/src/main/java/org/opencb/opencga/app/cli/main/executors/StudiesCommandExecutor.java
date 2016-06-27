/*
 * Copyright 2015 OpenCB
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

package org.opencb.opencga.app.cli.main.executors;


import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.models.alignment.Alignment;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResponse;
import org.opencb.opencga.analysis.storage.variant.CatalogVariantDBAdaptor;
import org.opencb.opencga.app.cli.main.OpencgaCommandExecutor;
import org.opencb.opencga.app.cli.main.options.StudyCommandOptions;
import org.opencb.opencga.catalog.db.api.CatalogFileDBAdaptor;
import org.opencb.opencga.catalog.db.api.CatalogJobDBAdaptor;
import org.opencb.opencga.catalog.db.api.CatalogSampleDBAdaptor;
import org.opencb.opencga.catalog.db.api.CatalogStudyDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.models.File;
import org.opencb.opencga.catalog.models.Job;
import org.opencb.opencga.catalog.models.Sample;
import org.opencb.opencga.catalog.models.Study;
import org.opencb.opencga.catalog.models.summaries.StudySummary;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;

import java.io.IOException;

/**
 * Created by imedina on 03/06/16.
 */
public class StudiesCommandExecutor extends OpencgaCommandExecutor {

    private StudyCommandOptions studiesCommandOptions;

    public StudiesCommandExecutor(StudyCommandOptions studiesCommandOptions) {
        super(studiesCommandOptions.commonCommandOptions);
        this.studiesCommandOptions = studiesCommandOptions;
    }


    @Override
    public void execute() throws Exception {
        logger.debug("Executing studies command line");

        String subCommandString = getParsedSubCommand(studiesCommandOptions.jCommander);
        switch (subCommandString) {
            case "create":
                create();
                break;
            case "info":
                info();
                break;
            case "update":
                update();
                break;
            case "delete":
                delete();
                break;
            case "summary":
                summary();
                break;
            case "search":
                search();
                break;
            case "scan-files":
                scanFiles();
                break;
            case "files":
                files();
                break;
            case "alignments":
                alignments();
                break;
            case "samples":
                samples();
                break;
            case "jobs":
                jobs();
                break;
            case "variants":
                variants();
                break;

            default:
                logger.error("Subcommand not valid");
                break;
        }

    }

    /**********************************************  Administration Commands  ***********************************************/

    private void create() throws CatalogException, IOException {

        logger.debug("Creating a new study");
        String alias = studiesCommandOptions.createCommandOptions.alias;
        String name = studiesCommandOptions.createCommandOptions.name;
        String projectId = studiesCommandOptions.createCommandOptions.projectId;
        String description = studiesCommandOptions.createCommandOptions.description;
        String status = studiesCommandOptions.createCommandOptions.status;

        ObjectMap o = new ObjectMap();
        o.append(CatalogStudyDBAdaptor.QueryParams.DESCRIPTION.key(),description);
        o.append(CatalogStudyDBAdaptor.QueryParams.STATUS_STATUS.key(),status);
        openCGAClient.getStudyClient().create(projectId, name, alias, o);
        System.out.println("Done.");
    }

    private void info() throws CatalogException, IOException {

        logger.debug("Getting the project info");
        QueryResponse<Study> info = openCGAClient.getStudyClient().get(studiesCommandOptions.infoCommandOptions.id, null);
        System.out.println("Study: " + info);
    }

    private void update() throws CatalogException, IOException {

        logger.debug("Updating the study");

        ObjectMap objectMap = new ObjectMap();

        if (StringUtils.isNotEmpty(studiesCommandOptions.updateCommandOptions.name)) {
            objectMap.put(CatalogFileDBAdaptor.QueryParams.NAME.key(), studiesCommandOptions.updateCommandOptions.name);
        }
        if (StringUtils.isNotEmpty(studiesCommandOptions.updateCommandOptions.type)) {
            objectMap.put(CatalogFileDBAdaptor.QueryParams.TYPE.key(), studiesCommandOptions.updateCommandOptions.type);
        }
        if (StringUtils.isNotEmpty(studiesCommandOptions.updateCommandOptions.description)) {
            objectMap.put(CatalogFileDBAdaptor.QueryParams.DESCRIPTION.key(), studiesCommandOptions.updateCommandOptions.description);
        }
        if (StringUtils.isNotEmpty(studiesCommandOptions.updateCommandOptions.status)) {
            objectMap.put(CatalogFileDBAdaptor.QueryParams.DISK_USAGE.key(), studiesCommandOptions.updateCommandOptions.status);
        }

        QueryResponse<Study> study = openCGAClient.getStudyClient().update(studiesCommandOptions.updateCommandOptions.id, objectMap);
        System.out.println("Study: " + study);
    }

    private void delete() throws CatalogException, IOException {

        logger.debug("Deleting a study");
        ObjectMap objectMap = new ObjectMap();
        QueryResponse<Study> study = openCGAClient.getStudyClient().delete(studiesCommandOptions.deleteCommandOptions.id, objectMap);
        System.out.println("Study: " + study);

    }

    /************************************************  Summary Commands  ***********************************************/

    private void summary() throws CatalogException, IOException {

        logger.debug("Doing summary with the general stats of a study");
        QueryOptions queryOptions = new QueryOptions();
        QueryResponse<StudySummary> studySummaryQueryResponse =
                openCGAClient.getStudyClient().getSummary(studiesCommandOptions.deleteCommandOptions.id, queryOptions);
        System.out.println("StudySummary: " + studySummaryQueryResponse);

    }

    /************************************************  Search Commands  ***********************************************/

    private void search() throws CatalogException, IOException {

        logger.debug("Searching study");

        Query query = new Query();

        String id = studiesCommandOptions.searchCommandOptions.id;
        String projectId = studiesCommandOptions.searchCommandOptions.projectId;
        String name = studiesCommandOptions.searchCommandOptions.name;
        String alias = studiesCommandOptions.searchCommandOptions.alias;
        String type = studiesCommandOptions.searchCommandOptions.type;
        String creationDate = studiesCommandOptions.searchCommandOptions.creationDate;
        String status = studiesCommandOptions.searchCommandOptions.status;
        String attributes = studiesCommandOptions.searchCommandOptions.attributes;
        String nattributes = studiesCommandOptions.searchCommandOptions.nattributes;
        boolean battributes = studiesCommandOptions.searchCommandOptions.battributes;
        String groups = studiesCommandOptions.searchCommandOptions.groups;
        String groupsUsers = studiesCommandOptions.searchCommandOptions.groupsUsers;


        if (StringUtils.isNotEmpty(id)) {
            query.put(CatalogStudyDBAdaptor.QueryParams.ID.key(), id );
        }

        if (StringUtils.isNotEmpty(projectId)) {
            query.put(CatalogStudyDBAdaptor.QueryParams.PROJECT_ID.key(), projectId);
        }

        if (StringUtils.isNotEmpty(name)) {
            query.put(CatalogStudyDBAdaptor.QueryParams.NAME.key(), name);
        }

        if (StringUtils.isNotEmpty(alias)) {
            query.put(CatalogStudyDBAdaptor.QueryParams.ALIAS.key(), alias);
        }

        if (StringUtils.isNotEmpty(type)) {
            query.put(CatalogStudyDBAdaptor.QueryParams.TYPE.key(), type);
        }

        if (StringUtils.isNotEmpty(creationDate)) {
            query.put(CatalogStudyDBAdaptor.QueryParams.CREATION_DATE.key(), creationDate);
        }

        if (StringUtils.isNotEmpty(status)) {
            query.put(CatalogStudyDBAdaptor.QueryParams.STATUS_STATUS.key(), status);
        }

        if (StringUtils.isNotEmpty(attributes)) {
            query.put(CatalogStudyDBAdaptor.QueryParams.ATTRIBUTES.key(), attributes);
        }

        if (StringUtils.isNotEmpty(nattributes)) {
            query.put(CatalogStudyDBAdaptor.QueryParams.NATTRIBUTES.key(), nattributes);
        }

        if (battributes) {
            query.put(CatalogStudyDBAdaptor.QueryParams.BATTRIBUTES.key(), battributes);
        }

        if (StringUtils.isNotEmpty(groups)) {
            query.put(CatalogStudyDBAdaptor.QueryParams.GROUPS.key(), groups);
        }

        if (StringUtils.isNotEmpty(groupsUsers)) {
            query.put(CatalogStudyDBAdaptor.QueryParams.GROUP_USER_IDS.key(), groupsUsers);
        }

        QueryResponse<Study> studies = openCGAClient.getStudyClient().search(query, null);
        System.out.println("Studies: " + studies);
    }

    private void scanFiles() throws CatalogException, IOException {

        logger.debug("Scan the study folder to find changes\n");
        QueryResponse scan =  openCGAClient.getStudyClient().scanFiles(studiesCommandOptions.scanFilesCommandOptions.id, null);
        System.out.println("Scan Files: " + scan);
    }

    private void files() throws CatalogException, IOException {
        logger.debug("Listing files of a study");

        QueryOptions queryOptions = new QueryOptions();
        if (StringUtils.isNotEmpty(studiesCommandOptions.filesCommandOptions.type)) {
            queryOptions.put(CatalogFileDBAdaptor.QueryParams.TYPE.key(), studiesCommandOptions.filesCommandOptions.type);
        }
        if (StringUtils.isNotEmpty(studiesCommandOptions.filesCommandOptions.bioformat)) {
            queryOptions.put(CatalogFileDBAdaptor.QueryParams.BIOFORMAT.key(), studiesCommandOptions.filesCommandOptions.bioformat);
        }
        if (StringUtils.isNotEmpty(studiesCommandOptions.filesCommandOptions.size)) {
            queryOptions.put(CatalogFileDBAdaptor.QueryParams.DISK_USAGE.key(), studiesCommandOptions.filesCommandOptions.size);
        }

        QueryResponse<File> files = openCGAClient.getStudyClient().getFiles(studiesCommandOptions.filesCommandOptions.id, queryOptions);
        files.first().getResult().stream().forEach(file -> System.out.println(file.toString()));
    }

    private void jobs() throws CatalogException, IOException {
        logger.debug("Listing jobs of a study");

        QueryOptions queryOptions = new QueryOptions();
        if (StringUtils.isNotEmpty(studiesCommandOptions.jobsCommandOptions.name)) {
            queryOptions.put(CatalogJobDBAdaptor.QueryParams.NAME.key(), studiesCommandOptions.jobsCommandOptions.name);
        }
        //TODO
        /*PENDING
        if (StringUtils.isNotEmpty(studiesCommandOptions.jobsCommandOptions.toolId)) {
            queryOptions.put(CatalogJobDBAdaptor.QueryParams.TOOL_ID.key(), studiesCommandOptions.jobsCommandOptions.toolId);
        }
         */
        if (StringUtils.isNotEmpty(studiesCommandOptions.jobsCommandOptions.execution)) {
            queryOptions.put(CatalogJobDBAdaptor.QueryParams.EXECUTION.key(), studiesCommandOptions.jobsCommandOptions.execution);
        }
        if (StringUtils.isNotEmpty(studiesCommandOptions.jobsCommandOptions.description)) {
            queryOptions.put(CatalogJobDBAdaptor.QueryParams.DESCRIPTION.key(), studiesCommandOptions.jobsCommandOptions.description);
        }

        QueryResponse<Job> jobs = openCGAClient.getStudyClient().getJobs(studiesCommandOptions.filesCommandOptions.id, queryOptions);
        jobs.first().getResult().stream().forEach(file -> System.out.println(jobs.toString()));
    }

    private void alignments() throws CatalogException, IOException {

        //TODO
        logger.debug("Listing alignments of a study");

      /*PENDING

        QueryOptions queryOptions = new QueryOptions();
        if (StringUtils.isNotEmpty(studiesCommandOptions.alignmentsCommandOptions.sampleId)) {
            queryOptions.put(CatalogStudyDBAdaptor.QueryParams.SAMPLE_ID.key(), studiesCommandOptions.alignmentsCommandOptions.sampleId);
        }
        if (StringUtils.isNotEmpty(studiesCommandOptions.alignmentsCommandOptions.fileId)) {
            queryOptions.put(CatalogStudyDBAdaptor.QueryParams.FILE_ID.key(), studiesCommandOptions.alignmentsCommandOptions.fileId);
        }
        if (studiesCommandOptions.alignmentsCommandOptions.view_as_pairs) {
            queryOptions.put(CatalogStudyDBAdaptor.QueryParams.VIEW_AS_PAIRS.key(), studiesCommandOptions.alignmentsCommandOptions.view_as_pairs);
        }
        if (studiesCommandOptions.alignmentsCommandOptions.include_coverage) {
            queryOptions.put(CatalogStudyDBAdaptor.QueryParams.INCLUDE_COVERAGE.key(), studiesCommandOptions.alignmentsCommandOptions.include_coverage);
        }
        if (studiesCommandOptions.alignmentsCommandOptions.process_differences) {
            queryOptions.put(CatalogStudyDBAdaptor.QueryParams.PROCESS_DIFFERENCES.key(), studiesCommandOptions.alignmentsCommandOptions.process_differences);
        }

        if (studiesCommandOptions.alignmentsCommandOptions.process_differences) {
            queryOptions.put(CatalogStudyDBAdaptor.QueryParams.HISTOGRAM.key(), studiesCommandOptions.alignmentsCommandOptions.histogram);
        }

        if (studiesCommandOptions.alignmentsCommandOptions.process_differences) {
            queryOptions.put(CatalogStudyDBAdaptor.QueryParams.INTERVAL.key(), studiesCommandOptions.alignmentsCommandOptions.interval);
        }

        Query query = new Query();
        QueryResponse<Alignment> alignments = openCGAClient.getStudyClient().getAlignments(studiesCommandOptions.alignmentsCommandOptions.id,
                studiesCommandOptions.alignmentsCommandOptions.sampleId, studiesCommandOptions.alignmentsCommandOptions.fileId,
                studiesCommandOptions.alignmentsCommandOptions.region, query, queryOptions);
        System.out.println(alignments.toString());*/
    }

    private void samples() throws CatalogException, IOException {

        logger.debug("Listing samples of a study");

        QueryOptions queryOptions = new QueryOptions();
        if (StringUtils.isNotEmpty(studiesCommandOptions.samplesCommandOptions.name)) {
            queryOptions.put(CatalogSampleDBAdaptor.QueryParams.NAME.key(), studiesCommandOptions.samplesCommandOptions.name);
        }
        if (StringUtils.isNotEmpty(studiesCommandOptions.samplesCommandOptions.source)) {
            queryOptions.put(CatalogSampleDBAdaptor.QueryParams.SOURCE.key(), studiesCommandOptions.samplesCommandOptions.source);
        }
        if (StringUtils.isNotEmpty(studiesCommandOptions.samplesCommandOptions.description)) {
            queryOptions.put(CatalogSampleDBAdaptor.QueryParams.DESCRIPTION.key(), studiesCommandOptions.samplesCommandOptions.description);
        }

        QueryResponse<Sample> samples =
                openCGAClient.getStudyClient().getSamples(studiesCommandOptions.samplesCommandOptions.id, queryOptions);
        System.out.println(samples.toString());
    }

    private void variants() throws CatalogException, IOException {

        //TODO
        logger.debug("Listing variants of a study. Building");
        //PENDING
        QueryOptions queryOptions = new QueryOptions();
       /* if (StringUtils.isNotEmpty(studiesCommandOptions.variantsCommandOptions.ids)) {
            queryOptions.put(CatalogVariantDBAdaptor.VariantQueryParams.IDS.key(), studiesCommandOptions.variantsCommandOptions.ids);
        }*/
        if (StringUtils.isNotEmpty(studiesCommandOptions.variantsCommandOptions.region)) {
            queryOptions.put(CatalogVariantDBAdaptor.VariantQueryParams.REGION.key(), studiesCommandOptions.variantsCommandOptions.region);
        }

        if (StringUtils.isNotEmpty(studiesCommandOptions.variantsCommandOptions.chromosome)) {
            queryOptions.put(CatalogVariantDBAdaptor.VariantQueryParams.CHROMOSOME.key(),
                    studiesCommandOptions.variantsCommandOptions.chromosome);
        }

        if (StringUtils.isNotEmpty(studiesCommandOptions.variantsCommandOptions.gene)) {
            queryOptions.put(CatalogVariantDBAdaptor.VariantQueryParams.GENE.key(), studiesCommandOptions.variantsCommandOptions.gene);
        }

        if (StringUtils.isNotEmpty(studiesCommandOptions.variantsCommandOptions.type)) {
            queryOptions.put(CatalogVariantDBAdaptor.VariantQueryParams.TYPE.key(), studiesCommandOptions.variantsCommandOptions.type);
        }

        if (StringUtils.isNotEmpty(studiesCommandOptions.variantsCommandOptions.reference)) {
            queryOptions.put(CatalogVariantDBAdaptor.VariantQueryParams.REFERENCE.key(),
                    studiesCommandOptions.variantsCommandOptions.reference);
        }

        if (StringUtils.isNotEmpty(studiesCommandOptions.variantsCommandOptions.alternate)) {
            queryOptions.put(CatalogVariantDBAdaptor.VariantQueryParams.ALTERNATE.key(),
                    studiesCommandOptions.variantsCommandOptions.alternate);
        }

        if (StringUtils.isNotEmpty(studiesCommandOptions.variantsCommandOptions.returnedStudies)) {
            queryOptions.put(CatalogVariantDBAdaptor.VariantQueryParams.RETURNED_STUDIES.key(),
                    studiesCommandOptions.variantsCommandOptions.returnedStudies);
        }

        if (StringUtils.isNotEmpty(studiesCommandOptions.variantsCommandOptions.returnedSamples)) {
            queryOptions.put(CatalogVariantDBAdaptor.VariantQueryParams.RETURNED_SAMPLES.key(),
                    studiesCommandOptions.variantsCommandOptions.returnedSamples);
        }

        if (StringUtils.isNotEmpty(studiesCommandOptions.variantsCommandOptions.returnedFiles)) {
            queryOptions.put(CatalogVariantDBAdaptor.VariantQueryParams.RETURNED_FILES.key(),
                    studiesCommandOptions.variantsCommandOptions.returnedFiles);
        }

        if (StringUtils.isNotEmpty(studiesCommandOptions.variantsCommandOptions.files)) {
            queryOptions.put(CatalogVariantDBAdaptor.VariantQueryParams.FILES.key(), studiesCommandOptions.variantsCommandOptions.files);
        }

        if (StringUtils.isNotEmpty(studiesCommandOptions.variantsCommandOptions.maf)) {
            queryOptions.put(CatalogVariantDBAdaptor.VariantQueryParams.STATS_MAF.key(), studiesCommandOptions.variantsCommandOptions.maf);
        }

        if (StringUtils.isNotEmpty(studiesCommandOptions.variantsCommandOptions.mgf)) {
            queryOptions.put(CatalogVariantDBAdaptor.VariantQueryParams.STATS_MGF.key(), studiesCommandOptions.variantsCommandOptions.mgf);
        }

        if (StringUtils.isNotEmpty(studiesCommandOptions.variantsCommandOptions.missingAlleles)) {
            queryOptions.put(CatalogVariantDBAdaptor.VariantQueryParams.MISSING_ALLELES.key(),
                    studiesCommandOptions.variantsCommandOptions.missingAlleles);
        }

        if (StringUtils.isNotEmpty(studiesCommandOptions.variantsCommandOptions.missingGenotypes)) {
            queryOptions.put(CatalogVariantDBAdaptor.VariantQueryParams.MISSING_GENOTYPES.key(),
                    studiesCommandOptions.variantsCommandOptions.missingGenotypes);
        }

        if (StringUtils.isNotEmpty(studiesCommandOptions.variantsCommandOptions.annotationExists)) {
            queryOptions.put(CatalogVariantDBAdaptor.VariantQueryParams.ANNOTATION_EXISTS.key(),
                    studiesCommandOptions.variantsCommandOptions.annotationExists);
        }

        if (StringUtils.isNotEmpty(studiesCommandOptions.variantsCommandOptions.genotype)) {
            queryOptions.put(CatalogVariantDBAdaptor.VariantQueryParams.GENOTYPE.key(),
                    studiesCommandOptions.variantsCommandOptions.genotype);
        }

        if (StringUtils.isNotEmpty(studiesCommandOptions.variantsCommandOptions.annot_ct)) {
            queryOptions.put(CatalogVariantDBAdaptor.VariantQueryParams.ANNOT_CONSEQUENCE_TYPE.key(),
                    studiesCommandOptions.variantsCommandOptions.annot_ct);
        }

        if (StringUtils.isNotEmpty(studiesCommandOptions.variantsCommandOptions.annot_xref)) {
            queryOptions.put(CatalogVariantDBAdaptor.VariantQueryParams.ANNOT_XREF.key(),
                    studiesCommandOptions.variantsCommandOptions.annot_xref);
        }

        if (StringUtils.isNotEmpty(studiesCommandOptions.variantsCommandOptions.annot_biotype)) {
            queryOptions.put(CatalogVariantDBAdaptor.VariantQueryParams.ANNOT_BIOTYPE.key(),
                    studiesCommandOptions.variantsCommandOptions.annot_biotype);
        }

        if (StringUtils.isNotEmpty(studiesCommandOptions.variantsCommandOptions.polyphen)) {
            queryOptions.put(CatalogVariantDBAdaptor.VariantQueryParams.ANNOT_POLYPHEN.key(),
                    studiesCommandOptions.variantsCommandOptions.polyphen);
        }

        if (StringUtils.isNotEmpty(studiesCommandOptions.variantsCommandOptions.sift)) {
            queryOptions.put(CatalogVariantDBAdaptor.VariantQueryParams.ANNOT_SIFT.key(), studiesCommandOptions.variantsCommandOptions.sift);
        }

        if (StringUtils.isNotEmpty(studiesCommandOptions.variantsCommandOptions.conservation)) {
            queryOptions.put(CatalogVariantDBAdaptor.VariantQueryParams.ANNOT_CONSERVATION.key(),
                    studiesCommandOptions.variantsCommandOptions.conservation);
        }
        if (StringUtils.isNotEmpty(studiesCommandOptions.variantsCommandOptions.annotPopulationMaf)) {
            queryOptions.put(CatalogVariantDBAdaptor.VariantQueryParams.ANNOT_POPULATION_MINOR_ALLELE_FREQUENCY.key(),
                    studiesCommandOptions.variantsCommandOptions.annotPopulationMaf);
        }

        if (StringUtils.isNotEmpty(studiesCommandOptions.variantsCommandOptions.alternate_frequency)) {
            queryOptions.put(CatalogVariantDBAdaptor.VariantQueryParams.ANNOT_POPULATION_ALTERNATE_FREQUENCY.key(),
                    studiesCommandOptions.variantsCommandOptions.alternate_frequency);
        }

        if (StringUtils.isNotEmpty(studiesCommandOptions.variantsCommandOptions.reference_frequency)) {
            queryOptions.put(CatalogVariantDBAdaptor.VariantQueryParams.ANNOT_POPULATION_REFERENCE_FREQUENCY.key(),
                    studiesCommandOptions.variantsCommandOptions.reference_frequency);
        }

        if (StringUtils.isNotEmpty(studiesCommandOptions.variantsCommandOptions.transcriptionFlags)) {
            queryOptions.put(CatalogVariantDBAdaptor.VariantQueryParams.ANNOT_TRANSCRIPTION_FLAGS.key(),
                    studiesCommandOptions.variantsCommandOptions.transcriptionFlags);
        }

        if (StringUtils.isNotEmpty(studiesCommandOptions.variantsCommandOptions.geneTraitId)) {
            queryOptions.put(CatalogVariantDBAdaptor.VariantQueryParams.ANNOT_GENE_TRAITS_ID.key(),
                    studiesCommandOptions.variantsCommandOptions.geneTraitId);
        }

        if (StringUtils.isNotEmpty(studiesCommandOptions.variantsCommandOptions.geneTraitName)) {
            queryOptions.put(CatalogVariantDBAdaptor.VariantQueryParams.ANNOT_GENE_TRAITS_NAME.key(),
                    studiesCommandOptions.variantsCommandOptions.geneTraitName);
        }

        if (StringUtils.isNotEmpty(studiesCommandOptions.variantsCommandOptions.hpo)) {
            queryOptions.put(CatalogVariantDBAdaptor.VariantQueryParams.ANNOT_HPO.key(),
                    studiesCommandOptions.variantsCommandOptions.hpo);
        }

        if (StringUtils.isNotEmpty(studiesCommandOptions.variantsCommandOptions.proteinKeyword)) {
            queryOptions.put(CatalogVariantDBAdaptor.VariantQueryParams.ANNOT_PROTEIN_KEYWORDS.key(),
                    studiesCommandOptions.variantsCommandOptions.proteinKeyword);
        }

        if (StringUtils.isNotEmpty(studiesCommandOptions.variantsCommandOptions.drug)) {
            queryOptions.put(CatalogVariantDBAdaptor.VariantQueryParams.ANNOT_DRUG.key(),
                    studiesCommandOptions.variantsCommandOptions.drug);
        }

        if (StringUtils.isNotEmpty(studiesCommandOptions.variantsCommandOptions.functionalScore)) {
            queryOptions.put(CatalogVariantDBAdaptor.VariantQueryParams.ANNOT_FUNCTIONAL_SCORE.key(),
                    studiesCommandOptions.variantsCommandOptions.functionalScore);
        }

        if (StringUtils.isNotEmpty(studiesCommandOptions.variantsCommandOptions.unknownGenotype)) {
            queryOptions.put(CatalogVariantDBAdaptor.VariantQueryParams.UNKNOWN_GENOTYPE.key(),
                    studiesCommandOptions.variantsCommandOptions.unknownGenotype);
        }
        //PENDING
        /*if (StringUtils.isNotEmpty(studiesCommandOptions.variantsCommandOptions.samplesMetadata)) {
            queryOptions.put(CatalogVariantDBAdaptor.VariantQueryParams.SAMPLE_METADATA.key(), studiesCommandOptions.variantsCommandOptions.samplesMetadata);
        }

        if (StringUtils.isNotEmpty(studiesCommandOptions.variantsCommandOptions.sort)) {
            queryOptions.put(CatalogVariantDBAdaptor.VariantQueryParams.SORT.key(), studiesCommandOptions.variantsCommandOptions.sort);
        }

        if (StringUtils.isNotEmpty(studiesCommandOptions.variantsCommandOptions.groupBy)) {
            queryOptions.put(CatalogVariantDBAdaptor.VariantQueryParams.GROUPBY.key(), studiesCommandOptions.variantsCommandOptions.groupBy);
        }

        if (StringUtils.isNotEmpty(studiesCommandOptions.variantsCommandOptions.count)) {
            queryOptions.put(CatalogVariantDBAdaptor.VariantQueryParams.COUNT.key(), studiesCommandOptions.variantsCommandOptions.count);
        }

        if (StringUtils.isNotEmpty(studiesCommandOptions.variantsCommandOptions.histogram)) {
            queryOptions.put(CatalogVariantDBAdaptor.VariantQueryParams.HISTOGRAM.key(),
                    studiesCommandOptions.variantsCommandOptions.histogram);
        }

        if (StringUtils.isNotEmpty(studiesCommandOptions.variantsCommandOptions.interval)) {
            queryOptions.put(CatalogVariantDBAdaptor.VariantQueryParams.INTERVAL.key(),
                    studiesCommandOptions.variantsCommandOptions.interval);
        }

        if (StringUtils.isNotEmpty(studiesCommandOptions.variantsCommandOptions.merge)) {
            queryOptions.put(CatalogVariantDBAdaptor.VariantQueryParams.MERGE.key(),
                    studiesCommandOptions.variantsCommandOptions.merge);
        }*/
    }



    /********************************************  Administration ACLS commands  ***********************************************/

    private void removeRole() throws CatalogException,IOException{

    }
    private void assignRole() throws CatalogException,IOException{

    }


}
