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

package org.opencb.opencga.app.cli.main.executors.catalog;


import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResponse;
import org.opencb.opencga.analysis.storage.variant.CatalogVariantDBAdaptor;
import org.opencb.opencga.app.cli.main.OpencgaCommandExecutor;
import org.opencb.opencga.app.cli.main.executors.commons.AclCommandExecutor;
import org.opencb.opencga.app.cli.main.options.catalog.StudyCommandOptions;
import org.opencb.opencga.catalog.db.api.CatalogFileDBAdaptor;
import org.opencb.opencga.catalog.db.api.CatalogJobDBAdaptor;
import org.opencb.opencga.catalog.db.api.CatalogSampleDBAdaptor;
import org.opencb.opencga.catalog.db.api.CatalogStudyDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.models.File;
import org.opencb.opencga.catalog.models.Job;
import org.opencb.opencga.catalog.models.Sample;
import org.opencb.opencga.catalog.models.Study;
import org.opencb.opencga.catalog.models.acls.permissions.StudyAclEntry;
import org.opencb.opencga.catalog.models.summaries.StudySummary;
import org.opencb.opencga.client.rest.StudyClient;

import java.io.IOException;

/**
 * Created by imedina on 03/06/16.
 */
public class StudiesCommandExecutor extends OpencgaCommandExecutor {
    // TODO: Add include/exclude/skip/... (queryOptions) to the client calls !!!!

    private StudyCommandOptions studiesCommandOptions;
    private AclCommandExecutor<Study, StudyAclEntry> aclCommandExecutor;

    public StudiesCommandExecutor(StudyCommandOptions studiesCommandOptions) {
        super(studiesCommandOptions.commonCommandOptions);
        this.studiesCommandOptions = studiesCommandOptions;
        this.aclCommandExecutor = new AclCommandExecutor<>();
    }


    @Override
    public void execute() throws Exception {

        String subCommandString = getParsedSubCommand(studiesCommandOptions.jCommander);
        QueryResponse queryResponse = null;
        logger.debug("Executing studies command line: {}", subCommandString);
        switch (subCommandString) {
            case "create":
                queryResponse = create();
                break;
            case "info":
                queryResponse = info();
                break;
            case "update":
                queryResponse = update();
                break;
            case "delete":
                queryResponse = delete();
                break;
            case "summary":
                queryResponse = summary();
                break;
            case "help":
                queryResponse = help();
                break;
            case "search":
                queryResponse = search();
                break;
            case "scan-files":
                queryResponse = scanFiles();
                break;
            case "files":
                queryResponse = files();
                break;
            case "alignments":
                queryResponse = alignments();
                break;
            case "samples":
                queryResponse = samples();
                break;
            case "jobs":
                queryResponse = jobs();
                break;
            case "variants":
                queryResponse = variants();
                break;
            case "acl":
                queryResponse = aclCommandExecutor.acls(studiesCommandOptions.aclsCommandOptions, openCGAClient.getStudyClient());
                break;
            case "acl-create":
                queryResponse = aclCommandExecutor.aclsCreateTemplate(studiesCommandOptions.aclsCreateCommandOptions,
                        openCGAClient.getStudyClient());
                break;
            case "acl-member-delete":
                queryResponse = aclCommandExecutor.aclMemberDelete(studiesCommandOptions.aclsMemberDeleteCommandOptions,
                        openCGAClient.getStudyClient());
                break;
            case "acl-member-info":
                queryResponse = aclCommandExecutor.aclMemberInfo(studiesCommandOptions.aclsMemberInfoCommandOptions,
                        openCGAClient.getStudyClient());
                break;
            case "acl-member-update":
                queryResponse = aclCommandExecutor.aclMemberUpdate(studiesCommandOptions.aclsMemberUpdateCommandOptions,
                        openCGAClient.getStudyClient());
                break;
            case "groups":
                queryResponse = groups();
                break;
            case "groups-create":
                queryResponse = groupsCreate();
                break;
            case "groups-delete":
                queryResponse = groupsDelete();
                break;
            case "groups-info":
                queryResponse = groupsInfo();
                break;
            case "groups-update":
                queryResponse = groupsUpdate();
                break;

            default:
                logger.error("Subcommand not valid");
                break;
        }

        createOutput(queryResponse);

    }

    /**********************************************  Administration Commands  ***********************************************/

    private QueryResponse<Study> create() throws CatalogException, IOException {

        logger.debug("Creating a new study");
        String alias = studiesCommandOptions.createCommandOptions.alias;
        String name = studiesCommandOptions.createCommandOptions.name;
        String projectId = studiesCommandOptions.createCommandOptions.projectId;
        String description = studiesCommandOptions.createCommandOptions.description;
        String type = studiesCommandOptions.createCommandOptions.type;

        ObjectMap o = new ObjectMap();
        if (description != null) {
            o.append(CatalogStudyDBAdaptor.QueryParams.DESCRIPTION.key(), description);
        }
        if (type != null) {
            try {
                o.append(CatalogStudyDBAdaptor.QueryParams.TYPE.key(), Study.Type.valueOf(type));
            } catch (IllegalArgumentException e) {
                logger.error("{} not recognized as a proper study type", type);
                return null;
            }
        }

        return openCGAClient.getStudyClient().create(projectId, name, alias, o);
    }

    private QueryResponse<Study> info() throws CatalogException, IOException {

        logger.debug("Getting the study info");
        return openCGAClient.getStudyClient().get(studiesCommandOptions.infoCommandOptions.id, null);
    }

    private QueryResponse<Study> update() throws CatalogException, IOException {

        logger.debug("Updating the study");

        ObjectMap params = new ObjectMap();
        params.putIfNotNull(CatalogStudyDBAdaptor.QueryParams.NAME.key(), studiesCommandOptions.updateCommandOptions.name);
        params.putIfNotNull(CatalogStudyDBAdaptor.QueryParams.TYPE.key(), studiesCommandOptions.updateCommandOptions.type);
        params.putIfNotNull(CatalogStudyDBAdaptor.QueryParams.DESCRIPTION.key(), studiesCommandOptions.updateCommandOptions.description);
        params.putIfNotNull(CatalogStudyDBAdaptor.QueryParams.STATS.key(), studiesCommandOptions.updateCommandOptions.stats);
        params.putIfNotNull(CatalogStudyDBAdaptor.QueryParams.ATTRIBUTES.key(), studiesCommandOptions.updateCommandOptions.attributes);

        return openCGAClient.getStudyClient().update(studiesCommandOptions.updateCommandOptions.id, params);
    }

    private QueryResponse<Study> delete() throws CatalogException, IOException {

        logger.debug("Deleting a study");
        ObjectMap objectMap = new ObjectMap();
        return openCGAClient.getStudyClient().delete(studiesCommandOptions.deleteCommandOptions.id, objectMap);
    }

    /************************************************  Summary and help Commands  ***********************************************/

    private QueryResponse<StudySummary> summary() throws CatalogException, IOException {

        logger.debug("Doing summary with the general stats of a study");
        QueryOptions queryOptions = new QueryOptions();
        return openCGAClient.getStudyClient().getSummary(studiesCommandOptions.deleteCommandOptions.id, queryOptions);
    }

    private QueryResponse<Study> help() throws CatalogException, IOException {

        logger.debug("Helping");
        /*QueryOptions queryOptions = new QueryOptions();
        QueryResponse<Study> study =
                openCGAClient.getStudyClient().help(queryOptions);
        System.out.println("Help: " + study);*/
        System.out.println("PENDING");
        return null;
    }

    /************************************************  Search Commands  ***********************************************/

    private QueryResponse<Study> search() throws CatalogException, IOException {

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
        String battributes = studiesCommandOptions.searchCommandOptions.battributes;
//        String groups = studiesCommandOptions.searchCommandOptions.groups;
//        String groupsUsers = studiesCommandOptions.searchCommandOptions.groupsUsers;


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
            try {
                query.append(CatalogStudyDBAdaptor.QueryParams.TYPE.key(), Study.Type.valueOf(type));
            } catch (IllegalArgumentException e) {
                logger.error("{} not recognized as a proper study type", type);
                return null;
            }
        }

        if (StringUtils.isNotEmpty(creationDate)) {
            query.put(CatalogStudyDBAdaptor.QueryParams.CREATION_DATE.key(), creationDate);
        }

        if (StringUtils.isNotEmpty(status)) {
            query.put(CatalogStudyDBAdaptor.QueryParams.STATUS_NAME.key(), status);
        }

        if (StringUtils.isNotEmpty(attributes)) {
            query.put(CatalogStudyDBAdaptor.QueryParams.ATTRIBUTES.key(), attributes);
        }

        if (StringUtils.isNotEmpty(nattributes)) {
            query.put(CatalogStudyDBAdaptor.QueryParams.NATTRIBUTES.key(), nattributes);
        }

        if (StringUtils.isNotEmpty(battributes)) {
            query.put(CatalogStudyDBAdaptor.QueryParams.BATTRIBUTES.key(), battributes);
        }

//        if (StringUtils.isNotEmpty(groups)) {
//            query.put(CatalogStudyDBAdaptor.QueryParams.GROUPS.key(), groups);
//        }
//
//        if (StringUtils.isNotEmpty(groupsUsers)) {
//            query.put(CatalogStudyDBAdaptor.QueryParams.GROUP_USER_IDS.key(), groupsUsers);
//        }

        return openCGAClient.getStudyClient().search(query, null);
    }

    private QueryResponse scanFiles() throws CatalogException, IOException {

        logger.debug("Scan the study folder to find changes\n");
        return openCGAClient.getStudyClient().scanFiles(studiesCommandOptions.scanFilesCommandOptions.id, null);
    }

    private QueryResponse<File> files() throws CatalogException, IOException {

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

        return openCGAClient.getStudyClient().getFiles(studiesCommandOptions.filesCommandOptions.id, queryOptions);
    }

    private QueryResponse<Job> jobs() throws CatalogException, IOException {

        logger.debug("Listing jobs of a study");

        QueryOptions queryOptions = new QueryOptions();
        if (StringUtils.isNotEmpty(studiesCommandOptions.jobsCommandOptions.name)) {
            queryOptions.put(CatalogJobDBAdaptor.QueryParams.NAME.key(), studiesCommandOptions.jobsCommandOptions.name);
        }
        //TODO
        /*PENDING
        if (StringUtils.isNotEmpty(studiesCommandOptions.jobsCommandOptions.toolId)) {
            queryOptions.put(CatalogJobDBAdaptor.QueryParams.TOOL_ID.key(), studiesCommandOptions.jobsCommandOptions.toolId);
        }*/

        if (StringUtils.isNotEmpty(studiesCommandOptions.jobsCommandOptions.status)) {
            queryOptions.put(CatalogJobDBAdaptor.QueryParams.STATUS_NAME.key(), studiesCommandOptions.jobsCommandOptions.status);
        }
        if (StringUtils.isNotEmpty(studiesCommandOptions.jobsCommandOptions.ownerId)) {
            queryOptions.put(CatalogJobDBAdaptor.QueryParams.USER_ID.key(), studiesCommandOptions.jobsCommandOptions.ownerId);
        }
        if (StringUtils.isNotEmpty(studiesCommandOptions.jobsCommandOptions.date)) {
            queryOptions.put(CatalogJobDBAdaptor.QueryParams.CREATION_DATE.key(), studiesCommandOptions.jobsCommandOptions.date);
        }

        if (StringUtils.isNotEmpty(studiesCommandOptions.jobsCommandOptions.inputFiles)) {
            queryOptions.put(CatalogJobDBAdaptor.QueryParams.INPUT.key(), studiesCommandOptions.jobsCommandOptions.inputFiles);
        }
        if (StringUtils.isNotEmpty(studiesCommandOptions.jobsCommandOptions.outputFiles)) {
            queryOptions.put(CatalogJobDBAdaptor.QueryParams.OUTPUT.key(), studiesCommandOptions.jobsCommandOptions.outputFiles);
        }

        if (StringUtils.isNotEmpty(studiesCommandOptions.jobsCommandOptions.include)) {
            queryOptions.put(QueryOptions.INCLUDE, studiesCommandOptions.jobsCommandOptions.include);
        }
        if (StringUtils.isNotEmpty(studiesCommandOptions.jobsCommandOptions.exclude)) {
            queryOptions.put(QueryOptions.EXCLUDE, studiesCommandOptions.jobsCommandOptions.exclude);
        }

        if (StringUtils.isNotEmpty(studiesCommandOptions.jobsCommandOptions.limit)) {
            queryOptions.put(QueryOptions.LIMIT, studiesCommandOptions.jobsCommandOptions.limit);
        }
        if (StringUtils.isNotEmpty(studiesCommandOptions.jobsCommandOptions.skip)) {
            queryOptions.put(QueryOptions.SKIP, studiesCommandOptions.jobsCommandOptions.skip);
        }

        queryOptions.put("count", studiesCommandOptions.jobsCommandOptions.count);

        return openCGAClient.getStudyClient().getJobs(studiesCommandOptions.jobsCommandOptions.id, queryOptions);
    }

    private QueryResponse alignments() throws CatalogException, IOException {

        //TODO
        logger.debug("Listing alignments of a study");
        return null;
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

    private QueryResponse<Sample> samples() throws CatalogException, IOException {

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

        if (StringUtils.isNotEmpty(studiesCommandOptions.samplesCommandOptions.include)) {
            queryOptions.put(QueryOptions.INCLUDE, studiesCommandOptions.samplesCommandOptions.include);
        }
        if (StringUtils.isNotEmpty(studiesCommandOptions.samplesCommandOptions.exclude)) {
            queryOptions.put(QueryOptions.EXCLUDE, studiesCommandOptions.samplesCommandOptions.exclude);
        }
        if (StringUtils.isNotEmpty(studiesCommandOptions.samplesCommandOptions.limit)) {
            queryOptions.put(QueryOptions.LIMIT, studiesCommandOptions.samplesCommandOptions.limit);
        }
        if (StringUtils.isNotEmpty(studiesCommandOptions.samplesCommandOptions.skip)) {
            queryOptions.put(QueryOptions.SKIP, studiesCommandOptions.samplesCommandOptions.skip);
        }
        queryOptions.put("count", studiesCommandOptions.samplesCommandOptions.count);
        return openCGAClient.getStudyClient().getSamples(studiesCommandOptions.samplesCommandOptions.id, queryOptions);
    }

    private QueryResponse<Variant> variants() throws CatalogException, IOException {

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
        if (StringUtils.isNotEmpty(studiesCommandOptions.variantsCommandOptions.include)) {
            queryOptions.put(QueryOptions.INCLUDE, studiesCommandOptions.variantsCommandOptions.include);
        }
        if (StringUtils.isNotEmpty(studiesCommandOptions.variantsCommandOptions.exclude)) {
            queryOptions.put(QueryOptions.EXCLUDE, studiesCommandOptions.variantsCommandOptions.exclude);
        }
        if (StringUtils.isNotEmpty(studiesCommandOptions.variantsCommandOptions.limit)) {
            queryOptions.put(QueryOptions.LIMIT, studiesCommandOptions.variantsCommandOptions.limit);
        }
        if (StringUtils.isNotEmpty(studiesCommandOptions.variantsCommandOptions.skip)) {
            queryOptions.put(QueryOptions.SKIP, studiesCommandOptions.variantsCommandOptions.skip);
        }
        queryOptions.put("count", studiesCommandOptions.variantsCommandOptions.count);

        return openCGAClient.getStudyClient().getVariants(studiesCommandOptions.variantsCommandOptions.id, queryOptions);
    }

    /************************************************* Groups commands *********************************************************/
    private QueryResponse<ObjectMap> groups() throws CatalogException,IOException {
        logger.debug("Creating groups");
        QueryOptions queryOptions = new QueryOptions();
        return openCGAClient.getStudyClient().groups(studiesCommandOptions.groupsCommandOptions.id, queryOptions);
    }
    private QueryResponse<ObjectMap> groupsCreate() throws CatalogException,IOException {

        logger.debug("Creating groups");
        QueryOptions queryOptions = new QueryOptions();
        return openCGAClient.getStudyClient().createGroup(studiesCommandOptions.groupsCreateCommandOptions.id,
                studiesCommandOptions.groupsCreateCommandOptions.groupId, studiesCommandOptions.groupsCreateCommandOptions.users,
                queryOptions);
    }

    private QueryResponse<ObjectMap> groupsDelete() throws CatalogException,IOException {

        logger.debug("Deleting groups");
        QueryOptions queryOptions = new QueryOptions();
        return openCGAClient.getStudyClient().deleteGroup(studiesCommandOptions.groupsDeleteCommandOptions.id,
                studiesCommandOptions.groupsDeleteCommandOptions.groupId, studiesCommandOptions.groupsDeleteCommandOptions.users,
                queryOptions);
    }

    private QueryResponse<ObjectMap> groupsInfo() throws CatalogException,IOException {

        logger.debug("Info groups");
        QueryOptions queryOptions = new QueryOptions();
        return openCGAClient.getStudyClient().infoGroup(studiesCommandOptions.groupsInfoCommandOptions.id,
                studiesCommandOptions.groupsInfoCommandOptions.groupId, queryOptions);
    }
    private QueryResponse<ObjectMap> groupsUpdate() throws CatalogException,IOException {

        logger.debug("Updating groups");

        QueryOptions queryOptions = new QueryOptions();

        if (StringUtils.isNotEmpty(studiesCommandOptions.groupsUpdateCommandOptions.addUsers)) {
            queryOptions.put(StudyClient.GroupUpdateParams.ADD_USERS.key(),
                    studiesCommandOptions.groupsUpdateCommandOptions.addUsers);
        }

        if (StringUtils.isNotEmpty(studiesCommandOptions.groupsUpdateCommandOptions.setUsers)) {
            queryOptions.put(StudyClient.GroupUpdateParams.SET_USERS.key(),
                    studiesCommandOptions.groupsUpdateCommandOptions.setUsers);
        }

        if (StringUtils.isNotEmpty(studiesCommandOptions.groupsUpdateCommandOptions.removeUsers)) {
            queryOptions.put(StudyClient.GroupUpdateParams.REMOVE_USERS.key(),
                    studiesCommandOptions.groupsUpdateCommandOptions.removeUsers);
        }

        return openCGAClient.getStudyClient().updateGroup(studiesCommandOptions.groupsUpdateCommandOptions.id,
                studiesCommandOptions.groupsUpdateCommandOptions.groupId, queryOptions);
    }

}
