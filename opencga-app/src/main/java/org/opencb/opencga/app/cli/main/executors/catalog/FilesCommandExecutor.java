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


import org.apache.commons.collections.iterators.ArrayListIterator;
import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.*;
import org.opencb.opencga.analysis.storage.variant.CatalogVariantDBAdaptor;
import org.opencb.opencga.app.cli.main.OpencgaCommandExecutor;
import org.opencb.opencga.app.cli.main.executors.commons.AclCommandExecutor;
import org.opencb.opencga.app.cli.main.options.catalog.FileCommandOptions;
import org.opencb.opencga.catalog.db.api.FileDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogFileUtils;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.catalog.models.File;
import org.opencb.opencga.catalog.models.Job;
import org.opencb.opencga.catalog.models.acls.permissions.FileAclEntry;
import org.opencb.opencga.catalog.utils.FileMetadataReader;
import org.opencb.opencga.core.common.UriUtils;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by imedina on 03/06/16.
 */
public class FilesCommandExecutor extends OpencgaCommandExecutor {

    private FileCommandOptions filesCommandOptions;
    private AclCommandExecutor<File, FileAclEntry> aclCommandExecutor;

    public FilesCommandExecutor(FileCommandOptions filesCommandOptions) {
        super(filesCommandOptions.commonCommandOptions);
        this.filesCommandOptions = filesCommandOptions;
        this.aclCommandExecutor = new AclCommandExecutor<>();
    }


    @Override
    public void execute() throws Exception {
        logger.debug("Executing files command line");

        String subCommandString = getParsedSubCommand(filesCommandOptions.jCommander);
        QueryResponse queryResponse = null;
        switch (subCommandString) {
            case "copy":
                queryResponse = copy();
                break;
            case "create-folder":
                queryResponse = createFolder();
                break;
            case "info":
                queryResponse = info();
                break;
            case "download":
                queryResponse = download();
                break;
            case "grep":
                queryResponse = grep();
                break;
            case "search":
                queryResponse = search();
                break;
            case "list":
                queryResponse = list();
                break;
            case "tree-view":
                queryResponse = treeView();
                break;
            case "index":
                queryResponse = index();
                break;
            case "alignment":
                queryResponse = alignment();
                break;
            case "content":
                queryResponse = content();
                break;
            case "fetch":
                queryResponse = fetch();
                break;
            case "update":
                queryResponse = update();
                break;
            case "upload":
                queryResponse = upload();
                break;
            case "delete":
                queryResponse = delete();
                break;
            case "link":
                queryResponse = link();
                break;
            case "relink":
                queryResponse = relink();
                break;
            case "unlink":
                queryResponse = unlink();
                break;
            case "refresh":
                queryResponse = refresh();
                break;
            case "group-by":
                queryResponse = groupBy();
                break;
            case "variants":
                queryResponse = variants();
                break;
            case "acl":
                queryResponse = aclCommandExecutor.acls(filesCommandOptions.aclsCommandOptions, openCGAClient.getFileClient());
                break;
            case "acl-create":
                queryResponse = aclCommandExecutor.aclsCreate(filesCommandOptions.aclsCreateCommandOptions, openCGAClient.getFileClient());
                break;
            case "acl-member-delete":
                queryResponse = aclCommandExecutor.aclMemberDelete(filesCommandOptions.aclsMemberDeleteCommandOptions,
                        openCGAClient.getFileClient());
                break;
            case "acl-member-info":
                queryResponse = aclCommandExecutor.aclMemberInfo(filesCommandOptions.aclsMemberInfoCommandOptions,
                        openCGAClient.getFileClient());
                break;
            case "acl-member-update":
                queryResponse = aclCommandExecutor.aclMemberUpdate(filesCommandOptions.aclsMemberUpdateCommandOptions,
                        openCGAClient.getFileClient());
                break;
            default:
                logger.error("Subcommand not valid");
                break;
        }

        createOutput(queryResponse);
    }

    private QueryResponse<File> copy() throws CatalogException {
        logger.debug("Creating a new file");
        //openCGAClient.getFileClient(). /******************* Falta el create en FileClient.java ?? **//
//        OptionsParser.FileCommands.CreateCommand c = optionsParser.getFileCommands().createCommand;
        FileCommandOptions.CopyCommandOptions copyCommandOptions = filesCommandOptions.copyCommandOptions;
        long studyId = catalogManager.getStudyId(copyCommandOptions.studyId);
        Path inputFile = Paths.get(copyCommandOptions.inputFile);
        URI sourceUri;
        try {
            sourceUri = new URI(null, copyCommandOptions.inputFile, null);
        } catch (URISyntaxException e) {
            throw new CatalogException("Input file is not a proper URI");
        }
        if (sourceUri.getScheme() == null || sourceUri.getScheme().isEmpty()) {
            sourceUri = inputFile.toUri();
        }
        if (!catalogManager.getCatalogIOManagerFactory().get(sourceUri).exists(sourceUri)) {
            throw new CatalogException("File " + sourceUri + " does not exist");
        }

        QueryResult<File> file = catalogManager.createFile(studyId, copyCommandOptions.format, copyCommandOptions.bioformat,
                Paths.get(copyCommandOptions.path, inputFile.getFileName().toString()).toString(), copyCommandOptions.description,
                copyCommandOptions.parents, -1, sessionId);
        new CatalogFileUtils(catalogManager).upload(sourceUri, file.first(), null, sessionId, false, false,
                copyCommandOptions.move, copyCommandOptions.calculateChecksum);
        FileMetadataReader.get(catalogManager).setMetadataInformation(file.first(), null, new QueryOptions(), sessionId, false);
        return new QueryResponse<>(new QueryOptions(), Arrays.asList(file));
    }

    private QueryResponse createFolder() throws CatalogException, IOException {
        logger.debug("Creating a new folder");
        ObjectMap o = new ObjectMap();
        if (filesCommandOptions.createFolderCommandOptions.parents){
            o.put("parents",filesCommandOptions.createFolderCommandOptions.parents);
        }

        return openCGAClient.getFileClient().createFolder(filesCommandOptions.createFolderCommandOptions.studyId,
                filesCommandOptions.createFolderCommandOptions.folder, o);
    }


    private QueryResponse<File> info() throws CatalogException, IOException {
        logger.debug("Getting file information");
        QueryOptions queryOptions = new QueryOptions();
        if (StringUtils.isNotEmpty(filesCommandOptions.infoCommandOptions.include)) {
            queryOptions.put(QueryOptions.INCLUDE, filesCommandOptions.infoCommandOptions.include);
        }

        if (StringUtils.isNotEmpty(filesCommandOptions.infoCommandOptions.exclude)) {
            queryOptions.put(QueryOptions.EXCLUDE, filesCommandOptions.infoCommandOptions.exclude);
        }
        return openCGAClient.getFileClient().get(filesCommandOptions.infoCommandOptions.fileIds, queryOptions);
    }

    private QueryResponse download() throws CatalogException, IOException {
        logger.debug("Downloading file");
        return openCGAClient.getFileClient().download(filesCommandOptions.downloadCommandOptions.id, new ObjectMap());
    }

    private QueryResponse grep() throws CatalogException, IOException {
        logger.debug("Grep command: File content");
        ObjectMap objectMap = new ObjectMap();
        objectMap.put("ignoreCase", filesCommandOptions.grepCommandOptions.ignoreCase);
        objectMap.put("multi", filesCommandOptions.grepCommandOptions.multi);
        return openCGAClient.getFileClient().grep(filesCommandOptions.grepCommandOptions.id, filesCommandOptions.grepCommandOptions.pattern,
                objectMap);
    }

    private QueryResponse search() throws CatalogException, IOException {
        logger.debug("Searching files");
        //FIXME check and put the correct format for type and bioformat. See StudiesCommandExecutor search param type. Is better
        Query query = new Query();
        QueryOptions queryOptions = new QueryOptions();

        query.putIfNotEmpty(FileDBAdaptor.QueryParams.ID.key(), filesCommandOptions.searchCommandOptions.id);
        query.putIfNotEmpty(FileDBAdaptor.QueryParams.STUDY_ID.key(), filesCommandOptions.searchCommandOptions.studyId);
        query.putIfNotEmpty(FileDBAdaptor.QueryParams.NAME.key(), filesCommandOptions.searchCommandOptions.name);
        query.putIfNotEmpty(FileDBAdaptor.QueryParams.PATH.key(), filesCommandOptions.searchCommandOptions.path);
        query.putIfNotNull(FileDBAdaptor.QueryParams.TYPE.key(), filesCommandOptions.searchCommandOptions.type);
        query.putIfNotNull(FileDBAdaptor.QueryParams.BIOFORMAT.key(), filesCommandOptions.searchCommandOptions.bioformat);
        query.putIfNotEmpty(FileDBAdaptor.QueryParams.FORMAT.key(), filesCommandOptions.searchCommandOptions.format);
        query.putIfNotEmpty(FileDBAdaptor.QueryParams.STATUS.key(), filesCommandOptions.searchCommandOptions.status);
        query.putIfNotEmpty(FileDBAdaptor.QueryParams.DIRECTORY.key(), filesCommandOptions.searchCommandOptions.directory);
        query.putIfNotEmpty(FileDBAdaptor.QueryParams.OWNER_ID.key(), filesCommandOptions.searchCommandOptions.ownerId);
        query.putIfNotEmpty(FileDBAdaptor.QueryParams.CREATION_DATE.key(), filesCommandOptions.searchCommandOptions.creationDate);
        query.putIfNotEmpty(FileDBAdaptor.QueryParams.MODIFICATION_DATE.key(),
                filesCommandOptions.groupByCommandOptions.modificationDate);
        query.putIfNotEmpty(FileDBAdaptor.QueryParams.DESCRIPTION.key(), filesCommandOptions.searchCommandOptions.description);
        query.putIfNotEmpty(FileDBAdaptor.QueryParams.DISK_USAGE.key(), filesCommandOptions.searchCommandOptions.diskUsage);
        query.putIfNotEmpty(FileDBAdaptor.QueryParams.SAMPLE_IDS.key(), filesCommandOptions.searchCommandOptions.sampleIds);
        query.putIfNotEmpty(FileDBAdaptor.QueryParams.JOB_ID.key(), filesCommandOptions.searchCommandOptions.jobId);
        query.putIfNotEmpty(FileDBAdaptor.QueryParams.ATTRIBUTES.key(), filesCommandOptions.searchCommandOptions.attributes);
        query.putIfNotEmpty(FileDBAdaptor.QueryParams.NATTRIBUTES.key(), filesCommandOptions.searchCommandOptions.nattributes);
        query.putIfNotEmpty(FileDBAdaptor.QueryParams.SAMPLE_IDS.key(), filesCommandOptions.searchCommandOptions.sampleIds);
        queryOptions.putIfNotEmpty(QueryOptions.INCLUDE, filesCommandOptions.searchCommandOptions.include);
        queryOptions.putIfNotEmpty(QueryOptions.EXCLUDE, filesCommandOptions.searchCommandOptions.exclude);
        queryOptions.putIfNotEmpty(QueryOptions.LIMIT, filesCommandOptions.searchCommandOptions.limit);
        queryOptions.putIfNotEmpty(QueryOptions.SKIP, filesCommandOptions.searchCommandOptions.skip);

        queryOptions.put("count", filesCommandOptions.searchCommandOptions.count);
        return openCGAClient.getFileClient().search(query,queryOptions);
    }

    private QueryResponse<File> list() throws CatalogException, IOException {
        logger.debug("Listing files in folder");

        QueryOptions queryOptions = new QueryOptions();
        queryOptions.putIfNotEmpty(QueryOptions.INCLUDE, filesCommandOptions.listCommandOptions.include);
        queryOptions.putIfNotEmpty(QueryOptions.EXCLUDE, filesCommandOptions.listCommandOptions.exclude);
        queryOptions.putIfNotEmpty(QueryOptions.LIMIT, filesCommandOptions.listCommandOptions.limit);
        queryOptions.putIfNotEmpty(QueryOptions.SKIP, filesCommandOptions.listCommandOptions.skip);
        queryOptions.put("count", filesCommandOptions.listCommandOptions.count);

        return openCGAClient.getFileClient().list(filesCommandOptions.listCommandOptions.folderId, queryOptions);
    }

    private QueryResponse<Job> index() throws CatalogException, IOException {
        logger.debug("Indexing variant(s)");

        String fileIds = filesCommandOptions.indexCommandOptions.fileIds;

        ObjectMap o = new ObjectMap();
        o.putIfNotNull("studyId", filesCommandOptions.indexCommandOptions.studyId);
        o.putIfNotNull("outDir", filesCommandOptions.indexCommandOptions.outdir);
        o.putIfNotNull("transform", filesCommandOptions.indexCommandOptions.transform);
        o.putIfNotNull("load", filesCommandOptions.indexCommandOptions.load);
        o.putIfNotNull("includeExtraFields", filesCommandOptions.indexCommandOptions.extraFields);
        o.putIfNotNull("aggregated", filesCommandOptions.indexCommandOptions.aggregated);
        o.putIfNotNull("calculateStats", filesCommandOptions.indexCommandOptions.calculateStats);
        o.putIfNotNull("annotate", filesCommandOptions.indexCommandOptions.annotate);
        o.putIfNotNull("overwrite", filesCommandOptions.indexCommandOptions.overwriteAnnotations);

        return openCGAClient.getFileClient().index(fileIds, o);
    }

    private QueryResponse<File> treeView() throws CatalogException, IOException {
        logger.debug("Obtain a tree view of the files and folders within a folder");

        ObjectMap o = new ObjectMap();
        o.putIfNotNull("maxDepth", filesCommandOptions.treeViewCommandOptions.maxDepth);
        o.putIfNotNull("include", filesCommandOptions.treeViewCommandOptions.include);
        o.putIfNotNull("exclude", filesCommandOptions.treeViewCommandOptions.exclude);
        o.putIfNotNull("limit", filesCommandOptions.treeViewCommandOptions.limit);
        return openCGAClient.getFileClient().treeView(filesCommandOptions.treeViewCommandOptions.folderId, o);
    }

    private QueryResponse alignment() throws CatalogException, IOException {
        logger.debug("Fetch alignments from a BAM file");

        QueryOptions queryOptions = new QueryOptions();

            queryOptions.putIfNotEmpty(QueryOptions.INCLUDE, filesCommandOptions.alignmentCommandOptions.include);
            queryOptions.putIfNotEmpty(QueryOptions.EXCLUDE, filesCommandOptions.alignmentCommandOptions.exclude);
            queryOptions.putIfNotEmpty(QueryOptions.LIMIT, filesCommandOptions.alignmentCommandOptions.limit);
            queryOptions.putIfNotEmpty(QueryOptions.SKIP, filesCommandOptions.alignmentCommandOptions.skip);
            queryOptions.put("count", filesCommandOptions.alignmentCommandOptions.count);

        return openCGAClient.getFileClient().alignments(filesCommandOptions.alignmentCommandOptions.id, queryOptions);
    }

    private QueryResponse content() throws CatalogException, IOException {
        ObjectMap objectMap = new ObjectMap();
        objectMap.put("start", filesCommandOptions.contentCommandOptions.start);
        objectMap.put(QueryOptions.LIMIT, filesCommandOptions.contentCommandOptions.limit);
        return openCGAClient.getFileClient().content(filesCommandOptions.contentCommandOptions.id, objectMap);
    }
    private QueryResponse fetch() throws CatalogException {
        logger.debug("File Fetch. [DEPRECATED]  Use .../files/{fileId}/[variants|alignments] or " +
                ".../studies/{studyId}/[variants|alignments] instead");
        return null;
    }


    private QueryResponse update() throws CatalogException, IOException {
        logger.debug("updating file");
        ObjectMap objectMap = new ObjectMap();
        objectMap.putIfNotEmpty(FileDBAdaptor.QueryParams.FORMAT.key(), filesCommandOptions.updateCommandOptions.format);
        objectMap.putIfNotEmpty(FileDBAdaptor.QueryParams.BIOFORMAT.key(), filesCommandOptions.updateCommandOptions.bioformat);
        objectMap.putIfNotEmpty(FileDBAdaptor.QueryParams.DESCRIPTION.key(), filesCommandOptions.updateCommandOptions.description);
        objectMap.putIfNotEmpty(FileDBAdaptor.QueryParams.ATTRIBUTES.key(), filesCommandOptions.updateCommandOptions.attributes);
        objectMap.putIfNotEmpty(FileDBAdaptor.QueryParams.STATS.key(), filesCommandOptions.updateCommandOptions.stats);
        objectMap.putIfNotEmpty(FileDBAdaptor.QueryParams.SAMPLE_IDS.key(), filesCommandOptions.updateCommandOptions.sampleIds);
        objectMap.putIfNotEmpty(FileDBAdaptor.QueryParams.JOB_ID.key(), filesCommandOptions.updateCommandOptions.jobId);
        objectMap.putIfNotEmpty(FileDBAdaptor.QueryParams.PATH.key(), filesCommandOptions.updateCommandOptions.path);
        objectMap.putIfNotEmpty(FileDBAdaptor.QueryParams.NAME.key(), filesCommandOptions.updateCommandOptions.name);

        return openCGAClient.getFileClient().update(filesCommandOptions.updateCommandOptions.id, objectMap);

    }

    private QueryResponse<File> upload() throws CatalogException, IOException {
        logger.debug("uploading file");

        ObjectMap objectMap = new ObjectMap()
                .append("fileFormat", filesCommandOptions.uploadCommandOptions.fileFormat)
                .append("bioformat", filesCommandOptions.uploadCommandOptions.bioformat)
                .append("parents", filesCommandOptions.uploadCommandOptions.parents);

        if (filesCommandOptions.uploadCommandOptions.catalogPath != null) {
            objectMap.append("relativeFilePath", filesCommandOptions.uploadCommandOptions.catalogPath);
        }

        if (filesCommandOptions.uploadCommandOptions.description != null) {
            objectMap.append("description", filesCommandOptions.uploadCommandOptions.description);
        }

        if (filesCommandOptions.uploadCommandOptions.fileName != null) {
            objectMap.append("fileName", filesCommandOptions.uploadCommandOptions.fileName);
        }

        return openCGAClient.getFileClient().upload(filesCommandOptions.uploadCommandOptions.studyId,
                filesCommandOptions.uploadCommandOptions.inputFile, objectMap);
    }

    private QueryResponse<File> delete() throws CatalogException, IOException {
        logger.debug("Deleting file");

        ObjectMap objectMap = new ObjectMap()
                .append("deleteExternal", filesCommandOptions.deleteCommandOptions.deleteExternal)
                .append("skipTrash", filesCommandOptions.deleteCommandOptions.skipTrash);

        return openCGAClient.getFileClient().delete(filesCommandOptions.deleteCommandOptions.id, objectMap);
    }

    private QueryResponse<File> link() throws CatalogException, IOException, URISyntaxException {
        logger.debug("Linking the file or folder into catalog.");

        ObjectMap objectMap = new ObjectMap()
                .append(FileDBAdaptor.QueryParams.DESCRIPTION.key(), filesCommandOptions.linkCommandOptions.description)
                .append("parents", filesCommandOptions.linkCommandOptions.parents);

        CatalogManager catalogManager = null;
        try {
            catalogManager = new CatalogManager(catalogConfiguration);
        } catch (CatalogException e) {
            logger.error("Catalog manager could not be initialized. Is the configuration OK?");
        }
        if (!catalogManager.existsCatalogDB()) {
            logger.error("The database could not be found. Are you running this from the server?");
            return null;
        }

        List<QueryResult<File>> linkQueryResultList = new ArrayList<>(filesCommandOptions.linkCommandOptions.inputs.size());

        for (String input : filesCommandOptions.linkCommandOptions.inputs) {
            URI uri = UriUtils.createUri(input);
            logger.debug("uri: {}", uri.toString());

            linkQueryResultList.add(catalogManager.link(uri, filesCommandOptions.linkCommandOptions.path,
                    filesCommandOptions.linkCommandOptions.studyId, objectMap, sessionId));
        }

        return new QueryResponse<>(new QueryOptions(), linkQueryResultList);
    }

    private QueryResponse relink() throws CatalogException, IOException {
        logger.debug("Change file location. Provided file must be either STAGED or an external file. [DEPRECATED]");
        QueryOptions queryOptions = new QueryOptions();
        queryOptions.put("calculateChecksum", filesCommandOptions.relinkCommandOptions.calculateChecksum);
        return  openCGAClient.getFileClient().relink(filesCommandOptions.relinkCommandOptions.id,
                filesCommandOptions.relinkCommandOptions.uri, queryOptions);
    }

    private QueryResponse<File> unlink() throws CatalogException, IOException {
        logger.debug("Unlink an external file from catalog");

        // LOCAL EXECUTION
//        CatalogManager catalogManager = null;
//        try {
//            catalogManager = new CatalogManager(catalogConfiguration);
//        } catch (CatalogException e) {
//            logger.error("Catalog manager could not be initialized. Is the configuration OK?");
//        }
//        if (!catalogManager.existsCatalogDB()) {
//            logger.error("The database could not be found. Are you running this from the server?");
//            return;
//        }
//        QueryResult<File> unlinkQueryResult = catalogManager.unlink(filesCommandOptions.unlinkCommandOptions.id, new QueryOptions(),
//                sessionId);
//
//        QueryResponse<File> unlink = new QueryResponse<>(new QueryOptions(), Arrays.asList(unlinkQueryResult));

        return openCGAClient.getFileClient().unlink(filesCommandOptions.unlinkCommandOptions.id, new ObjectMap());
    }

    private QueryResponse refresh() throws CatalogException, IOException {
        logger.debug("Refreshing metadata from the selected file or folder. Print updated files.");
        return openCGAClient.getFileClient().refresh(filesCommandOptions.refreshCommandOptions.id, new QueryOptions());
    }


    private QueryResponse groupBy() throws CatalogException, IOException {
        logger.debug("Grouping files by several fields");

        QueryOptions queryOptions = new QueryOptions();

        queryOptions.putIfNotEmpty(FileDBAdaptor.QueryParams.ID.key(), filesCommandOptions.groupByCommandOptions.id);
        queryOptions.putIfNotEmpty(FileDBAdaptor.QueryParams.NAME.key(), filesCommandOptions.groupByCommandOptions.name);
        queryOptions.putIfNotEmpty(FileDBAdaptor.QueryParams.PATH.key(), filesCommandOptions.groupByCommandOptions.path);
        queryOptions.putIfNotEmpty(FileDBAdaptor.QueryParams.TYPE.key(), filesCommandOptions.groupByCommandOptions.type);
        queryOptions.putIfNotEmpty(FileDBAdaptor.QueryParams.BIOFORMAT.key(), filesCommandOptions.groupByCommandOptions.bioformat);
        queryOptions.putIfNotEmpty(FileDBAdaptor.QueryParams.FORMAT.key(), filesCommandOptions.groupByCommandOptions.format);
        queryOptions.putIfNotEmpty(FileDBAdaptor.QueryParams.STATUS.key(), filesCommandOptions.groupByCommandOptions.status);
        queryOptions.putIfNotEmpty(FileDBAdaptor.QueryParams.DIRECTORY.key(), filesCommandOptions.groupByCommandOptions.directory);
        queryOptions.putIfNotEmpty(FileDBAdaptor.QueryParams.OWNER_ID.key(), filesCommandOptions.groupByCommandOptions.ownerId);
        queryOptions.putIfNotEmpty(FileDBAdaptor.QueryParams.CREATION_DATE.key(), filesCommandOptions.groupByCommandOptions.creationDate);
        queryOptions.putIfNotEmpty(FileDBAdaptor.QueryParams.MODIFICATION_DATE.key(),
                filesCommandOptions.groupByCommandOptions.modificationDate);
        queryOptions.putIfNotEmpty(FileDBAdaptor.QueryParams.DESCRIPTION.key(), filesCommandOptions.groupByCommandOptions.description);
        queryOptions.putIfNotEmpty(FileDBAdaptor.QueryParams.DISK_USAGE.key(), filesCommandOptions.groupByCommandOptions.diskUsage);
        queryOptions.putIfNotEmpty(FileDBAdaptor.QueryParams.SAMPLE_IDS.key(), filesCommandOptions.groupByCommandOptions.sampleIds);
        queryOptions.putIfNotEmpty(FileDBAdaptor.QueryParams.JOB_ID.key(), filesCommandOptions.groupByCommandOptions.jobId);
        queryOptions.putIfNotEmpty(FileDBAdaptor.QueryParams.ATTRIBUTES.key(), filesCommandOptions.groupByCommandOptions.attributes);
        queryOptions.putIfNotEmpty(FileDBAdaptor.QueryParams.NATTRIBUTES.key(), filesCommandOptions.groupByCommandOptions.nattributes);

        return openCGAClient.getFileClient().groupBy(filesCommandOptions.groupByCommandOptions.studyId,
                filesCommandOptions.groupByCommandOptions.fields, queryOptions);
    }
    private QueryResponse variants() throws CatalogException, IOException {
        logger.debug("Fetch variants from a VCF/gVCF file");
        QueryOptions queryOptions = new QueryOptions();

        queryOptions.putIfNotEmpty("ids", filesCommandOptions.variantsCommandOptions.ids);
        queryOptions.putIfNotEmpty(CatalogVariantDBAdaptor.VariantQueryParams.REGION.key(), filesCommandOptions.variantsCommandOptions.region);
        queryOptions.putIfNotEmpty(CatalogVariantDBAdaptor.VariantQueryParams.CHROMOSOME.key(),
                filesCommandOptions.variantsCommandOptions.chromosome);
        queryOptions.putIfNotEmpty(CatalogVariantDBAdaptor.VariantQueryParams.GENE.key(), filesCommandOptions.variantsCommandOptions.gene);
        queryOptions.putIfNotEmpty(CatalogVariantDBAdaptor.VariantQueryParams.TYPE.key(), filesCommandOptions.variantsCommandOptions.type);
        queryOptions.putIfNotEmpty(CatalogVariantDBAdaptor.VariantQueryParams.REFERENCE.key(),
                filesCommandOptions.variantsCommandOptions.reference);
        queryOptions.putIfNotEmpty(CatalogVariantDBAdaptor.VariantQueryParams.ALTERNATE.key(),
                filesCommandOptions.variantsCommandOptions.alternate);
        queryOptions.putIfNotEmpty(CatalogVariantDBAdaptor.VariantQueryParams.RETURNED_STUDIES.key(),
                filesCommandOptions.variantsCommandOptions.returnedStudies);
        queryOptions.putIfNotEmpty(CatalogVariantDBAdaptor.VariantQueryParams.RETURNED_SAMPLES.key(),
                filesCommandOptions.variantsCommandOptions.returnedSamples);
        queryOptions.putIfNotEmpty(CatalogVariantDBAdaptor.VariantQueryParams.RETURNED_FILES.key(),
                filesCommandOptions.variantsCommandOptions.returnedFiles);
        queryOptions.putIfNotEmpty(CatalogVariantDBAdaptor.VariantQueryParams.FILES.key(), filesCommandOptions.variantsCommandOptions.files);
        queryOptions.putIfNotEmpty(CatalogVariantDBAdaptor.VariantQueryParams.STATS_MAF.key(), filesCommandOptions.variantsCommandOptions.maf);
        queryOptions.putIfNotEmpty(CatalogVariantDBAdaptor.VariantQueryParams.STATS_MGF.key(), filesCommandOptions.variantsCommandOptions.mgf);
        queryOptions.putIfNotEmpty(CatalogVariantDBAdaptor.VariantQueryParams.MISSING_ALLELES.key(),
                filesCommandOptions.variantsCommandOptions.missingAlleles);
        queryOptions.putIfNotEmpty(CatalogVariantDBAdaptor.VariantQueryParams.MISSING_GENOTYPES.key(),
                filesCommandOptions.variantsCommandOptions.missingGenotypes);
        queryOptions.put(CatalogVariantDBAdaptor.VariantQueryParams.ANNOTATION_EXISTS.key(),
                filesCommandOptions.variantsCommandOptions.annotationExists);
        queryOptions.putIfNotEmpty(CatalogVariantDBAdaptor.VariantQueryParams.GENOTYPE.key(),
                filesCommandOptions.variantsCommandOptions.genotype);
        queryOptions.putIfNotEmpty(CatalogVariantDBAdaptor.VariantQueryParams.ANNOT_CONSEQUENCE_TYPE.key(),
                filesCommandOptions.variantsCommandOptions.annot_ct);
        queryOptions.putIfNotEmpty(CatalogVariantDBAdaptor.VariantQueryParams.ANNOT_XREF.key(),
                filesCommandOptions.variantsCommandOptions.annot_xref);
        queryOptions.putIfNotEmpty(CatalogVariantDBAdaptor.VariantQueryParams.ANNOT_BIOTYPE.key(),
                filesCommandOptions.variantsCommandOptions.annot_biotype);
        queryOptions.putIfNotEmpty(CatalogVariantDBAdaptor.VariantQueryParams.ANNOT_POLYPHEN.key(),
                filesCommandOptions.variantsCommandOptions.polyphen);
        queryOptions.putIfNotEmpty(CatalogVariantDBAdaptor.VariantQueryParams.ANNOT_SIFT.key(), filesCommandOptions.variantsCommandOptions.sift);
        queryOptions.putIfNotEmpty(CatalogVariantDBAdaptor.VariantQueryParams.ANNOT_CONSERVATION.key(),
                filesCommandOptions.variantsCommandOptions.conservation);
        queryOptions.putIfNotEmpty(CatalogVariantDBAdaptor.VariantQueryParams.ANNOT_POPULATION_MINOR_ALLELE_FREQUENCY.key(),
                filesCommandOptions.variantsCommandOptions.annotPopulationMaf);
        queryOptions.putIfNotEmpty(CatalogVariantDBAdaptor.VariantQueryParams.ANNOT_POPULATION_ALTERNATE_FREQUENCY.key(),
                filesCommandOptions.variantsCommandOptions.alternate_frequency);
        queryOptions.putIfNotEmpty(CatalogVariantDBAdaptor.VariantQueryParams.ANNOT_POPULATION_REFERENCE_FREQUENCY.key(),
                filesCommandOptions.variantsCommandOptions.reference_frequency);
        queryOptions.putIfNotEmpty(CatalogVariantDBAdaptor.VariantQueryParams.ANNOT_TRANSCRIPTION_FLAGS.key(),
                filesCommandOptions.variantsCommandOptions.transcriptionFlags);
        queryOptions.putIfNotEmpty(CatalogVariantDBAdaptor.VariantQueryParams.ANNOT_GENE_TRAITS_ID.key(),
                filesCommandOptions.variantsCommandOptions.geneTraitId);
        queryOptions.putIfNotEmpty(CatalogVariantDBAdaptor.VariantQueryParams.ANNOT_GENE_TRAITS_NAME.key(),
                filesCommandOptions.variantsCommandOptions.geneTraitName);
        queryOptions.putIfNotEmpty(CatalogVariantDBAdaptor.VariantQueryParams.ANNOT_HPO.key(),
                filesCommandOptions.variantsCommandOptions.hpo);
        queryOptions.putIfNotEmpty(CatalogVariantDBAdaptor.VariantQueryParams.ANNOT_GO.key(),
                filesCommandOptions.variantsCommandOptions.go);
        queryOptions.putIfNotEmpty(CatalogVariantDBAdaptor.VariantQueryParams.ANNOT_EXPRESSION.key(),
                filesCommandOptions.variantsCommandOptions.expression);
        queryOptions.putIfNotEmpty(CatalogVariantDBAdaptor.VariantQueryParams.ANNOT_PROTEIN_KEYWORDS.key(),
                filesCommandOptions.variantsCommandOptions.proteinKeyword);
        queryOptions.putIfNotEmpty(CatalogVariantDBAdaptor.VariantQueryParams.ANNOT_DRUG.key(),
                filesCommandOptions.variantsCommandOptions.drug);
        queryOptions.putIfNotEmpty(CatalogVariantDBAdaptor.VariantQueryParams.ANNOT_FUNCTIONAL_SCORE.key(),
                filesCommandOptions.variantsCommandOptions.functionalScore);
        queryOptions.putIfNotEmpty(CatalogVariantDBAdaptor.VariantQueryParams.UNKNOWN_GENOTYPE.key(),
                filesCommandOptions.variantsCommandOptions.unknownGenotype);
        queryOptions.put("samplesMetadata", filesCommandOptions.variantsCommandOptions.samplesMetadata);
        queryOptions.put(QueryOptions.SORT, filesCommandOptions.variantsCommandOptions.sort);
        queryOptions.putIfNotEmpty("groupBy", filesCommandOptions.variantsCommandOptions.groupBy);
        queryOptions.put("histogram", filesCommandOptions.variantsCommandOptions.histogram);
        queryOptions.putIfNotEmpty("interval", filesCommandOptions.variantsCommandOptions.interval);
        queryOptions.putIfNotEmpty("merge", filesCommandOptions.variantsCommandOptions.merge);
        queryOptions.putIfNotEmpty(QueryOptions.INCLUDE, filesCommandOptions.variantsCommandOptions.include);
        queryOptions.putIfNotEmpty(QueryOptions.EXCLUDE, filesCommandOptions.variantsCommandOptions.exclude);
        queryOptions.putIfNotEmpty(QueryOptions.LIMIT, filesCommandOptions.variantsCommandOptions.limit);
        queryOptions.putIfNotEmpty(QueryOptions.SKIP, filesCommandOptions.variantsCommandOptions.skip);

        queryOptions.put("count", filesCommandOptions.variantsCommandOptions.count);

        return openCGAClient.getFileClient().getVariants(filesCommandOptions.variantsCommandOptions.id, queryOptions);
    }


}
