package org.opencb.opencga.analysis.file;

import org.apache.commons.collections4.CollectionUtils;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.tools.OpenCgaToolScopeStudy;
import org.opencb.opencga.catalog.db.api.FileDBAdaptor;
import org.opencb.opencga.catalog.db.api.SampleDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.FileManager;
import org.opencb.opencga.catalog.managers.SampleManager;
import org.opencb.opencga.catalog.utils.Constants;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.file.*;
import org.opencb.opencga.core.models.sample.Sample;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.opencb.opencga.core.tools.annotations.Tool;
import org.opencb.opencga.core.tools.annotations.ToolParams;

import java.util.*;

@Tool(id = PostLinkSampleAssociation.ID, resource = Enums.Resource.FILE, type = Tool.Type.OPERATION,
        description = PostLinkSampleAssociation.DESCRIPTION)
public class PostLinkSampleAssociation extends OpenCgaToolScopeStudy {

    public static final String ID = "postlink";
    public static final String DESCRIPTION = "Associate samples to files that were linked and could not associate their samples because "
            + "the number of samples contained was too high.";

    @ToolParams
    protected final PostLinkToolParams postLinkParams = new PostLinkToolParams();

    @Override
    protected void check() throws Exception {
        super.check();
        // Add default batch size
        if (postLinkParams.getBatchSize() == null || postLinkParams.getBatchSize() <= 0) {
            postLinkParams.setBatchSize(1000);
        }
    }

    @Override
    protected void run() throws Exception {
        // Obtain an iterator to get all the files that were link and not associated to any of its samples
        Query fileQuery = new Query(FileDBAdaptor.QueryParams.INTERNAL_STATUS_NAME.key(), FileStatus.MISSING_SAMPLES);
        QueryOptions options = new QueryOptions(FileManager.INCLUDE_FILE_URI_PATH);
        List<String> includeList = new ArrayList<>(options.getAsStringList(QueryOptions.INCLUDE));
        includeList.add(FileDBAdaptor.QueryParams.INTERNAL_MISSING_SAMPLES.key());
        includeList.add(FileDBAdaptor.QueryParams.INTERNAL_STATUS.key());
        options.put(QueryOptions.INCLUDE, includeList);
        options.put(QueryOptions.LIMIT, 20);
        options.put(QueryOptions.COUNT, true);

        List<String> files = null;
        if (CollectionUtils.isEmpty(postLinkParams.getFiles())
                || postLinkParams.getFiles().size() == 1 && postLinkParams.getFiles().get(0).equals(ParamConstants.ALL)) {
            logger.info("Processing all files with internal status = '" + FileStatus.MISSING_SAMPLES + "'");
        } else {
            files = new LinkedList<>(postLinkParams.getFiles());
        }

        int numPendingFiles = -1;
        int numFiles = 0;
        while (true) {
            OpenCGAResult<File> fileResult;
            if (files == null) {
                // We need to associate all non-associated files
                fileResult = catalogManager.getFileManager().search(study, fileQuery, options, token);
                if (fileResult.getNumResults() == 0) {
                    break;
                }
            } else {
                // We will only process the files provided
                if (files.isEmpty()) {
                    break;
                }
                fileResult = catalogManager.getFileManager().get(study, files.remove(0), options, token);
            }
            if (numPendingFiles < 0) {
                numPendingFiles = ((int) fileResult.getNumMatches());
            }

            for (File file : fileResult.getResults()) {
                numFiles++;
                logger.info("Processing file {}/{} - {}", numFiles, numPendingFiles, file.getId());
                // Validate status
                if (!FileStatus.MISSING_SAMPLES.equals(file.getInternal().getStatus().getName())) {
                    // Skip current file. This file seems to be already properly associated
                    continue;
                }

                // Process samples that need to be created first
                if (file.getInternal() != null && file.getInternal().getMissingSamples() != null) {
                    List<String> sampleList = new LinkedList<>();

                    if (CollectionUtils.isNotEmpty(file.getInternal().getMissingSamples().getNonExisting())) {
                        logger.info("Create {} missing samples", file.getInternal().getMissingSamples().getNonExisting().size());
                        for (String sampleId : file.getInternal().getMissingSamples().getNonExisting()) {
                            if (!sampleExists(sampleId)) {
                                try {
                                    // Sample still doesn't exist, so we create it
                                    OpenCGAResult<Sample> sampleResult = catalogManager.getSampleManager().create(study, new Sample().setId(sampleId),
                                            QueryOptions.empty(), token);
                                    if (sampleResult.getNumResults() != 1) {
                                        throw new CatalogException("Could not create sample '" + sampleId + "'");
                                    }
                                } catch (CatalogException e) {
                                    try {
                                        if (sampleExists(sampleId)) {
                                            // If sample was successfully created, but still got an exception.
                                            // Ignore exception

                                            // Log INFO without stack trace
                                            logger.info("Caught exception creating sample \"" + sampleId + "\","
                                                    + " but sample was actually created. Ignoring " + e.toString());

                                            // Log DEBUG with full stack trace
                                            logger.debug("Ignored exception", e);
                                        } else {
                                            // Sample could not be created.
                                            // Throw exception
                                            throw e;
                                        }
                                    } catch (Exception e1) {
                                        // Something went wrong. Throw original exception, and add this new as suppressed
                                        e.addSuppressed(e1);
                                        throw e;
                                    }
                                }
                            }

                            sampleList.add(sampleId);
                        }
                    }

                    if (file.getInternal().getMissingSamples().getExisting() != null) {
                        // Process existing samples
                        sampleList.addAll(file.getInternal().getMissingSamples().getExisting());
                    }

                    // Create sample batches
                    int batchSize = postLinkParams.getBatchSize();
                    List<List<String>> sampleListList = new ArrayList<>((sampleList.size() / batchSize) + 1);
                    // Create batches
                    List<String> currentList = null;
                    for (int i = 0; i < sampleList.size(); i++) {
                        if (i % batchSize == 0) {
                            currentList = new ArrayList<>(batchSize);
                            sampleListList.add(currentList);
                        }

                        currentList.add(sampleList.get(i));
                    }
                    if (!sampleList.isEmpty()) {
                        logger.info("Update {} samples in {} batches", sampleList.size(), sampleListList.size());
                    }

                    // Update file
                    ObjectMap actionMap = new ObjectMap()
                            .append(FileDBAdaptor.QueryParams.SAMPLE_IDS.key(), ParamUtils.BasicUpdateAction.ADD);

                    int sampleListCount = 0;
                    for (List<String> auxSampleList : sampleListList) {
                        sampleListCount++;
                        logger.info("Update batch {}/{} with {} samples", sampleListCount, sampleListList.size(), auxSampleList.size());
                        FileUpdateParams fileUpdateParams = new FileUpdateParams()
                                .setSampleIds(auxSampleList);

                        QueryOptions queryOptions = new QueryOptions(Constants.ACTIONS, actionMap);

                        OpenCGAResult<File> fileUpdateResult = catalogManager.getFileManager().update(study, file.getUuid(),
                                fileUpdateParams, queryOptions, token);
                        if (fileUpdateResult.getNumUpdated() != 1) {
                            throw new CatalogException("Could not update sample list of file '" + file.getPath() + "'.");
                        }
                    }

                    // Now that all the samples are updated, we update the internal status
                    FileUpdateParams fileUpdateParams = new FileUpdateParams()
                            .setInternal(new SmallFileInternal(new FileStatus(FileStatus.READY), MissingSamples.initialize()));

                    OpenCGAResult<File> fileUpdateResult = catalogManager.getFileManager().update(study, file.getUuid(), fileUpdateParams,
                            QueryOptions.empty(), token);
                    if (fileUpdateResult.getNumUpdated() != 1) {
                        throw new CatalogException("Could not update internal status of file '" + file.getPath() + "'.");
                    }
                }
            }
        }
    }

    private boolean sampleExists(String sampleId) throws CatalogException {
        Query sampleQuery = new Query(SampleDBAdaptor.QueryParams.ID.key(), sampleId);
        OpenCGAResult<Sample> sampleResult = catalogManager.getSampleManager().search(study, sampleQuery,
                SampleManager.INCLUDE_SAMPLE_IDS, token);

        return sampleResult.getNumResults() == 1;
    }
}
