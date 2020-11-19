package org.opencb.opencga.analysis.file;

import org.apache.commons.collections.CollectionUtils;
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
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.file.*;
import org.opencb.opencga.core.models.sample.Sample;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.opencb.opencga.core.tools.annotations.Tool;

import java.util.*;

@Tool(id = PostLinkSampleAssociation.ID, resource = Enums.Resource.FILE, type = Tool.Type.OPERATION,
        description = PostLinkSampleAssociation.DESCRIPTION)
public class PostLinkSampleAssociation extends OpenCgaToolScopeStudy {

    public static final String ID = "postlink";
    public static final String DESCRIPTION = "Associate samples to files that were linked and could not associate their samples because "
            + "the number of samples contained was too high.";

    private final PostLinkToolParams postLinkParams = new PostLinkToolParams();

    @Override
    protected void check() throws Exception {
        postLinkParams.updateParams(params);
        super.check();
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

        List<String> files = null;
        if (CollectionUtils.isNotEmpty(postLinkParams.getFiles())) {
            files = new LinkedList<>(postLinkParams.getFiles());
        }

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

            for (File file : fileResult.getResults()) {
                // Validate status
                if (!FileStatus.MISSING_SAMPLES.equals(file.getInternal().getStatus().getName())) {
                    // Skip current file. This file seems to be already properly associated
                    continue;
                }

                // Process samples that need to be created first
                if (file.getInternal() != null && file.getInternal().getMissingSamples() != null) {
                    List<String> sampleList = new LinkedList<>();

                    if (file.getInternal().getMissingSamples().getNonExisting() != null) {
                        for (String sampleId : file.getInternal().getMissingSamples().getNonExisting()) {
                            Query sampleQuery = new Query(SampleDBAdaptor.QueryParams.ID.key(), sampleId);
                            OpenCGAResult<Sample> sampleResult = catalogManager.getSampleManager().search(study, sampleQuery,
                                    SampleManager.INCLUDE_SAMPLE_IDS, token);

                            if (sampleResult.getNumResults() != 1) {
                                // Sample still doesn't exist, so we create it
                                sampleResult = catalogManager.getSampleManager().create(study, new Sample().setId(sampleId),
                                        QueryOptions.empty(), token);
                                if (sampleResult.getNumResults() != 1) {
                                    throw new CatalogException("Could not create sample '" + sampleId + "'");
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
                    int batchSize = 1000;
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


                    // Update file
                    ObjectMap actionMap = new ObjectMap()
                            .append(FileDBAdaptor.QueryParams.SAMPLE_IDS.key(), ParamUtils.BasicUpdateAction.ADD);

                    for (List<String> auxSampleList : sampleListList) {
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
}
