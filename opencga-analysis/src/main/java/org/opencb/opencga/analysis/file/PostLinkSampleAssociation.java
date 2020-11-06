package org.opencb.opencga.analysis.file;

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
import org.opencb.opencga.core.models.file.File;
import org.opencb.opencga.core.models.file.FileUpdateParams;
import org.opencb.opencga.core.models.file.PostLinkToolParams;
import org.opencb.opencga.core.models.sample.Sample;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.opencb.opencga.core.tools.annotations.Tool;

import java.util.*;
import java.util.stream.Collectors;

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
        Query fileQuery = new Query(FileDBAdaptor.QueryParams.TAGS.key(), ParamConstants.FILE_SAMPLES_NOT_PROCESSED);
        QueryOptions options = new QueryOptions(FileManager.INCLUDE_FILE_URI_PATH);
        List<String> includeList = new ArrayList<>(options.getAsStringList(QueryOptions.INCLUDE));
        includeList.add(FileDBAdaptor.QueryParams.ATTRIBUTES.key());
        options.put(QueryOptions.INCLUDE, includeList);
        options.put(QueryOptions.LIMIT, 20);

        while (true) {
            OpenCGAResult<File> fileResult = catalogManager.getFileManager().search(study, fileQuery, options, token);
            if (fileResult.getNumResults() == 0) {
                break;
            }

            for (File file : fileResult.getResults()) {
                List<String> sampleList = new LinkedList<>();

                // Process samples that need to be created first
                List<Map<String, Object>> sampleMapList = (List<Map<String, Object>>) file.getAttributes()
                        .get(ParamConstants.FILE_NON_EXISTING_SAMPLES);
                for (Map<String, Object> sampleMap : sampleMapList) {
                    Sample sample = convertToSample(sampleMap);

                    Query sampleQuery = new Query(SampleDBAdaptor.QueryParams.ID.key(), sample.getId());
                    OpenCGAResult<Sample> sampleResult = catalogManager.getSampleManager().search(study, sampleQuery,
                            SampleManager.INCLUDE_SAMPLE_IDS, token);

                    if (sampleResult.getNumResults() == 1) {
                        sample = sampleResult.first();
                    } else {
                        // Sample still doesn't exist, so we create it
                        sampleResult = catalogManager.getSampleManager().create(study, sample, QueryOptions.empty(), token);
                        if (sampleResult.getNumResults() != 1) {
                            throw new CatalogException("Could not create sample '" + sample.getId() + "'");
                        }
                        sample = sampleResult.first();
                    }

                    sampleList.add(sample.getId());
                }

                // Process existing samples
                sampleMapList = (List<Map<String, Object>>) file.getAttributes().get(ParamConstants.FILE_EXISTING_SAMPLES);
                sampleList.addAll(convertToSamples(sampleMapList).stream().map(Sample::getId).collect(Collectors.toList()));

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
                        .append(FileDBAdaptor.QueryParams.SAMPLE_IDS.key(), ParamUtils.UpdateAction.ADD);

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
                Map<String, Object> attributes = file.getAttributes();
                attributes.remove(ParamConstants.FILE_EXISTING_SAMPLES);
                attributes.remove(ParamConstants.FILE_NON_EXISTING_SAMPLES);

                FileUpdateParams fileUpdateParams = new FileUpdateParams()
                        .setTags(Collections.singletonList(ParamConstants.FILE_SAMPLES_NOT_PROCESSED))
                        .setAttributes(attributes);
                actionMap = new ObjectMap()
                        .append(FileDBAdaptor.QueryParams.TAGS.key(), ParamUtils.UpdateAction.REMOVE);
                QueryOptions queryOptions = new QueryOptions(Constants.ACTIONS, actionMap);

                OpenCGAResult<File> fileUpdateResult = catalogManager.getFileManager().update(study, file.getUuid(), fileUpdateParams,
                        queryOptions, token);

                if (fileUpdateResult.getNumUpdated() != 1) {
                    throw new CatalogException("Could not update internal status of file '" + file.getPath() + "'.");
                }
            }
        }
    }

    private Sample convertToSample(Map<String, Object> sampleMap) {
        return new Sample()
                .setUid(Long.valueOf(String.valueOf(sampleMap.get(SampleDBAdaptor.QueryParams.UID.key()))))
                .setUuid(String.valueOf(sampleMap.get(SampleDBAdaptor.QueryParams.UUID.key())))
                .setId(String.valueOf(sampleMap.get(SampleDBAdaptor.QueryParams.ID.key())));
    }

    private List<Sample> convertToSamples(List<Map<String, Object>> sampleMapList) {
        List<Sample> sampleList = new ArrayList<>(sampleMapList.size());
        for (Map<String, Object> sampleMap : sampleMapList) {
            sampleList.add(convertToSample(sampleMap));
        }
        return sampleList;
    }

}
