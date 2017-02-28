package org.opencb.opencga.storage.core.manager.variant;

import org.apache.commons.lang.StringUtils;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.catalog.db.api.FileDBAdaptor;
import org.opencb.opencga.catalog.db.api.SampleDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.catalog.models.File;
import org.opencb.opencga.catalog.models.FileIndex;
import org.opencb.opencga.catalog.models.Sample;
import org.opencb.opencga.storage.core.manager.CatalogUtils;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryException;

import java.util.List;
import java.util.stream.Collectors;

import static org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptorUtils.isValidParam;

/**
 * Created on 28/02/17.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class VariantCatalogQueryUtils extends CatalogUtils {

    public static final String SAMPLE_ANNOTATION = "sampleAnnotation";

    public VariantCatalogQueryUtils(CatalogManager catalogManager) {
        super(catalogManager);
    }

    /**
     * Transforms a high level Query to a query fully understandable by storage.
     * @param query     High level query. Will be modified by the method.
     * @param sessionId User's session id
     * @return          Modified input query (same instance)
     * @throws CatalogException if there is any catalog error
     */
    public Query parseQuery(Query query, String sessionId) throws CatalogException {
        if (query == null) {
            // Nothing to do!
            return null;
        }
        List<Long> studies = getStudies(query, VariantDBAdaptor.VariantQueryParams.STUDIES, sessionId);
        String defaultStudyStr;
        long defaultStudyId = -1;
        if (studies.size() == 1) {
            defaultStudyId = studies.get(0);
            defaultStudyStr = String.valueOf(studies.get(0));
        } else {
            defaultStudyStr = null;
        }

        transformFilter(query, VariantDBAdaptor.VariantQueryParams.STUDIES, value -> catalogManager.getStudyId(value, sessionId));
        transformFilter(query, VariantDBAdaptor.VariantQueryParams.RETURNED_STUDIES, value -> catalogManager.getStudyId(value, sessionId));
        transformFilter(query, VariantDBAdaptor.VariantQueryParams.COHORTS, value ->
                catalogManager.getCohortManager().getId(value, defaultStudyStr, sessionId).getResourceId());
        transformFilter(query, VariantDBAdaptor.VariantQueryParams.FILES, value ->
                catalogManager.getFileId(value, defaultStudyStr, sessionId));
        transformFilter(query, VariantDBAdaptor.VariantQueryParams.RETURNED_FILES, value ->
                catalogManager.getFileId(value, defaultStudyStr, sessionId));
        // TODO: Parse returned sample filter and add genotype filter

        if (StringUtils.isNotBlank(query.getString(SAMPLE_ANNOTATION))) {
            String sampleAnnotation = query.getString(SAMPLE_ANNOTATION);
            Query sampleQuery = parseSampleAnnotationQuery(sampleAnnotation, SampleDBAdaptor.QueryParams::getParam);
            sampleQuery.append(SampleDBAdaptor.QueryParams.STUDY_ID.key(), defaultStudyId);
            QueryOptions options = new QueryOptions(QueryOptions.INCLUDE, SampleDBAdaptor.QueryParams.ID);
            List<Long> sampleIds = catalogManager.getAllSamples(defaultStudyId, sampleQuery, options, sessionId).getResult().stream()
                    .map(Sample::getId)
                    .collect(Collectors.toList());

            if (sampleIds.isEmpty()) {
                throw new VariantQueryException("Could not found samples with this annotation: " + sampleAnnotation);
            }

            if (!isValidParam(query, VariantDBAdaptor.VariantQueryParams.RETURNED_SAMPLES)) {
                query.append(VariantDBAdaptor.VariantQueryParams.RETURNED_SAMPLES.key(), sampleIds);
            }

            Query fileQuery = new Query(FileDBAdaptor.QueryParams.INDEX_STATUS_NAME.key(), FileIndex.IndexStatus.READY)
                    .append(FileDBAdaptor.QueryParams.SAMPLE_IDS.key(), sampleIds);

            List<Long> fileIds = catalogManager.getAllFiles(defaultStudyId, fileQuery, options, sessionId).getResult().stream()
                    .map(File::getId)
                    .collect(Collectors.toList());

            if (fileIds.isEmpty()) {
                throw new VariantQueryException("Samples not indexed: " + sampleIds);
            }

            if (!isValidParam(query, VariantDBAdaptor.VariantQueryParams.FILES)) {
                query.put(VariantDBAdaptor.VariantQueryParams.FILES.key(), fileIds);
            }
            if (!isValidParam(query, VariantDBAdaptor.VariantQueryParams.RETURNED_FILES)) {
                query.put(VariantDBAdaptor.VariantQueryParams.RETURNED_FILES.key(), fileIds);
            }

//            String genotype = query.getString("sampleAnnotationGenotype");
            String genotype = query.getString(VariantDBAdaptor.VariantQueryParams.GENOTYPE.key());
            if (StringUtils.isNotBlank(genotype)) {
                StringBuilder sb = new StringBuilder();
                for (Long sampleId : sampleIds) {
                    sb.append(sampleId).append(':')
                            .append(genotype)
                            .append(';'); // TODO: Should this be an AND (;) or an OR (,)?
                }
                query.append(VariantDBAdaptor.VariantQueryParams.GENOTYPE.key(), sb.toString());
            }
        }

        return query;
    }
}
