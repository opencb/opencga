/*
 * Copyright 2015-2017 OpenCB
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

package org.opencb.opencga.storage.core.manager.variant;

import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryParam;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.catalog.db.api.*;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.core.models.*;
import org.opencb.opencga.storage.core.manager.CatalogUtils;
import org.opencb.opencga.storage.core.variant.adaptors.VariantField;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryException;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryUtils;

import java.util.*;
import java.util.stream.Collectors;

import static org.opencb.opencga.storage.core.variant.adaptors.VariantQueryUtils.*;

/**
 * Created on 28/02/17.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class VariantCatalogQueryUtils extends CatalogUtils {

    public static final String SAMPLE_ANNOTATION_DESC =
            "Selects some samples using metadata information from Catalog. e.g. age>20;phenotype=hpo:123,hpo:456;name=smith";
    public static final QueryParam SAMPLE_ANNOTATION
            = QueryParam.create("sampleAnnotation", SAMPLE_ANNOTATION_DESC, QueryParam.Type.TEXT_ARRAY);
    public static final String PROJECT_DESC = "Project [user@]project where project can be either the ID or the alias";
    public static final QueryParam PROJECT = QueryParam.create("project", PROJECT_DESC, QueryParam.Type.TEXT_ARRAY);
    private final StudyTransformFilter studyTransformFilter;
    private final FileTransformFilter fileTransformFilter;
    private final SampleTransformFilter sampleTransformFilter;
    private final CohortTransformFilter cohortTransformFilter;
    //    public static final QueryParam SAMPLE_FILTER_GENOTYPE = QueryParam.create("sampleFilterGenotype", "", QueryParam.Type.TEXT_ARRAY);

    public VariantCatalogQueryUtils(CatalogManager catalogManager) {
        super(catalogManager);

        studyTransformFilter = new StudyTransformFilter();
        fileTransformFilter = new FileTransformFilter();
        sampleTransformFilter = new SampleTransformFilter();
        cohortTransformFilter = new CohortTransformFilter();
    }

    public static VariantQueryException wrongReleaseException(VariantQueryParam param, String value, int release) {
        return new VariantQueryException("Unable to have '" + value + "' within '" + param.key() + "' filter. "
                + "Not part of release " + release);
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
        List<Long> studies = getStudies(query, sessionId);
        long defaultStudyId = getDefaultStudyId(studies);
        String defaultStudyStr = defaultStudyId > 0 ? String.valueOf(defaultStudyId) : null;
        Integer release = getReleaseFilter(query, sessionId);

        studyTransformFilter.processFilter(query, VariantQueryParam.STUDY, release, sessionId, defaultStudyStr);
        studyTransformFilter.processFilter(query, VariantQueryParam.INCLUDE_STUDY, release, sessionId, defaultStudyStr);
        sampleTransformFilter.processFilter(query, VariantQueryParam.SAMPLE, release, sessionId, defaultStudyStr);
        sampleTransformFilter.processFilter(query, VariantQueryParam.INCLUDE_SAMPLE, release, sessionId, defaultStudyStr);
        //TODO: Parse genotype filter
        //sampleTransformFilter.processFilter(query, VariantQueryParam.GENOTYPE, release, sessionId, defaultStudyStr);
        fileTransformFilter.processFilter(query, VariantQueryParam.FILE, release, sessionId, defaultStudyStr);
        fileTransformFilter.processFilter(query, VariantQueryParam.INCLUDE_FILE, release, sessionId, defaultStudyStr);
        cohortTransformFilter.processFilter(query, VariantQueryParam.COHORT, release, sessionId, defaultStudyStr);
        cohortTransformFilter.processFilter(query, VariantQueryParam.STATS_MAF, release, sessionId, defaultStudyStr);
        cohortTransformFilter.processFilter(query, VariantQueryParam.STATS_MGF, release, sessionId, defaultStudyStr);
        cohortTransformFilter.processFilter(query, VariantQueryParam.MISSING_ALLELES, release, sessionId, defaultStudyStr);
        cohortTransformFilter.processFilter(query, VariantQueryParam.MISSING_GENOTYPES, release, sessionId, defaultStudyStr);

        if (release != null) {
            // If no list of included files is specified:
            if (VariantQueryUtils.isIncludeFilesDefined(query, Collections.singleton(VariantField.STUDIES_FILES))) {
                List<String> includeFiles = new ArrayList<>();
                QueryOptions fileOptions = new QueryOptions(QueryOptions.INCLUDE, FileDBAdaptor.QueryParams.UID.key());
                Query fileQuery = new Query(FileDBAdaptor.QueryParams.RELEASE.key(), "<=" + release)
                        .append(FileDBAdaptor.QueryParams.INDEX_STATUS_NAME.key(), FileIndex.IndexStatus.READY);

                for (Long study : studies) {
                    for (File file : catalogManager.getFileManager().get(study, fileQuery, fileOptions, sessionId).getResult()) {
                        includeFiles.add(String.valueOf(file.getUid()));
                    }
                }
                query.append(VariantQueryParam.INCLUDE_FILE.key(), includeFiles);
            }
            // If no list of included samples is specified:
            if (!VariantQueryUtils.isIncludeSamplesDefined(query, Collections.singleton(VariantField.STUDIES_SAMPLES_DATA))) {
                List<String> includeSamples = new ArrayList<>();
                Query sampleQuery = new Query(SampleDBAdaptor.QueryParams.RELEASE.key(), "<=" + release);
                QueryOptions sampleOptions = new QueryOptions(QueryOptions.INCLUDE, SampleDBAdaptor.QueryParams.UID.key());

                for (Long study : studies) {
                    Query cohortQuery = new Query(CohortDBAdaptor.QueryParams.ID.key(), StudyEntry.DEFAULT_COHORT);
                    QueryOptions cohortOptions = new QueryOptions(QueryOptions.INCLUDE, CohortDBAdaptor.QueryParams.SAMPLE_UIDS.key());
                    // Get default cohort. It contains the list of indexed samples. If it doesn't exist, or is empty, do not include any
                    // sample from this study.
                    QueryResult<Cohort> result = catalogManager.getCohortManager().get(study, cohortQuery, cohortOptions, sessionId);
                    if (result.first() != null || result.first().getSamples().isEmpty()) {
                        Set<Long> sampleIds = result
                                .first()
                                .getSamples()
                                .stream()
                                .map(Sample::getUid)
                                .collect(Collectors.toSet());
                        for (Sample s : catalogManager.getSampleManager().get(study, sampleQuery, sampleOptions, sessionId).getResult()) {
                            if (sampleIds.contains(s.getUid())) {
                                includeSamples.add(String.valueOf(s.getUid()));
                            }
                        }
                    }
                }
                query.append(VariantQueryParam.INCLUDE_SAMPLE.key(), includeSamples);
            }
        }

        if (isValidParam(query, SAMPLE_ANNOTATION)) {
            String sampleAnnotation = query.getString(SAMPLE_ANNOTATION.key());
            Query sampleQuery = parseSampleAnnotationQuery(sampleAnnotation, SampleDBAdaptor.QueryParams::getParam);
            sampleQuery.append(SampleDBAdaptor.QueryParams.STUDY_ID.key(), defaultStudyId);
            QueryOptions options = new QueryOptions(QueryOptions.INCLUDE, SampleDBAdaptor.QueryParams.UID);
            List<Long> sampleIds = catalogManager.getSampleManager().get(defaultStudyId, sampleQuery, options, sessionId).getResult()
                    .stream()
                    .map(Sample::getUid)
                    .collect(Collectors.toList());

            if (sampleIds.isEmpty()) {
                throw new VariantQueryException("Could not found samples with this annotation: " + sampleAnnotation);
            }

            String genotype = query.getString("sampleAnnotationGenotype");
//            String genotype = query.getString(VariantDBAdaptor.VariantQueryParams.GENOTYPE.key());
            if (StringUtils.isNotBlank(genotype)) {
                StringBuilder sb = new StringBuilder();
                for (Long sampleId : sampleIds) {
                    sb.append(sampleId).append(IS)
                            .append(genotype)
                            .append(AND); // TODO: Should this be an AND (;) or an OR (,)?
                }
                query.append(VariantQueryParam.GENOTYPE.key(), sb.toString());
                if (!isValidParam(query, VariantQueryParam.INCLUDE_SAMPLE)) {
                    query.append(VariantQueryParam.INCLUDE_SAMPLE.key(), sampleIds);
                }
            } else {
                query.append(VariantQueryParam.SAMPLE.key(), sampleIds);
            }
        }

        return query;
    }

    public long getDefaultStudyId(Collection<Long> studies) throws CatalogException {
        final long defaultStudyId;
        if (studies.size() == 1) {
            defaultStudyId = studies.iterator().next();
        } else {
            defaultStudyId = -1;
        }
        return defaultStudyId;
    }

    public Integer getReleaseFilter(Query query, String sessionId) throws CatalogException {
        Integer release;
        if (isValidParam(query, VariantQueryParam.RELEASE)) {
            release = query.getInt(VariantQueryParam.RELEASE.key(), -1);
            if (release <= 0) {
                throw VariantQueryException.malformedParam(VariantQueryParam.RELEASE, query.getString(VariantQueryParam.RELEASE.key()));
            }
            Project project = getProjectFromQuery(query, sessionId,
                    new QueryOptions(QueryOptions.INCLUDE, ProjectDBAdaptor.QueryParams.CURRENT_RELEASE.key()));
            int currentRelease = project.getCurrentRelease();
            if (release > currentRelease) {
                throw VariantQueryException.malformedParam(VariantQueryParam.RELEASE, query.getString(VariantQueryParam.RELEASE.key()));
            } else if (release == currentRelease) {
                // Using latest release. We don't need to filter by release!
                release = null;
            } // else, filter by release

        } else {
            release = null;
        }
        return release;
    }

    public abstract class TransformFilter {
        protected final QueryOptions OPTIONS = new QueryOptions(QueryOptions.INCLUDE, FileDBAdaptor.QueryParams.RELEASE.key());

        /**
         * Splits the value from the query (if any) and translates the IDs to numerical Ids.
         * If a release value is given, checks that every element is part of that release.
         * @param query        Query with the data
         * @param param        Param to modify
         * @param release      Release filter, if any
         * @param sessionId    SessionId
         * @param defaultStudy Default study
         * @throws CatalogException if there is any catalog error
         */
        protected void processFilter(Query query, VariantQueryParam param, Integer release, String sessionId, String defaultStudy)
                throws CatalogException {
            if (VariantQueryUtils.isValidParam(query, param)) {
                String valuesStr = query.getString(param.key());
                // Do not try to transform ALL or NONE values
                if (isNoneOrAll(valuesStr)) {
                    return;
                }
                VariantQueryUtils.QueryOperation queryOperation = VariantQueryUtils.checkOperator(valuesStr);
                if (queryOperation == null) {
                    queryOperation = VariantQueryUtils.QueryOperation.OR;
                }
                List<String> values = VariantQueryUtils.splitValue(valuesStr, queryOperation);
                StringBuilder sb = new StringBuilder();
                for (String value : values) {
                    if (sb.length() > 0) {
                        sb.append(queryOperation.separator());
                    }
                    if (isNegated(value)) {
                        sb.append(NOT);
                        value = removeNegation(value);
                    }
                    String[] strings = VariantQueryUtils.splitOperator(value);
                    boolean withComparisionOperator = strings[0] != null;
                    if (withComparisionOperator) {
                        value = strings[0];
                        withComparisionOperator = true;
                    }

                    Long id;
                    if (StringUtils.isNumeric(value)) {
                        id = Long.parseLong(value);
                        sb.append(value);
                    } else {
                        id = toId(defaultStudy, value, sessionId);
                        sb.append(id);
                    }
                    if (!releaseMatches(id, release, sessionId)) {
                        throw wrongReleaseException(param, value, release);
                    }

                    if (withComparisionOperator) {
                        sb.append(strings[1]);
                        sb.append(strings[2]);
                    }
                }
                query.put(param.key(), sb.toString());
            }
        }

        protected abstract boolean releaseMatches(Long id, Integer release, String sessionId) throws CatalogException;

        protected abstract Long toId(String defaultStudyStr, String value, String sessionId) throws CatalogException;
    }


    public class StudyTransformFilter extends TransformFilter {
        @Override
        protected Long toId(String defaultStudyStr, String value, String sessionId) throws CatalogException {
            return catalogManager.getStudyManager().getId(catalogManager.getUserManager().getUserId(sessionId), value);
        }

        @Override
        protected boolean releaseMatches(Long id, Integer release, String sessionId) throws CatalogException {
            if (release == null) {
                return true;
            }

            return catalogManager.getStudyManager().get(id.toString(), OPTIONS, sessionId).first().getRelease() <= release;
        }
    }

    public class FileTransformFilter extends TransformFilter {
        protected final QueryOptions OPTIONS = new QueryOptions(QueryOptions.INCLUDE,
                Arrays.asList(FileDBAdaptor.QueryParams.RELEASE.key(), FileDBAdaptor.QueryParams.INDEX.key()));

        @Override
        protected Long toId(String defaultStudyStr, String value, String sessionId) throws CatalogException {
            return catalogManager.getFileManager().getUid(value, defaultStudyStr, sessionId).getResourceId();
        }

        @Override
        protected boolean releaseMatches(Long id, Integer release, String sessionId) throws CatalogException {
            if (release == null) {
                return true;
            }

            File file = catalogManager.getFileManager().get(id, OPTIONS, sessionId).first();
            return file.getIndex() != null && file.getIndex().getRelease() <= release;
//            return file.getRelease() <= release;
//            return catalogManager.getFileManager().count(defaultStudyStr,
//                    new Query(FileDBAdaptor.QueryParams.ID.key(), id)
//                            .append(FileDBAdaptor.QueryParams.RELEASE.key(), release), sessionId).getNumTotalResults() == 1;
        }
    }

    public class SampleTransformFilter extends TransformFilter {

        @Override
        protected Long toId(String defaultStudyStr, String value, String sessionId) throws CatalogException {
            return catalogManager.getSampleManager().getUid(value, defaultStudyStr, sessionId).getResourceId();
        }

        @Override
        protected boolean releaseMatches(Long id, Integer release, String sessionId) throws CatalogException {
            if (release == null) {
                return true;
            }
            return catalogManager.getSampleManager().get(id, OPTIONS, sessionId).first().getRelease() <= release;
        }
    }

    public class CohortTransformFilter extends TransformFilter {
        @Override
        protected Long toId(String defaultStudyStr, String value, String sessionId) throws CatalogException {
            return catalogManager.getCohortManager().getUid(value, defaultStudyStr, sessionId).getResourceId();
        }

        @Override
        protected boolean releaseMatches(Long id, Integer release, String sessionId) throws CatalogException {
            if (release == null) {
                return true;
            }

            Long studyId = catalogManager.getCohortManager().getStudyId(id);
            return catalogManager.getCohortManager().get(studyId, new Query(CohortDBAdaptor.QueryParams.UID.key(), id), OPTIONS, sessionId)
                    .first().getRelease() <= release;
        }
    }

}
