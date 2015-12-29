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

package org.opencb.opencga.storage.core.variant.adaptors;

import org.opencb.biodata.models.variant.stats.VariantSourceStats;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.datastore.core.QueryResult;
import org.opencb.opencga.storage.core.StudyConfiguration;

import java.util.List;

/**
 * @author Cristina Yenyxe Gonzalez Garcia <cyenyxe@ebi.ac.uk>
 */
public interface VariantSourceDBAdaptor {

//    QueryResult<StudyConfiguration> getStudyConfiguration(int studyId, QueryOptions options);
//
//    QueryResult updateStudyConfiguration(StudyConfiguration studyConfiguration, QueryOptions options);

    QueryResult countSources();

    QueryResult getAllSources(QueryOptions options);

    QueryResult getAllSourcesByStudyId(String studyId, QueryOptions options);

    QueryResult getAllSourcesByStudyIds(List<String> studyIds, QueryOptions options);

    QueryResult getSamplesBySource(String fileId, QueryOptions options);

    QueryResult getSamplesBySources(List<String> fileIds, QueryOptions options);

    QueryResult getSourceDownloadUrlByName(String filename);

    List<QueryResult> getSourceDownloadUrlByName(List<String> filenames);

    QueryResult getSourceDownloadUrlById(String fileId, String studyId);

    QueryResult updateSourceStats(VariantSourceStats variantSourceStats, StudyConfiguration studyConfiguration, QueryOptions queryOptions);

    boolean close();

}
