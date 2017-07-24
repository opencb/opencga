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

package org.opencb.opencga.storage.core.alignment;

import org.ga4gh.models.ReadAlignment;
import org.opencb.biodata.models.alignment.RegionCoverage;
import org.opencb.biodata.models.core.Region;
import org.opencb.biodata.tools.alignment.stats.AlignmentGlobalStats;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryParam;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.storage.core.alignment.iterators.AlignmentIterator;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.opencb.commons.datastore.core.QueryParam.Type.*;

/**
 * @author Cristina Yenyxe Gonzalez Garcia <cgonzalez@cipf.es>
 *
 *     TODO: Implement {@link AutoCloseable}
 */
public interface AlignmentDBAdaptor {

    enum QueryParams implements QueryParam {
//        SESSION_ID("sid", TEXT, ""),
//        FILE_ID("fileId", TEXT, ""),
        REGION("region", TEXT, ""),
        WINDOW_SIZE("windowSize", INTEGER, ""),
        MIN_MAPQ("minMapQ", INTEGER, ""),
        LIMIT("limit", INTEGER, ""),
        SKIP("skip", INTEGER, ""),
        CONTAINED("contained", BOOLEAN, ""),
        MD_FIELD("mdField", BOOLEAN, ""),
        BIN_QUALITIES("binQualities", BOOLEAN, "");

        // Fixme: Index attributes
        private static Map<String, QueryParams> map = new HashMap<>();
        static {
            for (QueryParams param : QueryParams.values()) {
                map.put(param.key(), param);
            }
        }

        // TOCHECK: Pedro. Add annotation support?

        private final String key;
        private Type type;
        private String description;

        QueryParams(String key, Type type, String description) {
            this.key = key;
            this.type = type;
            this.description = description;
        }

        @Override
        public String key() {
            return key;
        }

        @Override
        public Type type() {
            return type;
        }

        @Override
        public String description() {
            return description;
        }

        public static Map<String, QueryParams> getMap() {
            return map;
        }

        public static QueryParams getParam(String key) {
            return map.get(key);
        }
    }

    //Query Options
    String QO_BAM_PATH = "bam_path";
    String QO_BAI_PATH = "bai_path";
    String QO_VIEW_AS_PAIRS = "view_as_pairs";
    String QO_PROCESS_DIFFERENCES = "process_differences";
    String QO_FILE_ID = "file_id";
    String QO_HISTOGRAM = "histogram";
    String QO_INCLUDE_COVERAGE = "include_coverage";
    //String QO_AVERAGE = "average";
    String QO_INTERVAL_SIZE = "interval_size";
    String QO_COVERAGE_CHUNK_SIZE = "chunk_size";

    QueryResult getAllAlignmentsByRegion(List<Region> regions, QueryOptions options);

//    List<QueryResult> getAllAlignmentsByRegionList(List<Region> region, QueryOptions options);


//    QueryResult getAllAlignmentBlocksByRegion(Region region, QueryOptions options);
//
//    List<QueryResult> getAllAlignmentBlocksByRegionList(List<Region> region, QueryOptions options);

    QueryResult getAllAlignmentsByGene(String gene, QueryOptions options);

    QueryResult getCoverageByRegion(Region region, QueryOptions options);

    @Deprecated
    QueryResult getAlignmentsHistogramByRegion(Region region, boolean histogramLogarithm, int histogramMax);

    QueryResult getAllIntervalFrequencies(Region region, QueryOptions options);

    QueryResult getAlignmentRegionInfo(Region region, QueryOptions options);

    QueryResult<ReadAlignment> get(Path path, Query query, QueryOptions options);

    AlignmentIterator iterator(Path path);

    AlignmentIterator iterator(Path path, Query query, QueryOptions options);

    <T> AlignmentIterator<T> iterator(Path path, Query query, QueryOptions options, Class<T> clazz);

    QueryResult<Long> count(Path path, Query query, QueryOptions options);

    QueryResult<AlignmentGlobalStats> stats(Path path, Path workspace) throws Exception;

    QueryResult<AlignmentGlobalStats> stats(Path path, Path workspace, Query query, QueryOptions options) throws Exception;

    QueryResult<RegionCoverage> coverage(Path path, Path workspace) throws Exception;

    QueryResult<RegionCoverage> coverage(Path path, Path workspace, Query query, QueryOptions options) throws Exception;
}
