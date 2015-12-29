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

package org.opencb.opencga.storage.core.alignment.adaptors;

import org.opencb.biodata.models.core.Region;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.datastore.core.QueryResult;

import java.util.List;

/**
 * @author Cristina Yenyxe Gonzalez Garcia <cgonzalez@cipf.es>
 */
public interface AlignmentDBAdaptor {

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

}
