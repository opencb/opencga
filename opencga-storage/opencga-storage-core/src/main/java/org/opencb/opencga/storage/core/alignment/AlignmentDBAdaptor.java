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
import org.opencb.biodata.tools.alignment.exceptions.AlignmentCoverageException;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.core.exception.ToolException;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.opencb.opencga.storage.core.alignment.iterators.AlignmentIterator;

import java.io.IOException;
import java.nio.file.Path;


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

    OpenCGAResult<ReadAlignment> get(Path path, Query query, QueryOptions options);

    AlignmentIterator iterator(Path path);

    AlignmentIterator iterator(Path path, Query query, QueryOptions options);

    <T> AlignmentIterator<T> iterator(Path path, Query query, QueryOptions options, Class<T> clazz);

    OpenCGAResult<Long> count(Path path, Query query, QueryOptions options);

    OpenCGAResult<String> statsInfo(Path path) throws ToolException;

    OpenCGAResult<RegionCoverage> coverageQuery(Path path, Region region, int minCoverage, int maxCoverage, int windowSize)
            throws Exception;

    OpenCGAResult<Long> getTotalCounts(Path path) throws AlignmentCoverageException, IOException;
}
