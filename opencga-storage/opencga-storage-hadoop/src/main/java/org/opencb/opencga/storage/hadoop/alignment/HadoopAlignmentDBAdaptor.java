/*
 * Copyright 2015-2016 OpenCB
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

package org.opencb.opencga.storage.hadoop.alignment;

import org.opencb.biodata.models.alignment.RegionCoverage;
import org.opencb.biodata.models.core.Region;
import org.opencb.biodata.tools.alignment.stats.AlignmentGlobalStats;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.storage.core.alignment.AlignmentDBAdaptor;
import org.opencb.opencga.storage.core.alignment.iterators.AlignmentIterator;

import java.util.List;

/**
 * Created by imedina on 16/06/15.
 */
public class HadoopAlignmentDBAdaptor implements AlignmentDBAdaptor {

    @Override
    public QueryResult getAllAlignmentsByRegion(List<Region> regions, QueryOptions options) {
        return null;
    }

    @Override
    public QueryResult getAllAlignmentsByGene(String gene, QueryOptions options) {
        return null;
    }

    @Override
    public QueryResult getCoverageByRegion(Region region, QueryOptions options) {
        return null;
    }

    @Override
    public QueryResult getAlignmentsHistogramByRegion(Region region, boolean histogramLogarithm, int histogramMax) {
        return null;
    }

    @Override
    public QueryResult getAllIntervalFrequencies(Region region, QueryOptions options) {
        return null;
    }

    @Override
    public QueryResult getAlignmentRegionInfo(Region region, QueryOptions options) {
        return null;
    }

    @Override
    public QueryResult get(String fileId, Query query, QueryOptions options) {
        throw new UnsupportedOperationException();
    }

    @Override
    public AlignmentIterator iterator(String fileId) {
        throw new UnsupportedOperationException();
    }

    @Override
    public AlignmentIterator iterator(String fileId, Query query, QueryOptions options) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long count(String fileId, Query query, QueryOptions options) {
        throw new UnsupportedOperationException();
    }

    @Override
    public AlignmentGlobalStats stats(String fileId) {
        throw new UnsupportedOperationException();
    }

    @Override
    public RegionCoverage coverage(String fileId) throws Exception {
        throw new UnsupportedOperationException();
    }

    @Override
    public RegionCoverage coverage(String fileId, Query query, QueryOptions options) throws Exception {
        throw new UnsupportedOperationException();
    }


}
