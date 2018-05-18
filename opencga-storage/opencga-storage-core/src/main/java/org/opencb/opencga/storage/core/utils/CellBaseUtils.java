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

package org.opencb.opencga.storage.core.utils;

import org.opencb.biodata.models.core.Gene;
import org.opencb.biodata.models.core.Region;
import org.opencb.cellbase.client.rest.CellBaseClient;
import org.opencb.cellbase.core.api.GeneDBAdaptor;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.*;

/**
 * Created on 30/03/17.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class CellBaseUtils {

    private static Logger logger = LoggerFactory.getLogger(CellBaseUtils.class);
    private static final int GENE_EXTRA_REGION = 5000;
    private final CellBaseClient cellBaseClient;
    public static final QueryOptions GENE_QUERY_OPTIONS = new QueryOptions(QueryOptions.INCLUDE, "name,chromosome,start,end");


    public CellBaseUtils(CellBaseClient cellBaseClient) {
        this.cellBaseClient = cellBaseClient;
    }

    public Region getGeneRegion(String geneStr) {
        List<Region> regions = getGeneRegion(Collections.singletonList(geneStr));
        if (regions.isEmpty()) {
            return null;
        } else {
            return regions.get(0);
        }
    }

    public List<Region> getGeneRegion(List<String> geneStrs) {
        List<Region> regions = new ArrayList<>(geneStrs.size());
        try {
            long ts = System.currentTimeMillis();
            List<Gene> genes = cellBaseClient.getGeneClient().get(geneStrs, GENE_QUERY_OPTIONS).allResults();
            logger.info("Query genes " + geneStrs + " -> " + (System.currentTimeMillis() - ts) / 1000.0 + "s ");
            for (Gene gene : genes) {
                if (gene != null) {
                    int start = Math.max(0, gene.getStart() - GENE_EXTRA_REGION);
                    int end = gene.getEnd() + GENE_EXTRA_REGION;
                    Region region = new Region(gene.getChromosome(), start, end);
                    regions.add(region);
                }
            }
            return regions;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public Set<String> getGenesByGo(List<String> goValues) {
        Set<String> genes = new HashSet<>();
        QueryOptions params = new QueryOptions(QueryOptions.INCLUDE, "name,chromosome,start,end");
        try {
            List<QueryResult<Gene>> responses = cellBaseClient.getGeneClient().get(goValues, params)
                    .getResponse();
            for (QueryResult<Gene> response : responses) {
                for (Gene gene : response.getResult()) {
                    genes.add(gene.getName());
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return genes;
    }

    public Set<String> getGenesByExpression(List<String> expressionValues) {
        Set<String> genes = new HashSet<>();
        QueryOptions params = new QueryOptions(QueryOptions.INCLUDE, "name,chromosome,start,end");

        // The number of results for each expression value may be huge. Query one by one
        for (String expressionValue : expressionValues) {
            try {
                String[] split = expressionValue.split(":");
                expressionValue = split[0];
                Query cellbaseQuery = new Query(2)
                        .append(GeneDBAdaptor.QueryParams.ANNOTATION_EXPRESSION_TISSUE.key(), expressionValue)
                        .append(GeneDBAdaptor.QueryParams.ANNOTATION_EXPRESSION_VALUE.key(), "UP");
                List<QueryResult<Gene>> responses = cellBaseClient.getGeneClient().search(cellbaseQuery, params)
                        .getResponse();
                for (QueryResult<Gene> response : responses) {
                    for (Gene gene : response.getResult()) {
                        genes.add(gene.getName());
                    }
                }
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
        return genes;
    }

    public CellBaseClient getCellBaseClient() {
        return cellBaseClient;
    }
}
