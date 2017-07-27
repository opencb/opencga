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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Created on 30/03/17.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class CellBaseUtils {

    private static final int GENE_EXTRA_REGION = 5000;
    private final CellBaseClient cellBaseClient;

    public CellBaseUtils(CellBaseClient cellBaseClient) {
        this.cellBaseClient = cellBaseClient;
    }

    public Region getGeneRegion(String geneStr) {
        QueryOptions params = new QueryOptions(QueryOptions.INCLUDE, "name,chromosome,start,end");
        try {
            Gene gene = cellBaseClient.getGeneClient().get(Collections.singletonList(geneStr), params).firstResult();
            if (gene != null) {
                int start = Math.max(0, gene.getStart() - GENE_EXTRA_REGION);
                int end = gene.getEnd() + GENE_EXTRA_REGION;
                return new Region(gene.getChromosome(), start, end);
            } else {
                return null;
            }
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
                // TODO: Add expression value {UP, DOWN}. See https://github.com/opencb/cellbase/issues/245
                Query cellbaseQuery = new Query(GeneDBAdaptor.QueryParams.ANNOTATION_EXPRESSION_TISSUE.key(), expressionValue);
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
