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
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.exceptions.NonStandardCompliantSampleField;
import org.opencb.biodata.tools.variant.VariantNormalizer;
import org.opencb.cellbase.client.rest.CellBaseClient;
import org.opencb.cellbase.client.rest.ParentRestClient;
import org.opencb.cellbase.core.ParamConstants;
import org.opencb.cellbase.core.result.CellBaseDataResponse;
import org.opencb.cellbase.core.result.CellBaseDataResult;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.storage.core.variant.adaptors.VariantField;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created on 30/03/17.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class CellBaseUtils {

    private static Logger logger = LoggerFactory.getLogger(CellBaseUtils.class);
    private static final int GENE_EXTRA_REGION = 5000;
    private final CellBaseClient cellBaseClient;
    private final String assembly;
    public static final QueryOptions GENE_QUERY_OPTIONS = new QueryOptions(QueryOptions.INCLUDE,
            "id,name,chromosome,start,end,transcripts.id,transcripts.name,transcripts.proteinId");

    private final ConcurrentHashMap<String, Region> cache = new ConcurrentHashMap<>();

    public CellBaseUtils(CellBaseClient cellBaseClient, String assembly) {
        this.cellBaseClient = cellBaseClient;
        this.assembly = assembly;
    }

    public Region getGeneRegion(String geneStr) {
        List<Region> regions = getGeneRegion(Collections.singletonList(geneStr), false);
        if (regions.isEmpty()) {
            return null;
        } else {
            return regions.get(0);
        }
    }

    public List<Region> getGeneRegion(List<String> geneStrs, boolean skipMissing) {
        return new ArrayList<>(getGeneRegionMap(geneStrs, skipMissing).values());
    }

    public Map<String, Region> getGeneRegionMap(List<String> geneStrs, boolean skipMissing) {
        if (geneStrs.size() > ParentRestClient.REST_CALL_BATCH_SIZE) {
            List<String> l1 = geneStrs.subList(0, geneStrs.size() / 2);
            List<String> l2 = geneStrs.subList(geneStrs.size() / 2, geneStrs.size());
            Map<String, Region> regions = getGeneRegionMap(l1, skipMissing);
            regions.putAll(getGeneRegionMap(l2, skipMissing));
            return regions;
        }
        geneStrs = new LinkedList<>(geneStrs);
        Map<String, Region> regions = new HashMap<>(geneStrs.size());
        Iterator<String> iterator = geneStrs.iterator();
        while (iterator.hasNext()) {
            String gene = iterator.next();
            Region region = cache.get(gene);
            if (region != null) {
                regions.put(gene, region);
                iterator.remove();
            }
        }
        if (geneStrs.isEmpty()) {
            return regions;
        }
        try {
            long ts = System.currentTimeMillis();
            QueryOptions options = new QueryOptions(GENE_QUERY_OPTIONS); // Copy options. DO NOT REUSE QUERY OPTIONS
            options.append(QueryOptions.LIMIT, ParentRestClient.REST_CALL_BATCH_SIZE * 2);

            CellBaseDataResponse<Gene> response = checkNulls(cellBaseClient.getGeneClient().get(geneStrs, new QueryOptions(options)));
            logger.info("Query genes from CellBase " + cellBaseClient.getSpecies() + ":" + assembly + " " + geneStrs + "  -> "
                    + (System.currentTimeMillis() - ts) / 1000.0 + "s ");
            List<String> missingGenes = null;
            iterator = geneStrs.iterator();
            for (CellBaseDataResult<Gene> result : response.getResponses()) {
                Gene gene = null;
//                String geneStr = result.getId(); // result.id might come empty! Do not use it
                String geneStr = iterator.next();
                // It may happen that CellBase returns more than 1 result for the same gene name.
                // Pick the gene where the given geneStr matches with the name,id,transcript.id,transcript.name or transcript.proteinId
                if (result.getResults().size() > 1) {
                    for (Gene aGene : result.getResults()) {
                        if (geneStr.equals(aGene.getName())
                                || geneStr.equals(aGene.getId())
                                || aGene.getTranscripts().stream().anyMatch(t -> geneStr.equals(t.getName()))
                                || aGene.getTranscripts().stream().anyMatch(t -> geneStr.equals(t.getId()))
                                || aGene.getTranscripts().stream().anyMatch(t -> geneStr.equals(t.getProteinId()))) {
//                            if (gene != null) {
//                                // More than one gene found!
//                                // Leave gene empty, so it is marked as "not found"
//                                gene = null;
//                                break;
//                            }
                            gene = aGene;
                            break;
                        }
                    }
                } else {
                    gene = result.first();
                }
                if (gene == null) {
                    Query query = new Query();
                    if (cellBaseClient.getClientConfiguration().getVersion().startsWith("v4")) {
                        if (geneStr.startsWith("ENSG")) {
                            query.put("id", geneStr);
                        } else if (geneStr.startsWith("ENST")) {
                            query.put("transcripts.id", geneStr);
                        } else {
                            query.put("name", geneStr);
                        }
                    } else {
                        // Filter by XREF is only available starting in CellBase V5
                        query.put("xref", geneStr);
                    }
                    QueryOptions searchQueryOptions = new QueryOptions(options).append(QueryOptions.LIMIT, 2);
                    CellBaseDataResponse<Gene> thisGeneResponse = cellBaseClient.getGeneClient().search(query, searchQueryOptions);
                    if (thisGeneResponse.first() != null && thisGeneResponse.first().getNumMatches() > 1) {
                        logger.warn("Found {} matches for gene '{}'", thisGeneResponse.first().getNumMatches(), geneStr);
                    }
                    gene = thisGeneResponse.firstResult();
                }
                if (gene == null) {
                    if (missingGenes == null) {
                        missingGenes = new ArrayList<>();
                    }
                    missingGenes.add(geneStr);
                    continue;
                }
                int start = Math.max(1, gene.getStart() - GENE_EXTRA_REGION);
                int end = gene.getEnd() + GENE_EXTRA_REGION;
                Region region = new Region(gene.getChromosome(), start, end);
                regions.put(geneStr, region);
                if (gene.getName() != null) {
                    cache.put(gene.getName(), region);
                }
                if (gene.getId() != null) {
                    cache.put(gene.getId(), region);
                }
                cache.put(geneStr, region);
            }
            if (!skipMissing && missingGenes != null) {
                throw VariantQueryException.geneNotFound(String.join(",", missingGenes),
                        cellBaseClient.getClientConfiguration().getRest().getHosts().get(0),
                        cellBaseClient.getClientConfiguration().getVersion(),
                        assembly
                );
            }
            return regions;
        } catch (IOException e) {
            throw VariantQueryException.internalException(e);
        }
    }

    public Set<String> getGenesByGo(List<String> goValues) {
        Set<String> genes = new HashSet<>();
        QueryOptions params = new QueryOptions(QueryOptions.INCLUDE, "name,chromosome,start,end");
        try {
            List<CellBaseDataResult<Gene>> responses = checkNulls(cellBaseClient.getGeneClient().get(goValues, params))
                    .getResponses();
            for (CellBaseDataResult<Gene> response : responses) {
                for (Gene gene : response.getResults()) {
                    genes.add(gene.getName());
                }
            }
        } catch (IOException e) {
            throw VariantQueryException.internalException(e);
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
                        .append(ParamConstants.ANNOTATION_EXPRESSION_TISSUE_PARAM, expressionValue)
                        .append(ParamConstants.ANNOTATION_EXPRESSION_VALUE_PARAM, "UP");
                List<CellBaseDataResult<Gene>> responses = checkNulls(cellBaseClient.getGeneClient().search(cellbaseQuery, params))
                        .getResponses();
                for (CellBaseDataResult<Gene> response : responses) {
                    for (Gene gene : response.getResults()) {
                        genes.add(gene.getName());
                    }
                }
            } catch (IOException e) {
                throw VariantQueryException.internalException(e);
            }
        }
        return genes;
    }

    public CellBaseClient getCellBaseClient() {
        return cellBaseClient;
    }

    public Variant getVariant(String variantStr) {
        return getVariants(Collections.singletonList(variantStr)).get(0);
    }

    public List<Variant> getVariants(List<String> variantsStr) {
        List<Variant> variants = new ArrayList<>(variantsStr.size());
        List<CellBaseDataResult<Variant>> response = null;
        try {
            response = checkNulls(cellBaseClient.getVariantClient().get(variantsStr,
                    new QueryOptions(QueryOptions.INCLUDE,
                            VariantField.CHROMOSOME.fieldName() + ","
                                    + VariantField.START.fieldName() + ","
                                    + VariantField.END.fieldName() + ","
                                    + VariantField.TYPE.fieldName() + ","
                                    + VariantField.REFERENCE.fieldName() + ","
                                    + VariantField.ALTERNATE.fieldName()
                    ))).getResponses();
        } catch (IOException e) {
            throw VariantQueryException.internalException(e);
        }
        VariantNormalizer variantNormalizer = new VariantNormalizer();
        for (CellBaseDataResult<Variant> result : response) {
            if (result.getResults().size() == 1) {
                Variant variant = result.getResults().get(0);
                try {
                    variants.add(variantNormalizer.normalize(Collections.singletonList(variant), true).get(0));
                } catch (NonStandardCompliantSampleField e) {
                    throw VariantQueryException.internalException(e);
                }
            } else if (result.getResults().isEmpty()) {
                throw new VariantQueryException("Unknown variant '" + result.getId() + "'");
            } else {
                throw new VariantQueryException("Not unique variant identifier '" + result.getId() + "'."
                        + " Found " + variants.size() + " results: " + variants);
            }
        }
        return variants;
    }

    private <T> CellBaseDataResponse<T> checkNulls(CellBaseDataResponse<T> response) {
        if (response == null) {
            response = new CellBaseDataResponse<>();
        }
        if (response.getResponses() == null) {
            response.setResponses(Collections.emptyList());
        } else {
            for (CellBaseDataResult<T> respons : response.getResponses()) {
                if (respons.getResults() == null) {
                    respons.setResults(Collections.emptyList());
                }
            }
        }
        if (response.getEvents() == null) {
            response.setEvents(Collections.emptyList());
        }
        return response;
    }

    public String getAssembly() {
        return assembly;
    }

    public String getSpecies() {
        return cellBaseClient.getSpecies();
    }
}
