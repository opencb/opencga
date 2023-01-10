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

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.opencb.biodata.models.core.Gene;
import org.opencb.biodata.models.core.Region;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.exceptions.NonStandardCompliantSampleField;
import org.opencb.biodata.tools.variant.VariantNormalizer;
import org.opencb.cellbase.client.rest.CellBaseClient;
import org.opencb.cellbase.client.rest.ParentRestClient;
import org.opencb.cellbase.core.ParamConstants;
import org.opencb.cellbase.core.config.SpeciesProperties;
import org.opencb.cellbase.core.result.CellBaseDataResponse;
import org.opencb.cellbase.core.result.CellBaseDataResult;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.core.common.VersionUtils;
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

    private final ConcurrentHashMap<String, GeneReference> cache = new ConcurrentHashMap<>();
    private String serverVersion;

    public static class GeneReference {
        private final Region region;
        private final String id;
        private final String name;

        public GeneReference(Region region, String id, String name) {
            this.region = region;
            this.id = id;
            this.name = name;
        }

        public GeneReference(Gene gene) {
            int start = Math.max(1, gene.getStart() - GENE_EXTRA_REGION);
            int end = gene.getEnd() + GENE_EXTRA_REGION;
            region = new Region(gene.getChromosome(), start, end);
            id = gene.getId();
            name = gene.getName();
        }

        public Region getRegion() {
            return region;
        }

        public String getId() {
            return id;
        }

        public String getName() {
            return name;
        }

        @Override
        public String toString() {
            return new ToStringBuilder(this)
                    .append("region", region)
                    .append("id", id)
                    .append("name", name)
                    .toString();
        }
    }

    public CellBaseUtils(CellBaseClient cellBaseClient) {
        this.cellBaseClient = cellBaseClient;
        this.assembly = cellBaseClient.getAssembly();
    }

    public static String toCellBaseSpeciesName(String scientificName) {
        if (scientificName != null && scientificName.contains(" ")) {
            String[] split = scientificName.split(" ", 2);
            scientificName = (split[0].charAt(0) + split[1]).toLowerCase();
        }
        return scientificName;
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
        Map<String, GeneReference> genes = getGenes(geneStrs, skipMissing);
        Map<String, Region> map = new HashMap<>(genes.size());
        genes.forEach((k, v) -> map.put(k, v.getRegion()));
        return map;
    }

    public Map<String, GeneReference> getGenes(List<String> geneStrs, boolean skipMissing) {
        if (geneStrs.size() > ParentRestClient.REST_CALL_BATCH_SIZE) {
            List<String> l1 = geneStrs.subList(0, geneStrs.size() / 2);
            List<String> l2 = geneStrs.subList(geneStrs.size() / 2, geneStrs.size());
            Map<String, GeneReference> regions = getGenes(l1, skipMissing);
            regions.putAll(getGenes(l2, skipMissing));
            return regions;
        }
        geneStrs = new LinkedList<>(geneStrs);
        Map<String, GeneReference> genesMap = new HashMap<>(geneStrs.size());
        Iterator<String> iterator = geneStrs.iterator();
        while (iterator.hasNext()) {
            String gene = iterator.next();
            GeneReference geneRef = cache.get(gene);
            if (geneRef != null) {
                genesMap.put(gene, geneRef);
                iterator.remove();
            }
        }
        if (geneStrs.isEmpty()) {
            return genesMap;
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
                    if (isMinVersion("5.0.0")) {
                        // Filter by XREF is only available starting in CellBase V5
                        query.put("xref", geneStr);
                    } else {
                        if (geneStr.startsWith("ENSG")) {
                            query.put("id", geneStr);
                        } else if (geneStr.startsWith("ENST")) {
                            query.put("transcripts.id", geneStr);
                        } else {
                            query.put("name", geneStr);
                        }
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
                GeneReference geneRef = cacheGene(gene, geneStr);
                genesMap.put(geneStr, geneRef);
            }
            if (!skipMissing && missingGenes != null) {
                throw VariantQueryException.geneNotFound(String.join(",", missingGenes),
                        cellBaseClient.getClientConfiguration().getRest().getHosts().get(0),
                        cellBaseClient.getClientConfiguration().getVersion(),
                        assembly
                );
            }
            return genesMap;
        } catch (IOException e) {
            throw VariantQueryException.internalException(e);
        }
    }

    private GeneReference cacheGene(Gene gene, String geneStr) {
        GeneReference geneRef = cacheGene(gene);
        cache.put(geneStr, geneRef);
        return geneRef;
    }

    private GeneReference cacheGene(Gene gene) {
        GeneReference geneRef = new GeneReference(gene);
        if (gene.getName() != null) {
            cache.put(gene.getName(), geneRef);
        }
        if (gene.getId() != null) {
            cache.put(gene.getId(), geneRef);
        }
        return geneRef;
    }

    public List<String> validateGenes(List<String> geneStrs, boolean skipMissing) {
        Map<String, CellBaseUtils.GeneReference> geneRefs = getGenes(geneStrs, skipMissing);

        List<String> validatedGenes = new ArrayList<>();
        for (Map.Entry<String, CellBaseUtils.GeneReference> entry : geneRefs.entrySet()) {
            String geneFromQuery = entry.getKey();
            CellBaseUtils.GeneReference geneReference = entry.getValue();
            if (geneFromQuery.equals(geneReference.getId()) || geneFromQuery.equals(geneReference.getName())) {
                // Valid gene name
                validatedGenes.add(geneFromQuery);
            } else {
                // The input gene might be a xref. Replace with the actual geneName or geneId
                if (geneReference.getName() != null) {
                    validatedGenes.add(geneReference.getName());
                } else {
                    validatedGenes.add(geneReference.getId());
                }
            }
        }
        return validatedGenes;
    }

    public Set<String> getGenesByGo(List<String> goValues) {
        Set<String> genes = new HashSet<>();
        try {
            List<CellBaseDataResult<Gene>> responses;
            if (isMinVersion("5.0.0")) {
                Query query = new Query("transcriptAnnotationOntologiesId", String.join(",", goValues));
                responses = checkNulls(cellBaseClient.getGeneClient().search(query, GENE_QUERY_OPTIONS))
                        .getResponses();
            } else {
                responses = checkNulls(cellBaseClient.getGeneClient().get(goValues, GENE_QUERY_OPTIONS))
                        .getResponses();
            }
            for (CellBaseDataResult<Gene> response : responses) {
                for (Gene gene : response.getResults()) {
                    genes.add(cacheGene(gene).getName());
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

    public Set<String> getGenesByRoleInCancer(List<String> roleInCancer) {
        if (CollectionUtils.isEmpty(roleInCancer)) {
            return Collections.emptySet();
        }
        Set<String> genes = new HashSet<>();
        try {
            if (!isMinVersion("5.2.7-SNAPSHOT")) {
                throw new VariantQueryException("Unable to fetch genes by role-in-cancer. "
                        + "Requires at least cellbase version 5.2.7");
            }
            Query query = new Query("roleInCancer", String.join(",", roleInCancer));
            List<CellBaseDataResult<Gene>> responses =
                    checkNulls(cellBaseClient.getGeneClient().search(query, GENE_QUERY_OPTIONS)).getResponses();
            for (CellBaseDataResult<Gene> response : responses) {
                for (Gene gene : response.getResults()) {
                    genes.add(cacheGene(gene).getName());
                }
            }
        } catch (IOException e) {
            throw VariantQueryException.internalException(e);
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

    public String getDataRelease() {
        return cellBaseClient.getDataRelease();
    }

    public String getURL() {
        return cellBaseClient.getClientConfiguration().getRest().getHosts().get(0);
    }

    public String getVersion() {
        return cellBaseClient.getClientConfiguration().getVersion();
    }

    public void validateCellBaseConnection() throws IOException {
        CellBaseDataResponse<SpeciesProperties> species = cellBaseClient.getMetaClient().species();
        if (species == null || species.firstResult() == null) {
            throw new IllegalArgumentException("Unable to access cellbase url '" + getURL() + "', version '" + getVersion() + "'");
        }
        String serverVersion = getVersionFromServerMajorMinor();
        if (!supportsDataRelease(serverVersion)) {
            logger.warn("DataRelease not supported on version '" + serverVersion + ".x'");
        } else {
            if (getDataRelease() == null) {
                throw new IllegalArgumentException("Missing DataRelease for cellbase "
                        + "url: '" + getURL() + "'"
                        + ", version: '" + getVersion()
                        + "', species: '" + getSpecies()
                        + "', assembly: '" + getAssembly() + "'");
            }
            boolean dataReleaseExists = cellBaseClient.getMetaClient().dataReleases()
                    .allResults()
                    .stream()
                    .anyMatch(dataRelease -> {
                        return getDataRelease().equals(String.valueOf(dataRelease.getRelease()));
                    });
            if (!dataReleaseExists) {
                throw new IllegalArgumentException("DataRelease '" + getDataRelease() + "' not found on cellbase "
                        + "url: '" + getURL() + "'"
                        + ", version: '" + getVersion()
                        + "', species: '" + getSpecies()
                        + "', assembly: '" + getAssembly() + "'");
            }
        }
    }

    public boolean supportsDataRelease() throws IOException {
        return supportsDataRelease(getVersionFromServer());
    }

    public static boolean supportsDataRelease(String serverVersion) {
        return !majorMinor(serverVersion).equals("5.0") && !major(serverVersion).equals("4");
    }

    public String getVersionFromServerMajor() throws IOException {
        return major(getVersionFromServer());
    }

    public String getVersionFromServerMajorMinor() throws IOException {
        String serverVersion = getVersionFromServer();
        serverVersion = majorMinor(serverVersion);
        return serverVersion;
    }

    private static String major(String version) {
        return version.split("\\.")[0];
    }

    private static String majorMinor(String version) {
        String[] split = version.split("\\.");
        if (split.length > 1) {
            version = split[0] + "." + split[1];
        }
        return version;
    }

    public String getVersionFromServer() throws IOException {
        if (serverVersion == null) {
            synchronized (this) {
                String serverVersion = cellBaseClient.getMetaClient().about().firstResult().getString("Version");
                if (StringUtils.isEmpty(serverVersion)) {
                    serverVersion = cellBaseClient.getMetaClient().about().firstResult().getString("Version: ");
                }
                this.serverVersion = serverVersion;
            }
        }
        return serverVersion;
    }

    public boolean isMinVersion(String minVersion) throws IOException {
        String serverVersion = getVersionFromServer();
        return VersionUtils.isMinVersion(minVersion, serverVersion);
    }

    @Override
    public String toString() {
        return "URL: '" + getURL() + "', "
                + "version '" + getVersion() + "', "
                + "species '" + getSpecies() + "', "
                + "assembly '" + getAssembly() + "', "
                + "dataRelease '" + getDataRelease() + "'";

    }
}
