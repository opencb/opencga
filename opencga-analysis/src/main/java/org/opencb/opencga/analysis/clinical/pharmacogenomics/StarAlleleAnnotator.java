/*
 * Copyright 2015-2020 OpenCB
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

package org.opencb.opencga.analysis.clinical.pharmacogenomics;

import org.opencb.biodata.models.pharma.PharmaChemical;
import org.opencb.biodata.models.pharma.PharmaVariantAnnotation;
import org.opencb.cellbase.core.result.CellBaseDataResponse;
import org.opencb.cellbase.client.rest.CellBaseClient;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.core.models.clinical.pharmacogenomics.StarAlleleAnnotation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Annotates star alleles with pharmacogenomics data from CellBase.
 * This class is tool-agnostic — it only knows about gene names and star allele strings.
 * CellBase results are cached per gene to avoid redundant queries.
 */
public class StarAlleleAnnotator {

    private static final int MAX_RESULTS = 1000;

    private final CellBaseClient cellBaseClient;
    private final Map<String, List<PharmaChemical>> geneCache;
    private final Logger logger = LoggerFactory.getLogger(StarAlleleAnnotator.class);

    public StarAlleleAnnotator(CellBaseClient cellBaseClient) {
        this.cellBaseClient = cellBaseClient;
        this.geneCache = new HashMap<>();
    }

    /**
     * Annotate a star allele diplotype for a given gene with CellBase pharmacogenomics data.
     *
     * @param geneName Gene name (e.g. "CYP2C9")
     * @param starAllele Star allele diplotype (e.g. "*4/*16")
     * @return StarAlleleAnnotation with filtered drugs for this gene and allele
     * @throws IOException if CellBase query fails
     */
    public StarAlleleAnnotation annotate(String geneName, String starAllele) throws IOException {
        // Parse diplotype into individual star allele names (e.g. "*4/*16" -> "*4", "*16")
        Set<String> individualAlleles = new HashSet<>();
        for (String allele : starAllele.split("/")) {
            individualAlleles.add(allele.trim());
        }

        // Query all drugs for this gene (cached)
        List<PharmaChemical> allDrugs = getPharmacogenomicsForGene(geneName);

        // Filter variants on a shallow copy so the cache stays intact
        List<PharmaChemical> filteredDrugs = filterDrugs(allDrugs, geneName, individualAlleles);

        // Build and return the annotation object
        String version = cellBaseClient.getClientConfiguration().getVersion();
        return new StarAlleleAnnotation(
                "cellbase", version != null ? version : "", "", filteredDrugs);
    }

    /**
     * Get pharmacogenomics data for a gene, querying CellBase only once per gene.
     */
    private List<PharmaChemical> getPharmacogenomicsForGene(String gene) throws IOException {
        if (geneCache.containsKey(gene)) {
            logger.debug("Using cached pharmacogenomics data for gene: {}", gene);
            return geneCache.get(gene);
        }

        List<PharmaChemical> results = queryPharmacogenomics(gene);
        geneCache.put(gene, results);
        return results;
    }

    /**
     * Query CellBase pharmacogenomics endpoint for a given gene.
     * Excludes the heavy 'genes' field since we already filter by geneName in the query
     * and only need the variants for haplotype matching.
     *
     * @param gene Gene name (e.g. "CYP2D6")
     * @return List of all PharmaChemical objects for this gene
     * @throws IOException if the query fails
     */
    private List<PharmaChemical> queryPharmacogenomics(String gene) throws IOException {
        logger.info("Querying CellBase pharmacogenomics for gene: {}", gene);

        QueryOptions queryOptions = new QueryOptions("geneName", gene);
        queryOptions.put(QueryOptions.LIMIT, MAX_RESULTS);
        queryOptions.put(QueryOptions.EXCLUDE, "genes");

        CellBaseDataResponse<PharmaChemical> response = cellBaseClient.getGenericClient()
                .get("clinical", "pharmacogenomics", "search", "", queryOptions, PharmaChemical.class);

        List<PharmaChemical> results = response.allResults();
        if (results == null) {
            results = new ArrayList<>();
        }

        logger.info("Retrieved {} pharmacogenomics results for gene: {}", results.size(), gene);
        return results;
    }

    /**
     * Filter variants within each drug to only keep those matching our gene and star alleles.
     * Creates new PharmaChemical instances to avoid mutating cached data.
     * The 'genes' field is excluded from CellBase queries for performance, so it is not filtered here.
     *
     * @param drugs List of PharmaChemical objects from CellBase (not modified)
     * @param gene Gene name to filter by
     * @param starAlleles Set of individual star allele names to match against haplotypes
     * @return New list of PharmaChemical with filtered variants
     */
    private List<PharmaChemical> filterDrugs(List<PharmaChemical> drugs, String gene, Set<String> starAlleles) {
        List<PharmaChemical> filteredDrugs = new ArrayList<>();

        for (PharmaChemical drug : drugs) {
            PharmaChemical copy = new PharmaChemical();
            copy.setId(drug.getId());
            copy.setName(drug.getName());
            copy.setSource(drug.getSource());
            copy.setTypes(drug.getTypes());

            // Filter variants: keep only those where geneNames contains our gene.
            // If haplotypes are present, also require at least one star allele match.
            if (drug.getVariants() != null) {
                List<PharmaVariantAnnotation> filteredVariants = drug.getVariants().stream()
                        .filter(v -> v.getGeneNames() != null && v.getGeneNames().contains(gene))
                        .filter(v -> v.getHaplotypes() == null || v.getHaplotypes().isEmpty()
                                || v.getHaplotypes().stream().anyMatch(starAlleles::contains))
                        .collect(Collectors.toList());
                copy.setVariants(filteredVariants);
            }

            filteredDrugs.add(copy);
        }

        return filteredDrugs;
    }
}
