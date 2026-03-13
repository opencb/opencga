package org.opencb.opencga.analysis.clinical.pharmacogenomics;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.opencb.opencga.core.models.clinical.pharmacogenomics.cpic.CpicAlleleAnnotation;
import org.opencb.opencga.core.models.clinical.pharmacogenomics.cpic.CpicAlleleInfo;
import org.opencb.opencga.core.models.clinical.pharmacogenomics.cpic.CpicAlleleLocationValue;
import org.opencb.opencga.core.models.clinical.pharmacogenomics.cpic.CpicDiplotypeAnnotation;
import org.opencb.opencga.core.models.clinical.pharmacogenomics.cpic.CpicDiplotypeInfo;
import org.opencb.opencga.core.models.clinical.pharmacogenomics.cpic.CpicDrug;
import org.opencb.opencga.core.models.clinical.pharmacogenomics.cpic.CpicDrugRecommendation;
import org.opencb.opencga.core.models.clinical.pharmacogenomics.cpic.CpicSequenceLocation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * Annotates diplotypes with CPIC (Clinical Pharmacogenomics Implementation Consortium) data.
 *
 * <p>For each gene diplotype the following CPIC endpoints are called:
 * <ul>
 *   <li>/diplotype            – phenotype classification and lookupkey</li>
 *   <li>/allele               – per-allele functional status and activity value</li>
 *   <li>/allele_definition    – per-allele variant locations (filled into CpicAlleleInfo.location)</li>
 *   <li>/pair                 – drug-gene pairs (CPIC level, PGx testing)</li>
 *   <li>/recommendation       – drug dosing recommendations for the lookupkey</li>
 *   <li>/drug                 – resolves drugid to human-readable drug name</li>
 * </ul>
 */
public class CpicAnnotator {

    static final String CPIC_BASE_URL = "https://api.cpicpgx.org/v1";
    static final String CPIC_SOURCE = "CPIC";

    private static final int CONNECT_TIMEOUT_MS = 10_000;
    private static final int READ_TIMEOUT_MS = 30_000;

    private final ObjectMapper objectMapper;
    private final Logger logger = LoggerFactory.getLogger(CpicAnnotator.class);

    // Caches to avoid repeated API calls across samples
    private final Map<String, String> drugNameCache = new HashMap<>();                         // drugid -> name
    private final Map<String, CpicAlleleInfo> alleleInfoCache = new HashMap<>();               // "gene:allele" -> info
    private final Map<String, List<CpicAlleleLocationValue>> alleleDefinitionCache = new HashMap<>(); // "gene:allele" -> location
    private final Map<String, CpicDiplotypeInfo> diplotypeInfoCache = new HashMap<>();         // "gene:diplotype" -> info
    private final Map<String, List<CpicDrugRecommendation>> recommendationCache = new HashMap<>(); // lookupkey JSON -> recs
    private final Map<String, List<RawPair>> pairCache = new HashMap<>();                      // gene -> pairs

    public CpicAnnotator() {
        this.objectMapper = new ObjectMapper()
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    /**
     * Annotate a gene diplotype with CPIC data.
     *
     * @param gene      gene symbol, e.g. "CYP2C9"
     * @param diplotype diplotype string, e.g. "*1/*4"
     * @return {@link CpicDiplotypeAnnotation}, or {@code null} if gene/diplotype are empty
     * @throws IOException if an HTTP or JSON parsing error occurs
     */
    public CpicDiplotypeAnnotation annotate(String gene, String diplotype) throws IOException {
        if (gene == null || gene.isEmpty() || diplotype == null || diplotype.isEmpty()
                || "no translation available".equals(diplotype)) {
            return null;
        }

        // 1. Diplotype info
        CpicDiplotypeInfo diplotypeInfo = fetchDiplotypeInfo(gene, diplotype);
        if (diplotypeInfo == null) {
            logger.warn("No CPIC diplotype info found for {}:{}", gene, diplotype);
        }

        // 2. Per-allele info: extract distinct allele names from the diplotype string
        List<String> alleles = extractAlleles(diplotype);
        List<CpicAlleleAnnotation> alleleAnnotations = fetchAlleleAnnotations(gene, alleles);

        // 3. Fetch drug-gene pairs for this gene and recommendations for the lookupkey,
        //    then match recommendations to pairs by drugid + guidelineid
        List<CpicDrug> drugs = buildDrugs(gene, diplotypeInfo);

        return new CpicDiplotypeAnnotation(gene, diplotype, diplotypeInfo, alleleAnnotations, drugs);
    }

    // -------------------------------------------------------------------------
    // Private helpers
    // -------------------------------------------------------------------------

    /**
     * Extract distinct star allele names from a diplotype string.
     * Handles simple ("*1/*4") and ambiguous ("{*1/*1, *1/*4, *4/*4}") formats.
     */
    List<String> extractAlleles(String diplotype) {
        // Strip curly brackets if present
        String clean = diplotype;
        if (clean.startsWith("{") && clean.endsWith("}")) {
            clean = clean.substring(1, clean.length() - 1);
        }

        Set<String> alleles = new LinkedHashSet<>();
        // Split by comma to get individual diplotype pairs
        for (String pair : clean.split(",")) {
            // Split each pair by '/' to get allele names
            for (String allele : pair.trim().split("/")) {
                String trimmed = allele.trim();
                if (!trimmed.isEmpty()) {
                    alleles.add(trimmed);
                }
            }
        }
        return new ArrayList<>(alleles);
    }

    private CpicDiplotypeInfo fetchDiplotypeInfo(String gene, String diplotype) throws IOException {
        String cacheKey = gene + ":" + diplotype;
        if (diplotypeInfoCache.containsKey(cacheKey)) {
            return diplotypeInfoCache.get(cacheKey);
        }
        String encodedDiplotype = diplotype.replace("/", "%2F");
        String url = CPIC_BASE_URL + "/diplotype?genesymbol=eq." + gene + "&diplotype=eq." + encodedDiplotype;
        String json = get(url);
        CpicDiplotypeInfo result = null;
        if (json != null) {
            List<CpicDiplotypeInfo> list = objectMapper.readValue(json, new TypeReference<List<CpicDiplotypeInfo>>() { });
            result = list.isEmpty() ? null : list.get(0);
        }
        diplotypeInfoCache.put(cacheKey, result);
        return result;
    }

    private List<CpicAlleleAnnotation> fetchAlleleAnnotations(String gene, List<String> alleles) throws IOException {
        List<CpicAlleleAnnotation> annotations = new ArrayList<>();
        for (String allele : alleles) {
            if (allele == null || allele.isEmpty()) {
                continue;
            }
            CpicAlleleInfo alleleInfo = fetchAlleleInfo(gene, allele);
            annotations.add(new CpicAlleleAnnotation(allele, alleleInfo));
        }
        return annotations;
    }

    private CpicAlleleInfo fetchAlleleInfo(String gene, String allele) throws IOException {
        String cacheKey = gene + ":" + allele;
        if (alleleInfoCache.containsKey(cacheKey)) {
            return alleleInfoCache.get(cacheKey);
        }
        String encodedAllele = URLEncoder.encode(allele, StandardCharsets.UTF_8.name());
        String url = CPIC_BASE_URL + "/allele?genesymbol=eq." + gene + "&name=eq." + encodedAllele;
        String json = get(url);
        CpicAlleleInfo result = null;
        if (json != null) {
            List<CpicAlleleInfo> list = objectMapper.readValue(json, new TypeReference<List<CpicAlleleInfo>>() { });
            result = list.isEmpty() ? null : list.get(0);
        }
        if (result != null) {
            // Enrich with variant-level location data from /allele_definition
            result.setLocation(fetchAlleleLocationValues(gene, allele, encodedAllele));
        }
        alleleInfoCache.put(cacheKey, result);
        return result;
    }

    /**
     * Fetch variant-level location values for a single allele from the CPIC /allele_definition endpoint.
     * Example: GET /allele_definition?genesymbol=eq.CYP2C9&name=eq.*6&select=*,allele_location_value(*,sequence_location(*))
     */
    private List<CpicAlleleLocationValue> fetchAlleleLocationValues(String gene, String allele,
                                                                    String encodedAllele) throws IOException {
        String cacheKey = gene + ":" + allele;
        if (alleleDefinitionCache.containsKey(cacheKey)) {
            return alleleDefinitionCache.get(cacheKey);
        }
        String url = CPIC_BASE_URL + "/allele_definition?genesymbol=eq." + gene + "&name=eq." + encodedAllele
                + "&select=*,allele_location_value(*,sequence_location(*))";
        String json = get(url);
        List<CpicAlleleLocationValue> result = new ArrayList<>();
        if (json != null) {
            List<RawAlleleDef> defs = objectMapper.readValue(json, new TypeReference<List<RawAlleleDef>>() { });
            if (!defs.isEmpty() && defs.get(0).allele_location_value != null) {
                for (RawLocationValue raw : defs.get(0).allele_location_value) {
                    CpicSequenceLocation seqLoc = null;
                    if (raw.sequence_location != null) {
                        seqLoc = new CpicSequenceLocation(
                                raw.sequence_location.name,
                                raw.sequence_location.dbsnpid,
                                raw.sequence_location.position,
                                raw.sequence_location.genelocation,
                                raw.sequence_location.proteinlocation,
                                raw.sequence_location.chromosomelocation);
                    }
                    result.add(new CpicAlleleLocationValue(raw.variantallele, seqLoc));
                }
            }
        }
        alleleDefinitionCache.put(cacheKey, result);
        return result;
    }

    /**
     * Build CpicDrug list by fetching /pair for the gene and /recommendation for the lookupkey,
     * then matching recommendations to drugs by drugid + guidelineid.
     */
    private List<CpicDrug> buildDrugs(String gene, CpicDiplotypeInfo diplotypeInfo) throws IOException {
        // Fetch all drug-gene pairs for this gene
        List<RawPair> pairs = fetchPairs(gene);
        if (pairs.isEmpty()) {
            return new ArrayList<>();
        }

        // Fetch recommendations (if we have a lookupkey)
        List<CpicDrugRecommendation> allRecommendations = new ArrayList<>();
        if (diplotypeInfo != null && diplotypeInfo.getLookupkey() != null && !diplotypeInfo.getLookupkey().isEmpty()) {
            allRecommendations = fetchRecommendations(diplotypeInfo.getLookupkey());
        }

        // Build CpicDrug for each pair, attaching matching recommendations
        List<CpicDrug> drugs = new ArrayList<>();
        for (RawPair pair : pairs) {
            String drugName = resolveDrugName(pair.drugid);

            // Find recommendations matching this pair's drugid + guidelineid
            List<CpicDrugRecommendation> matchedRecs = new ArrayList<>();
            for (CpicDrugRecommendation rec : allRecommendations) {
                if (pair.drugid != null && pair.drugid.equals(rec.getDrugid())
                        && pair.guidelineid != null && pair.guidelineid.equals(rec.getGuidelineid())) {
                    matchedRecs.add(rec);
                }
            }

            drugs.add(new CpicDrug(
                    pair.drugid,
                    drugName,
                    pair.genesymbol,
                    pair.guidelineid,
                    pair.cpiclevel,
                    pair.pgkbcalevel,
                    pair.pgxtesting,
                    Boolean.TRUE.equals(pair.usedforrecommendation),
                    matchedRecs));
        }
        return drugs;
    }

    /**
     * Fetch drug-gene pairs from the CPIC /pair endpoint for a given gene.
     * Results are cached per gene since pairs are the same across all samples.
     */
    private List<RawPair> fetchPairs(String gene) throws IOException {
        if (pairCache.containsKey(gene)) {
            return pairCache.get(gene);
        }
        String url = CPIC_BASE_URL + "/pair?genesymbol=eq." + gene;
        String json = get(url);
        List<RawPair> result;
        if (json == null) {
            result = new ArrayList<>();
        } else {
            result = objectMapper.readValue(json, new TypeReference<List<RawPair>>() { });
        }
        pairCache.put(gene, result);
        return result;
    }

    /**
     * Fetch recommendations for a given lookupkey.
     * The CPIC API may return multiple entries per drug for different activity score combinations;
     * we deduplicate by drugid + guidelineid, keeping the first occurrence per combination.
     */
    private List<CpicDrugRecommendation> fetchRecommendations(Map<String, String> lookupkey) throws IOException {
        String lookupkeyJson = objectMapper.writeValueAsString(lookupkey);
        if (recommendationCache.containsKey(lookupkeyJson)) {
            return recommendationCache.get(lookupkeyJson);
        }
        String encodedLookupkey = URLEncoder.encode(lookupkeyJson, StandardCharsets.UTF_8.name());
        String url = CPIC_BASE_URL + "/recommendation?lookupkey=cs." + encodedLookupkey;
        String json = get(url);
        if (json == null) {
            recommendationCache.put(lookupkeyJson, new ArrayList<>());
            return new ArrayList<>();
        }
        List<RawRecommendation> rawList = objectMapper.readValue(json, new TypeReference<List<RawRecommendation>>() { });

        // Deduplicate by drugid + guidelineid: keep first occurrence per combination
        Map<String, CpicDrugRecommendation> byKey = new LinkedHashMap<>();
        for (RawRecommendation raw : rawList) {
            String dedupeKey = raw.drugid + ":" + raw.guidelineid;
            if (byKey.containsKey(dedupeKey)) {
                continue;
            }
            String drugName = resolveDrugName(raw.drugid);
            byKey.put(dedupeKey, new CpicDrugRecommendation(
                    CPIC_SOURCE,
                    raw.drugid,
                    drugName,
                    raw.guidelineid,
                    raw.drugrecommendation,
                    raw.classification,
                    raw.implications,
                    raw.phenotypes,
                    raw.population,
                    Boolean.TRUE.equals(raw.dosinginformation),
                    Boolean.TRUE.equals(raw.alternatedrugavailable),
                    raw.comments));
        }
        List<CpicDrugRecommendation> result = new ArrayList<>(byKey.values());
        recommendationCache.put(lookupkeyJson, result);
        return result;
    }

    /**
     * Resolve a drugid (e.g. "RxNorm:704") to its human-readable name via the CPIC /drug endpoint.
     * Results are cached to avoid redundant API calls.
     */
    private String resolveDrugName(String drugid) {
        if (drugid == null || drugid.isEmpty()) {
            return null;
        }
        if (drugNameCache.containsKey(drugid)) {
            return drugNameCache.get(drugid);
        }
        try {
            String encodedDrugid = URLEncoder.encode(drugid, StandardCharsets.UTF_8.name());
            String url = CPIC_BASE_URL + "/drug?drugid=eq." + encodedDrugid;
            String json = get(url);
            if (json != null) {
                List<Map<String, Object>> drugs = objectMapper.readValue(json,
                        new TypeReference<List<Map<String, Object>>>() { });
                if (!drugs.isEmpty()) {
                    String name = (String) drugs.get(0).get("name");
                    drugNameCache.put(drugid, name);
                    return name;
                }
            }
        } catch (IOException e) {
            logger.warn("Failed to resolve drug name for {}: {}", drugid, e.getMessage());
        }
        drugNameCache.put(drugid, null);
        return null;
    }

    /**
     * Execute a GET request and return the response body, or {@code null} on non-200 status.
     */
    private String get(String url) throws IOException {
        HttpURLConnection connection = (HttpURLConnection) new URL(url).openConnection();
        connection.setConnectTimeout(CONNECT_TIMEOUT_MS);
        connection.setReadTimeout(READ_TIMEOUT_MS);
        connection.setRequestMethod("GET");
        connection.setRequestProperty("Accept", "application/json");
        int statusCode = connection.getResponseCode();
        if (statusCode != HttpURLConnection.HTTP_OK) {
            logger.warn("CPIC API returned HTTP {} for URL: {}", statusCode, url);
            return null;
        }
        StringBuilder sb = new StringBuilder();
        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(connection.getInputStream(), StandardCharsets.UTF_8))) {
            String line;
            while ((line = reader.readLine()) != null) {
                sb.append(line);
            }
        } finally {
            connection.disconnect();
        }
        return sb.toString();
    }

    /**
     * Internal DTO to deserialize the /allele_definition response (top-level object).
     */
    private static class RawAlleleDef {
        public List<RawLocationValue> allele_location_value;
    }

    /**
     * Internal DTO for one element of the allele_location_value array.
     */
    private static class RawLocationValue {
        public String variantallele;
        public RawSequenceLocation sequence_location;
    }

    /**
     * Internal DTO for the nested sequence_location object.
     */
    private static class RawSequenceLocation {
        public String name;
        public String dbsnpid;
        public Long position;
        public String genelocation;
        public String proteinlocation;
        public String chromosomelocation;
    }

    /**
     * Internal DTO to deserialize the /pair response.
     */
    private static class RawPair {
        public String drugid;
        public String genesymbol;
        public Integer guidelineid;
        public String cpiclevel;
        public String pgkbcalevel;
        public String pgxtesting;
        public Boolean usedforrecommendation;
    }

    /**
     * Internal DTO to deserialize the /recommendation response.
     * Only the fields we need are mapped; unknown fields are ignored.
     */
    private static class RawRecommendation {
        public String drugid;
        public Integer guidelineid;
        public String drugrecommendation;
        public String classification;
        public Map<String, String> implications;
        public Map<String, String> phenotypes;
        public String population;
        public Boolean dosinginformation;
        public Boolean alternatedrugavailable;
        public String comments;
    }
}
