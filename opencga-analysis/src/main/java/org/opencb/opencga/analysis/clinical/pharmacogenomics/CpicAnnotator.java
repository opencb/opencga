package org.opencb.opencga.analysis.clinical.pharmacogenomics;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.opencb.opencga.core.models.clinical.pharmacogenomics.AlleleTyperResult.AlleleCall;
import org.opencb.opencga.core.models.clinical.pharmacogenomics.cpic.CpicAlleleAnnotation;
import org.opencb.opencga.core.models.clinical.pharmacogenomics.cpic.CpicAlleleInfo;
import org.opencb.opencga.core.models.clinical.pharmacogenomics.cpic.CpicDiplotypeAnnotation;
import org.opencb.opencga.core.models.clinical.pharmacogenomics.cpic.CpicDiplotypeInfo;
import org.opencb.opencga.core.models.clinical.pharmacogenomics.cpic.CpicDrugRecommendation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Annotates diplotypes with CPIC (Clinical Pharmacogenomics Implementation Consortium) data.
 *
 * <p>For each gene diplotype the following CPIC endpoints are called:
 * <ul>
 *   <li>/diplotype  – phenotype classification and lookupkey</li>
 *   <li>/allele     – per-allele functional status and activity value</li>
 *   <li>/recommendation – drug dosing recommendations for the lookupkey</li>
 * </ul>
 */
public class CpicAnnotator {

    static final String CPIC_BASE_URL = "https://api.cpicpgx.org/v1";
    static final String CPIC_SOURCE = "CPIC";

    private static final int CONNECT_TIMEOUT_MS = 10_000;
    private static final int READ_TIMEOUT_MS = 30_000;

    private final ObjectMapper objectMapper;
    private final Logger logger = LoggerFactory.getLogger(CpicAnnotator.class);

    public CpicAnnotator() {
        this.objectMapper = new ObjectMapper()
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    /**
     * Annotate a gene diplotype with CPIC data.
     *
     * @param gene        gene symbol, e.g. "CYP2C9"
     * @param alleleCalls list of allele calls from the allele typer
     * @return {@link CpicDiplotypeAnnotation}, or {@code null} if gene/alleleCalls are empty
     * @throws IOException if an HTTP or JSON parsing error occurs
     */
    public CpicDiplotypeAnnotation annotate(String gene, List<AlleleCall> alleleCalls) throws IOException {
        if (gene == null || gene.isEmpty() || alleleCalls == null || alleleCalls.isEmpty()) {
            return null;
        }

        String diplotype = buildDiplotype(alleleCalls);

        // 1. Diplotype info
        CpicDiplotypeInfo diplotypeInfo = fetchDiplotypeInfo(gene, diplotype);
        if (diplotypeInfo == null) {
            logger.warn("No CPIC diplotype info found for {}/{}", gene, diplotype);
        }

        // 2. Per-allele info
        List<CpicAlleleAnnotation> alleleAnnotations = fetchAlleleAnnotations(gene, alleleCalls);

        // 3. Drug recommendations (require lookupkey from diplotype info)
        List<CpicDrugRecommendation> recommendations = new ArrayList<>();
        if (diplotypeInfo != null && diplotypeInfo.getLookupkey() != null && !diplotypeInfo.getLookupkey().isEmpty()) {
            recommendations = fetchRecommendations(diplotypeInfo.getLookupkey());
        }

        return new CpicDiplotypeAnnotation(gene, diplotype, diplotypeInfo, alleleAnnotations, recommendations);
    }

    // -------------------------------------------------------------------------
    // Private helpers
    // -------------------------------------------------------------------------

    /**
     * Build a diplotype string by joining allele calls with '/'.
     * E.g. ["*1", "*6"] -> "*1/*6"
     */
    String buildDiplotype(List<AlleleCall> alleleCalls) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < alleleCalls.size(); i++) {
            if (i > 0) {
                sb.append('/');
            }
            sb.append(alleleCalls.get(i).getAllele());
        }
        return sb.toString();
    }

    private CpicDiplotypeInfo fetchDiplotypeInfo(String gene, String diplotype) throws IOException {
        // Encode '/' in diplotype as '%2F' for the PostgREST eq. filter
        String encodedDiplotype = diplotype.replace("/", "%2F");
        String url = CPIC_BASE_URL + "/diplotype?genesymbol=eq." + gene + "&diplotype=eq." + encodedDiplotype;
        String json = get(url);
        if (json == null) {
            return null;
        }
        List<CpicDiplotypeInfo> list = objectMapper.readValue(json, new TypeReference<List<CpicDiplotypeInfo>>() { });
        return list.isEmpty() ? null : list.get(0);
    }

    private List<CpicAlleleAnnotation> fetchAlleleAnnotations(String gene, List<AlleleCall> alleleCalls) throws IOException {
        List<CpicAlleleAnnotation> annotations = new ArrayList<>();
        for (AlleleCall alleleCall : alleleCalls) {
            String allele = alleleCall.getAllele();
            if (allele == null || allele.isEmpty()) {
                continue;
            }
            CpicAlleleInfo alleleInfo = fetchAlleleInfo(gene, allele);
            annotations.add(new CpicAlleleAnnotation(allele, alleleInfo));
        }
        return annotations;
    }

    private CpicAlleleInfo fetchAlleleInfo(String gene, String allele) throws IOException {
        String encodedAllele = URLEncoder.encode(allele, StandardCharsets.UTF_8.name());
        String url = CPIC_BASE_URL + "/allele?genesymbol=eq." + gene + "&name=eq." + encodedAllele;
        String json = get(url);
        if (json == null) {
            return null;
        }
        List<CpicAlleleInfo> list = objectMapper.readValue(json, new TypeReference<List<CpicAlleleInfo>>() { });
        return list.isEmpty() ? null : list.get(0);
    }

    private List<CpicDrugRecommendation> fetchRecommendations(Map<String, String> lookupkey) throws IOException {
        String lookupkeyJson = objectMapper.writeValueAsString(lookupkey);
        String encodedLookupkey = URLEncoder.encode(lookupkeyJson, StandardCharsets.UTF_8.name());
        String url = CPIC_BASE_URL + "/recommendation?lookupkey=cs." + encodedLookupkey;
        String json = get(url);
        if (json == null) {
            return new ArrayList<>();
        }
        List<RawRecommendation> rawList = objectMapper.readValue(json, new TypeReference<List<RawRecommendation>>() { });
        List<CpicDrugRecommendation> result = new ArrayList<>(rawList.size());
        for (RawRecommendation raw : rawList) {
            result.add(new CpicDrugRecommendation(
                    CPIC_SOURCE,
                    raw.drugid,
                    raw.drugnamesource,
                    raw.drugrecommendation,
                    raw.classification,
                    raw.population,
                    raw.comments));
        }
        return result;
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
     * Internal DTO to deserialize the /recommendation response.
     * Only the fields we need are mapped; unknown fields are ignored.
     */
    private static class RawRecommendation {
        public String drugid;
        public String drugnamesource;
        public String drugrecommendation;
        public String classification;
        public String population;
        public String comments;
    }
}
