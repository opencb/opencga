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

package org.opencb.opencga.catalog.managers.clinical;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.models.clinical.ClinicalAcmg;
import org.opencb.biodata.models.clinical.ClinicalProperty;
import org.opencb.biodata.models.clinical.Disorder;
import org.opencb.biodata.models.clinical.Phenotype;
import org.opencb.biodata.models.clinical.interpretation.*;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.core.common.JacksonUtils;
import org.opencb.opencga.core.models.clinical.ClinicalAnalysis;
import org.opencb.opencga.core.models.clinical.Interpretation;
import org.opencb.opencga.core.models.individual.Individual;
import org.opencb.opencga.core.models.sample.Sample;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.util.*;

/**
 * Parser for Emedgene HL7 v2 JSON files. Converts Emedgene case reports into OpenCGA ClinicalAnalysis objects
 * with Interpretation and ClinicalVariant findings.
 */
public class EmedgeneParser {

    private static final Logger LOGGER = LoggerFactory.getLogger(EmedgeneParser.class);

    private static final String EMEDGENE_SOURCE = "emedgene";

    /**
     * Result object containing all parsed OpenCGA entities from an Emedgene file.
     */
    public static class EmedgeneParseResult {
        private ClinicalAnalysis clinicalAnalysis;
        private Individual proband;
        private Sample sample;
        private Interpretation interpretation;
        private Map<String, Object> rawJson;

        public ClinicalAnalysis getClinicalAnalysis() {
            return clinicalAnalysis;
        }

        public EmedgeneParseResult setClinicalAnalysis(ClinicalAnalysis clinicalAnalysis) {
            this.clinicalAnalysis = clinicalAnalysis;
            return this;
        }

        public Individual getProband() {
            return proband;
        }

        public EmedgeneParseResult setProband(Individual proband) {
            this.proband = proband;
            return this;
        }

        public Sample getSample() {
            return sample;
        }

        public EmedgeneParseResult setSample(Sample sample) {
            this.sample = sample;
            return this;
        }

        public Interpretation getInterpretation() {
            return interpretation;
        }

        public EmedgeneParseResult setInterpretation(Interpretation interpretation) {
            this.interpretation = interpretation;
            return this;
        }

        public Map<String, Object> getRawJson() {
            return rawJson;
        }

        public EmedgeneParseResult setRawJson(Map<String, Object> rawJson) {
            this.rawJson = rawJson;
            return this;
        }
    }

    /**
     * Parse an Emedgene JSON file and convert it to OpenCGA models.
     *
     * @param filePath Path to the Emedgene JSON file.
     * @return EmedgeneParseResult with ClinicalAnalysis, Individual, Sample, and Interpretation.
     * @throws CatalogException if the file cannot be parsed or required fields are missing.
     */
    public EmedgeneParseResult parse(Path filePath) throws CatalogException {
        ObjectMapper objectMapper = JacksonUtils.getDefaultObjectMapper();

        JsonNode root;
        Map<String, Object> rawJson;
        try {
            root = objectMapper.readTree(filePath.toFile());
            rawJson = objectMapper.convertValue(root, Map.class);
        } catch (IOException e) {
            throw new CatalogException("Error reading Emedgene file: " + e.getMessage(), e);
        }

        return parse(root, rawJson);
    }

    /**
     * Parse an Emedgene JSON string and convert it to OpenCGA models.
     *
     * @param jsonContent JSON content as string.
     * @return EmedgeneParseResult with ClinicalAnalysis, Individual, Sample, and Interpretation.
     * @throws CatalogException if the JSON cannot be parsed or required fields are missing.
     */
    public EmedgeneParseResult parse(String jsonContent) throws CatalogException {
        ObjectMapper objectMapper = JacksonUtils.getDefaultObjectMapper();

        JsonNode root;
        Map<String, Object> rawJson;
        try {
            root = objectMapper.readTree(jsonContent);
            rawJson = objectMapper.convertValue(root, Map.class);
        } catch (IOException e) {
            throw new CatalogException("Error parsing Emedgene JSON: " + e.getMessage(), e);
        }

        return parse(root, rawJson);
    }

    private EmedgeneParseResult parse(JsonNode root, Map<String, Object> rawJson) throws CatalogException {
        // Extract top-level fields
        String emgId = getRequiredText(root, "emg_id");
        String genomeBuild = getText(root, "genome_build", "GRCh38");

        // Extract proband sample ID
        JsonNode samplesNode = root.get("samples");
        if (samplesNode == null || !samplesNode.has("proband")) {
            throw new CatalogException("Missing 'samples.proband' in Emedgene file");
        }
        String sampleId = samplesNode.get("proband").asText();

        // Parse proband phenotypes (HPO terms)
        List<Phenotype> phenotypes = parsePhenotypes(root);

        // Parse variants into ClinicalVariants
        List<ClinicalVariant> primaryFindings = parseVariants(root, genomeBuild, phenotypes);

        // Derive disorder from first variant with non-empty evidence.disease
        Disorder disorder = deriveDisorder(root);

        // Build Sample
        Sample sample = new Sample().setId(sampleId);

        // Build Individual (proband) — use sampleId as individual ID too
        Individual proband = new Individual()
                .setId(sampleId)
                .setSamples(Collections.singletonList(sample))
                .setPhenotypes(phenotypes);
        if (disorder != null) {
            proband.setDisorders(Collections.singletonList(disorder));
        }

        // Build Interpretation with primary findings
        InterpretationMethod method = new InterpretationMethod()
                .setName(EMEDGENE_SOURCE);

        Interpretation interpretation = new Interpretation();
        interpretation.setId(emgId + ".1");
        interpretation.setDescription("Emedgene case " + emgId);
        interpretation.setMethod(method);
        interpretation.setPrimaryFindings(primaryFindings);
        interpretation.setSecondaryFindings(Collections.emptyList());

        // Build ClinicalAnalysis
        ClinicalAnalysis clinicalAnalysis = new ClinicalAnalysis()
                .setId(emgId)
                .setDescription("Imported from Emedgene case " + emgId)
                .setType(ClinicalAnalysis.Type.SINGLE)
                .setProband(proband)
                .setDisorder(disorder)
                .setInterpretation(interpretation)
                .setAttributes(new LinkedHashMap<>());

        // Store raw JSON and genome build in attributes
        clinicalAnalysis.getAttributes().put("EMEDGENE_RAW", rawJson);
        clinicalAnalysis.getAttributes().put("genomeBuild", genomeBuild);
        clinicalAnalysis.getAttributes().put("source", EMEDGENE_SOURCE);

        LOGGER.info("Parsed Emedgene case '{}': {} phenotypes, {} variants, disorder={}",
                emgId, phenotypes.size(), primaryFindings.size(),
                disorder != null ? disorder.getName() : "none");

        return new EmedgeneParseResult()
                .setClinicalAnalysis(clinicalAnalysis)
                .setProband(proband)
                .setSample(sample)
                .setInterpretation(interpretation)
                .setRawJson(rawJson);
    }

    private List<Phenotype> parsePhenotypes(JsonNode root) {
        List<Phenotype> phenotypes = new ArrayList<>();
        JsonNode phenotypesNode = root.get("proband_phenotypes");
        if (phenotypesNode != null && phenotypesNode.isArray()) {
            for (JsonNode pNode : phenotypesNode) {
                String id = getText(pNode, "id", "");
                String name = getText(pNode, "name", "");
                if (StringUtils.isNotEmpty(id)) {
                    phenotypes.add(new Phenotype(id, name, "HPO", Phenotype.Status.OBSERVED));
                }
            }
        }
        return phenotypes;
    }

    private Disorder deriveDisorder(JsonNode root) {
        JsonNode variantsNode = root.get("variants");
        if (variantsNode == null || !variantsNode.isArray()) {
            return null;
        }

        for (JsonNode variantNode : variantsNode) {
            JsonNode evidenceNode = variantNode.path("evidence").path("disease");
            String diseaseName = getText(evidenceNode, "name", "");
            if (StringUtils.isNotEmpty(diseaseName)) {
                String omimId = getText(evidenceNode, "omim_id", null);
                String orphanetId = getText(evidenceNode, "orphanet_id", null);

                // Use OMIM or Orphanet as disorder ID
                String disorderId;
                String source;
                if (StringUtils.isNotEmpty(omimId)) {
                    disorderId = "OMIM:" + omimId;
                    source = "OMIM";
                } else if (StringUtils.isNotEmpty(orphanetId)) {
                    disorderId = "ORPHA:" + orphanetId;
                    source = "Orphanet";
                } else {
                    disorderId = diseaseName.replaceAll("\\s+", "_");
                    source = EMEDGENE_SOURCE;
                }

                return new Disorder(disorderId, diseaseName, source, Collections.emptyMap(), "", Collections.emptyList());
            }
        }
        return null;
    }

    private List<ClinicalVariant> parseVariants(JsonNode root, String genomeBuild, List<Phenotype> phenotypes) {
        List<ClinicalVariant> clinicalVariants = new ArrayList<>();
        JsonNode variantsNode = root.get("variants");
        if (variantsNode == null || !variantsNode.isArray()) {
            return clinicalVariants;
        }

        for (JsonNode variantNode : variantsNode) {
            try {
                ClinicalVariant cv = parseVariant(variantNode, genomeBuild, phenotypes);
                if (cv != null) {
                    clinicalVariants.add(cv);
                }
            } catch (Exception e) {
                LOGGER.warn("Error parsing variant: {}", e.getMessage());
            }
        }
        return clinicalVariants;
    }

    private ClinicalVariant parseVariant(JsonNode variantNode, String genomeBuild, List<Phenotype> phenotypes) {
        String chromosome = getText(variantNode, "chromosome", "");
        int position = variantNode.path("position").asInt(0);
        String ref = getText(variantNode, "ref", "");
        String alt = getText(variantNode, "alt", "");
        String vartype = getText(variantNode, "vartype", "");

        if (StringUtils.isEmpty(chromosome) || position <= 0) {
            return null;
        }

        // Normalize chromosome: strip "chr" prefix
        String normalizedChrom = chromosome.startsWith("chr") ? chromosome.substring(3) : chromosome;

        // Determine variant type and build Variant
        Variant variant;
        if (isStructuralVariant(vartype)) {
            int positionEnd = variantNode.path("position_end").asInt(position);
            variant = buildStructuralVariant(normalizedChrom, position, positionEnd, ref, alt, vartype, variantNode);
        } else {
            variant = new Variant(normalizedChrom, position, ref, alt);
        }

        // Build ClinicalVariantEvidence
        ClinicalVariantEvidence evidence = buildEvidence(variantNode, phenotypes);

        // Build confidence from quality field
        ClinicalVariantConfidence confidence = buildConfidence(variantNode);

        // Build references from articles
        List<MiniPubmed> references = buildReferences(variantNode);

        // Build tags from Emedgene tag
        List<String> tags = new ArrayList<>();
        JsonNode tagNode = variantNode.path("tag");
        if (tagNode.has("value")) {
            tags.add(tagNode.get("value").asText());
        }

        // Determine status based on tag
        ClinicalVariant.Status status = ClinicalVariant.Status.NOT_REVIEWED;
        if (tags.contains("most_likely")) {
            status = ClinicalVariant.Status.REPORTED;
        }

        // Build attributes with extra Emedgene fields
        Map<String, Object> attributes = new LinkedHashMap<>();
        attributes.put("emedgene_vartype", vartype);
        attributes.put("emedgene_quality", getText(variantNode, "quality", ""));
        attributes.put("emedgene_evidence_text", getText(variantNode, "evidence_text", ""));
        attributes.put("emedgene_mutation_type", getText(variantNode, "mutation_type", ""));
        if (variantNode.has("proband_zygosity")) {
            attributes.put("emedgene_proband_zygosity", getText(variantNode, "proband_zygosity", ""));
        }
        if (variantNode.has("phenomeld_score")) {
            attributes.put("emedgene_phenomeld_score", variantNode.get("phenomeld_score").asDouble());
        }

        ClinicalVariant clinicalVariant = new ClinicalVariant(
                variant.getImpl(),
                Collections.singletonList(evidence),
                Collections.emptyList(),        // comments
                null,                            // filter
                Collections.emptyList(),         // modesOfInheritance (set at evidence level)
                "",                              // recommendation
                references,
                null,                            // discussion
                confidence,
                Collections.emptyList(),         // stats
                status,
                tags,
                Collections.emptyList(),         // images
                attributes
        );

        return clinicalVariant;
    }

    private boolean isStructuralVariant(String vartype) {
        if (StringUtils.isEmpty(vartype)) {
            return false;
        }
        String upper = vartype.toUpperCase();
        return "DEL".equals(upper) || "DUP".equals(upper) || "INV".equals(upper)
                || "INS".equals(upper) || "CNV".equals(upper) || "TRANSLOCATION".equals(upper);
    }

    private Variant buildStructuralVariant(String chromosome, int start, int end, String ref, String alt,
                                           String vartype, JsonNode variantNode) {
        String normalizedAlt;
        switch (vartype.toUpperCase()) {
            case "DEL":
                normalizedAlt = "<DEL>";
                break;
            case "DUP":
                normalizedAlt = "<DUP>";
                break;
            case "INV":
                normalizedAlt = "<INV>";
                break;
            case "INS":
                normalizedAlt = "<INS>";
                break;
            case "CNV":
                normalizedAlt = "<CNV>";
                break;
            default:
                normalizedAlt = alt;
                break;
        }

        // Use the Variant(chromosome, start, end, ref, alt) constructor which infers the VariantType
        return new Variant(chromosome, start, end, ref, normalizedAlt);
    }

    private ClinicalVariantEvidence buildEvidence(JsonNode variantNode, List<Phenotype> phenotypes) {
        ClinicalVariantEvidence evidence = new ClinicalVariantEvidence();
        evidence.setInterpretationMethodName(EMEDGENE_SOURCE);
        evidence.setPhenotypes(phenotypes);

        // GenomicFeature
        String geneName = getText(variantNode, "gene_name", "");
        if (StringUtils.isNotEmpty(geneName)) {
            // Use first gene if multiple (comma-separated)
            String primaryGene = geneName.split(",")[0].trim();
            String transcriptId = "";
            JsonNode transcripts = variantNode.get("transcripts");
            if (transcripts != null && transcripts.isArray() && transcripts.size() > 0) {
                transcriptId = getText(transcripts.get(0), "name", "");
            }

            // Get HGNC ID if available
            String hgncId = "";
            JsonNode hgncIds = variantNode.get("HGNC_IDS");
            if (hgncIds != null && hgncIds.isArray() && hgncIds.size() > 0) {
                hgncId = hgncIds.get(0).asText();
            }

            GenomicFeature genomicFeature = new GenomicFeature();
            genomicFeature.setId(StringUtils.isNotEmpty(hgncId) ? hgncId : primaryGene);
            genomicFeature.setType("GENE");
            genomicFeature.setGeneName(primaryGene);
            genomicFeature.setTranscriptId(transcriptId);
            evidence.setGenomicFeature(genomicFeature);
        }

        // Mode of inheritance from evidence.disease.inheritance
        JsonNode diseaseNode = variantNode.path("evidence").path("disease");
        String inheritance = getText(diseaseNode, "inheritance", "");
        if (StringUtils.isNotEmpty(inheritance)) {
            ClinicalProperty.ModeOfInheritance moi = mapModeOfInheritance(inheritance);
            evidence.setModeOfInheritances(Collections.singletonList(moi));
        }

        // ACMG classification
        VariantClassification classification = buildClassification(variantNode);
        evidence.setClassification(classification);

        return evidence;
    }

    private VariantClassification buildClassification(JsonNode variantNode) {
        VariantClassification classification = new VariantClassification();

        // Determine if it's a CNV or small variant classification
        String acmgClass = getText(variantNode, "acmg_classification", "");
        String cnvAcmgClass = getText(variantNode, "cnv_acmg_classification", "");
        String classificationString = StringUtils.isNotEmpty(acmgClass) ? acmgClass : cnvAcmgClass;

        // Map clinical significance
        if (StringUtils.isNotEmpty(classificationString)) {
            classification.setClinicalSignificance(mapClinicalSignificance(classificationString));
        }

        // Parse ACMG tags from acmg_tags_checked (small variants) or cnv_acmg_tags_sections_checked (CNVs)
        List<ClinicalAcmg> acmgList = new ArrayList<>();

        JsonNode acmgTags = variantNode.get("acmg_tags_checked");
        if (acmgTags != null && acmgTags.isArray()) {
            for (JsonNode tag : acmgTags) {
                String criterion = getText(tag, "criterion", "");
                String strength = getText(tag, "strength", "");
                if (StringUtils.isNotEmpty(criterion)) {
                    acmgList.add(new ClinicalAcmg(criterion, strength, "", EMEDGENE_SOURCE, ""));
                }
            }
        }

        JsonNode cnvAcmgTags = variantNode.get("cnv_acmg_tags_sections_checked");
        if (cnvAcmgTags != null && cnvAcmgTags.isArray()) {
            for (JsonNode tag : cnvAcmgTags) {
                String criterion = getText(tag, "criterion", "");
                double score = tag.path("score").asDouble(0.0);
                if (StringUtils.isNotEmpty(criterion) && score != 0.0) {
                    acmgList.add(new ClinicalAcmg(criterion, String.valueOf(score), "", EMEDGENE_SOURCE, ""));
                }
            }
        }

        classification.setAcmg(acmgList);
        return classification;
    }

    private ClinicalVariantConfidence buildConfidence(JsonNode variantNode) {
        String quality = getText(variantNode, "quality", "");
        ClinicalVariantConfidence.Confidence value;
        switch (quality.toUpperCase()) {
            case "HIGH":
                value = ClinicalVariantConfidence.Confidence.HIGH;
                break;
            case "MODERATE":
            case "MEDIUM":
                value = ClinicalVariantConfidence.Confidence.MEDIUM;
                break;
            case "LOW":
                value = ClinicalVariantConfidence.Confidence.LOW;
                break;
            default:
                value = null;
                break;
        }
        ClinicalVariantConfidence confidence = new ClinicalVariantConfidence();
        if (value != null) {
            confidence.setValue(value);
            confidence.setAuthor(EMEDGENE_SOURCE);
        }
        return confidence;
    }

    private List<MiniPubmed> buildReferences(JsonNode variantNode) {
        List<MiniPubmed> references = new ArrayList<>();
        JsonNode articles = variantNode.get("articles");
        if (articles != null && articles.isArray()) {
            for (JsonNode article : articles) {
                String title = getText(article, "title", "");
                String url = getText(article, "url", "");
                if (StringUtils.isNotEmpty(title)) {
                    MiniPubmed ref = new MiniPubmed();
                    ref.setTitle(title);
                    ref.setUrl(url);
                    references.add(ref);
                }
            }
        }
        return references;
    }

    private ClinicalProperty.ClinicalSignificance mapClinicalSignificance(String emedgeneClassification) {
        if (StringUtils.isEmpty(emedgeneClassification)) {
            return ClinicalProperty.ClinicalSignificance.UNCERTAIN_SIGNIFICANCE;
        }
        String normalized = emedgeneClassification.toLowerCase().replace("_", " ").trim();
        switch (normalized) {
            case "pathogenic":
                return ClinicalProperty.ClinicalSignificance.PATHOGENIC;
            case "likely pathogenic":
                return ClinicalProperty.ClinicalSignificance.LIKELY_PATHOGENIC;
            case "uncertain significance":
            case "vus":
                return ClinicalProperty.ClinicalSignificance.UNCERTAIN_SIGNIFICANCE;
            case "likely benign":
                return ClinicalProperty.ClinicalSignificance.LIKELY_BENIGN;
            case "benign":
                return ClinicalProperty.ClinicalSignificance.BENIGN;
            default:
                return ClinicalProperty.ClinicalSignificance.UNCERTAIN_SIGNIFICANCE;
        }
    }

    private ClinicalProperty.ModeOfInheritance mapModeOfInheritance(String inheritance) {
        if (StringUtils.isEmpty(inheritance)) {
            return ClinicalProperty.ModeOfInheritance.UNKNOWN;
        }
        String normalized = inheritance.toLowerCase().trim();
        if (normalized.contains("autosomal recessive")) {
            return ClinicalProperty.ModeOfInheritance.AUTOSOMAL_RECESSIVE;
        } else if (normalized.contains("autosomal dominant")) {
            return ClinicalProperty.ModeOfInheritance.AUTOSOMAL_DOMINANT;
        } else if (normalized.contains("x-linked recessive") || normalized.contains("x linked recessive")) {
            return ClinicalProperty.ModeOfInheritance.X_LINKED_RECESSIVE;
        } else if (normalized.contains("x-linked dominant") || normalized.contains("x linked dominant")) {
            return ClinicalProperty.ModeOfInheritance.X_LINKED_DOMINANT;
        } else if (normalized.contains("y-linked") || normalized.contains("y linked")) {
            return ClinicalProperty.ModeOfInheritance.Y_LINKED;
        } else if (normalized.contains("mitochondrial")) {
            return ClinicalProperty.ModeOfInheritance.MITOCHONDRIAL;
        } else if (normalized.contains("de novo")) {
            return ClinicalProperty.ModeOfInheritance.DE_NOVO;
        }
        return ClinicalProperty.ModeOfInheritance.UNKNOWN;
    }

    // Utility methods

    private String getRequiredText(JsonNode node, String field) throws CatalogException {
        if (!node.has(field) || node.get(field).isNull() || StringUtils.isEmpty(node.get(field).asText())) {
            throw new CatalogException("Missing required field '" + field + "' in Emedgene file");
        }
        return node.get(field).asText();
    }

    private String getText(JsonNode node, String field, String defaultValue) {
        if (node == null || !node.has(field) || node.get(field).isNull()) {
            return defaultValue;
        }
        String value = node.get(field).asText();
        return StringUtils.isNotEmpty(value) && !"null".equals(value) ? value : defaultValue;
    }
}
