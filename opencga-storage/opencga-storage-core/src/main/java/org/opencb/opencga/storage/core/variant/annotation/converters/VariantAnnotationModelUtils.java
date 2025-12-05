package org.opencb.opencga.storage.core.variant.annotation.converters;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.models.variant.avro.*;
import org.opencb.opencga.storage.core.variant.query.VariantQueryUtils;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class VariantAnnotationModelUtils {

    /**
     * Extracts all the XRefs from a VariantAnnotation object.
     * Includes:
     * - annotation.id
     * - annotation.xrefs.id
     * - annotation.hgvs
     * - annotation.consequenceTypes.geneName
     * - annotation.consequenceTypes.geneId
     * - annotation.consequenceTypes.ensemblGeneId
     * - annotation.consequenceTypes.transcriptId
     * - annotation.consequenceTypes.ensemblTranscriptId
     * - annotation.consequenceTypes.hgvs
     * - annotation.consequenceTypes.proteinVariantAnnotation.proteinId
     * - annotation.consequenceTypes.proteinVariantAnnotation.uniprotAccession
     * - annotation.consequenceTypes.proteinVariantAnnotation.uniprotName
     * - annotation.consequenceTypes.proteinVariantAnnotation.uniprotVariantId
     * - annotation.consequenceTypes.proteinVariantAnnotation.features.id
     * - annotation.traitAssociation.id
     * - annotation.geneTraitAssociation.id
     * - annotation.geneTraitAssociation.hpo
     * - annotation.pharmacogenomics.id
     * - annotation.pharmacogenomics.name
     *
     * @param variantAnnotation VariantAnnotation object
     * @return Set of XRefs
     */
    private static final Pattern HGVS_PATTERN = Pattern.compile("\\([^()]*\\)");

    public VariantAnnotationModelUtils() {
    }

    public static Set<String> extractXRefs(VariantAnnotation variantAnnotation) {
        Set<String> xrefs = new HashSet<>(100);

        if (variantAnnotation == null) {
            return xrefs;
        }

        xrefs.add(variantAnnotation.getId());

        if (CollectionUtils.isNotEmpty(variantAnnotation.getXrefs())) {
            for (Xref xref : variantAnnotation.getXrefs()) {
                if (xref != null) {
                    xrefs.add(xref.getId());
                }
            }
        }

        List<ConsequenceType> consequenceTypes = variantAnnotation.getConsequenceTypes();

        if (CollectionUtils.isNotEmpty(variantAnnotation.getHgvs())) {
            xrefs.addAll(variantAnnotation.getHgvs());

            // TODO Remove this code when CellBase 6.4.0 returns the expected HGVS
            for (String hgvs: variantAnnotation.getHgvs()) {
                if (VariantQueryUtils.isTranscript(hgvs)) {
                    // 1. Remove the content between parentheses, e.g. ENST00000680783.1(ENSG00000135744):c.776T>C
                    if (hgvs.contains("(")) {
                        Matcher matcher = HGVS_PATTERN.matcher(hgvs);
                        StringBuffer result = new StringBuffer();
                        while (matcher.find()) {
                            matcher.appendReplacement(result, "");
                        }
                        matcher.appendTail(result);
                        xrefs.add(result.toString());
                    }

                    // 2. Add the HGVS with the Ensembl and gene name, e.g. ENSG00000135744:c.776T>C, AGT:c.776T>C
                    if (CollectionUtils.isNotEmpty(consequenceTypes)) {
                        for (ConsequenceType conseqType : consequenceTypes) {
                            if (conseqType != null && conseqType.getHgvs() != null && conseqType.getHgvs().contains(hgvs)) {
                                String[] fields = hgvs.split(":", 2);
                                if (StringUtils.isNotEmpty(conseqType.getGeneId())) {
                                    xrefs.add(conseqType.getGeneId() + ":" + fields[1]);
                                }
                                if (StringUtils.isNotEmpty(conseqType.getGeneName())) {
                                    xrefs.add(conseqType.getGeneName() + ":" + fields[1]);
                                }
                                break;
                            }
                        }
                    }
                }
            }
        }

        if (CollectionUtils.isNotEmpty(consequenceTypes)) {
            for (ConsequenceType conseqType : consequenceTypes) {
                xrefs.add(conseqType.getGeneName());
                xrefs.add(conseqType.getGeneId());
                xrefs.add(conseqType.getTranscriptId());
                xrefs.add(conseqType.getEnsemblGeneId());
                xrefs.add(conseqType.getEnsemblTranscriptId());

                ProteinVariantAnnotation protVarAnnotation = conseqType.getProteinVariantAnnotation();
                if (protVarAnnotation != null) {
                    xrefs.add(protVarAnnotation.getProteinId());
                    xrefs.add(protVarAnnotation.getUniprotAccession());
                    xrefs.add(protVarAnnotation.getUniprotName());
                    xrefs.add(protVarAnnotation.getUniprotVariantId());

                    if (protVarAnnotation.getFeatures() != null) {
                        for (ProteinFeature proteinFeature : protVarAnnotation.getFeatures()) {
                            xrefs.add(proteinFeature.getId());
                        }
                    }
                }
            }
        }

        if (CollectionUtils.isNotEmpty(variantAnnotation.getTraitAssociation())) {
            for (EvidenceEntry evidenceEntry : variantAnnotation.getTraitAssociation()) {
                xrefs.add(evidenceEntry.getId());
            }
        }

        if (CollectionUtils.isNotEmpty(variantAnnotation.getGeneTraitAssociation())) {
            for (GeneTraitAssociation geneTrait : variantAnnotation.getGeneTraitAssociation()) {
                xrefs.add(geneTrait.getId());
                xrefs.add(geneTrait.getHpo());
            }
        }

        if (CollectionUtils.isNotEmpty(variantAnnotation.getPharmacogenomics())) {
            for (Pharmacogenomics pharmacogenomics : variantAnnotation.getPharmacogenomics()) {
                xrefs.add(pharmacogenomics.getId());
                xrefs.add(pharmacogenomics.getName());
            }
        }

        // Remove empty strings and nulls
        xrefs.remove("");
        xrefs.remove(null);

        return xrefs;
    }

}
