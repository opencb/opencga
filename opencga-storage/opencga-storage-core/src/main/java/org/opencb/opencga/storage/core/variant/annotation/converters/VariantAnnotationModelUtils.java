package org.opencb.opencga.storage.core.variant.annotation.converters;

import org.apache.commons.collections4.CollectionUtils;
import org.opencb.biodata.models.variant.avro.*;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class VariantAnnotationModelUtils {

    /**
     * Extracts all the XRefs from a VariantAnnotation object.
     * Includes:
     * - annotation.id
     * - annotation.xrefs.id
     * - annotation.cytoband.chromosome + cytoband.name
     * - annotation.hgvs
     * - annotation.consequenceTypes.geneName
     * - annotation.consequenceTypes.geneId
     * - annotation.consequenceTypes.ensemblGeneId
     * - annotation.consequenceTypes.transcriptId
     * - annotation.consequenceTypes.ensemblTranscriptId
     * - annotation.consequenceTypes.hgvs
     * - annotation.consequenceTypes.proteinVariantAnnotation.uniprotAccession
     * - annotation.consequenceTypes.proteinVariantAnnotation.uniprotName
     * - annotation.consequenceTypes.proteinVariantAnnotation.uniprotVariantId
     * - annotation.consequenceTypes.proteinVariantAnnotation.features.id
     * - annotation.traitAssociation.id
     * - annotation.geneTraitAssociation.hpo
     * - annotation.geneTraitAssociation.id
     *
     * @param variantAnnotation VariantAnnotation object
     * @return Set of XRefs
     */
    public Set<String> extractXRefs(VariantAnnotation variantAnnotation) {
        Set<String> xrefs = new HashSet<>();

        if (variantAnnotation == null) {
            return xrefs;
        }

        xrefs.add(variantAnnotation.getId());

        if (variantAnnotation.getXrefs() != null) {
            for (Xref xref : variantAnnotation.getXrefs()) {
                if (xref != null) {
                    xrefs.add(xref.getId());
                }
            }
        }

        if (variantAnnotation.getCytoband() != null) {
            for (Cytoband cytoband : variantAnnotation.getCytoband()) {
                // TODO: Why do we need to add the chromosome name?
                xrefs.add(cytoband.getChromosome() + cytoband.getName());
            }
        }

        if (variantAnnotation.getHgvs() != null) {
            xrefs.addAll(variantAnnotation.getHgvs());
        }

        List<ConsequenceType> consequenceTypes = variantAnnotation.getConsequenceTypes();
        if (consequenceTypes != null) {
            for (ConsequenceType conseqType : consequenceTypes) {
                xrefs.add(conseqType.getGeneName());
                xrefs.add(conseqType.getGeneId());
                xrefs.add(conseqType.getEnsemblGeneId());
                xrefs.add(conseqType.getTranscriptId());
                xrefs.add(conseqType.getEnsemblTranscriptId());

                if (conseqType.getHgvs() != null) {
                    xrefs.addAll(conseqType.getHgvs());
                }

                ProteinVariantAnnotation protVarAnnotation = conseqType.getProteinVariantAnnotation();
                if (protVarAnnotation != null) {

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

        if (variantAnnotation.getGeneTraitAssociation() != null) {
            for (GeneTraitAssociation geneTrait : variantAnnotation.getGeneTraitAssociation()) {
                xrefs.add(geneTrait.getHpo());
                xrefs.add(geneTrait.getId());
            }
        }

        // Remove empty strings and nulls
        xrefs.remove("");
        xrefs.remove(null);

        return xrefs;
    }

}
