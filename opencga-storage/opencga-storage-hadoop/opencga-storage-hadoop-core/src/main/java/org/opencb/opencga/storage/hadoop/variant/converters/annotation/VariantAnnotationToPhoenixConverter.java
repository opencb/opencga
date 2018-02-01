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

package org.opencb.opencga.storage.hadoop.variant.converters.annotation;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.client.Put;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.*;
import org.opencb.biodata.tools.Converter;
import org.opencb.opencga.storage.hadoop.variant.converters.AbstractPhoenixConverter;
import org.opencb.opencga.storage.hadoop.variant.index.phoenix.PhoenixHelper;
import org.opencb.opencga.storage.hadoop.variant.index.phoenix.VariantPhoenixHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static org.opencb.opencga.storage.hadoop.variant.index.phoenix.VariantPhoenixHelper.VariantColumn.*;
import static org.opencb.opencga.storage.hadoop.variant.index.phoenix.VariantPhoenixKeyFactory.generateVariantRowKey;

/**
 * Created on 01/12/15.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class VariantAnnotationToPhoenixConverter extends AbstractPhoenixConverter
        implements Converter<VariantAnnotation, Map<PhoenixHelper.Column, ?>> {

    public VariantAnnotationToPhoenixConverter(byte[] columnFamily) {
        super(columnFamily);
    }

    private final Logger logger = LoggerFactory.getLogger(VariantAnnotationToPhoenixConverter.class);

    @Override
    public Map<PhoenixHelper.Column, ?> convert(VariantAnnotation variantAnnotation) {

        HashMap<PhoenixHelper.Column, Object> map = new HashMap<>();

        map.put(FULL_ANNOTATION, variantAnnotation.toString());

        Set<String> genes = new HashSet<>();
        Set<String> transcripts = new HashSet<>();
        Set<String> flags = new HashSet<>();
        Set<Integer> so = new HashSet<>();
        Set<String> biotype = new HashSet<>();
        Set<Double> polyphen = new HashSet<>();
        Set<Double> sift = new HashSet<>();
        Set<String> polyphenDesc = new HashSet<>();
        Set<String> siftDesc = new HashSet<>();
        Set<String> geneTraitName = new HashSet<>();
        Set<String> geneTraitId = new HashSet<>();
//        Set<String> hpo = new HashSet<>();
        Set<String> drugs = new HashSet<>();
        Set<String> proteinKeywords = new HashSet<>();
        // Contains all the xrefs, and the id, the geneNames and transcripts
        Set<String> xrefs = new HashSet<>();

        addNotNull(xrefs, variantAnnotation.getId());

        for (ConsequenceType consequenceType : variantAnnotation.getConsequenceTypes()) {
            addNotNull(genes, consequenceType.getGeneName());
            addNotNull(genes, consequenceType.getEnsemblGeneId());
            addNotNull(transcripts, consequenceType.getEnsemblTranscriptId());
            addNotNull(biotype, consequenceType.getBiotype());
            addAllNotNull(flags, consequenceType.getTranscriptAnnotationFlags());
            for (SequenceOntologyTerm sequenceOntologyTerm : consequenceType.getSequenceOntologyTerms()) {
                String accession = sequenceOntologyTerm.getAccession();
                addNotNull(so, Integer.parseInt(accession.substring(3)));
            }
            if (consequenceType.getProteinVariantAnnotation() != null) {
                if (consequenceType.getProteinVariantAnnotation().getSubstitutionScores() != null) {
                    for (Score score : consequenceType.getProteinVariantAnnotation().getSubstitutionScores()) {
                        if (score.getSource().equalsIgnoreCase("sift")) {
                            addNotNull(sift, score.getScore());
                            addNotNull(siftDesc, score.getDescription());
                        } else if (score.getSource().equalsIgnoreCase("polyphen")) {
                            addNotNull(polyphen, score.getScore());
                            addNotNull(polyphenDesc, score.getDescription());
                        }
                    }
                }
                if (consequenceType.getProteinVariantAnnotation().getKeywords() != null) {
                    proteinKeywords.addAll(consequenceType.getProteinVariantAnnotation().getKeywords());
                }
                addNotNull(xrefs, consequenceType.getProteinVariantAnnotation().getUniprotName());
                addNotNull(xrefs, consequenceType.getProteinVariantAnnotation().getUniprotAccession());
                addNotNull(xrefs, consequenceType.getProteinVariantAnnotation().getUniprotVariantId());
            }
        }

        if (variantAnnotation.getVariantTraitAssociation() != null) {
            if (variantAnnotation.getVariantTraitAssociation().getCosmic() != null) {
                for (Cosmic cosmic : variantAnnotation.getVariantTraitAssociation().getCosmic()) {
                    addNotNull(xrefs, cosmic.getMutationId());
                }
            }
            if (variantAnnotation.getVariantTraitAssociation().getClinvar() != null) {
                for (ClinVar clinVar : variantAnnotation.getVariantTraitAssociation().getClinvar()) {
                    addNotNull(xrefs, clinVar.getAccession());
                }
            }
        }

        xrefs.addAll(genes);
        xrefs.addAll(transcripts);
        if (variantAnnotation.getXrefs() != null) {
            for (Xref xref : variantAnnotation.getXrefs()) {
                addNotNull(xrefs, xref.getId());
            }
        }

        if (variantAnnotation.getGeneTraitAssociation() != null) {
            for (GeneTraitAssociation geneTrait : variantAnnotation.getGeneTraitAssociation()) {
                addNotNull(geneTraitName, geneTrait.getName());
                addNotNull(geneTraitId, geneTrait.getId());
                addNotNull(xrefs, geneTrait.getHpo());
                addNotNull(xrefs, geneTrait.getId());
            }
        }

        if (variantAnnotation.getGeneDrugInteraction() != null) {
            for (GeneDrugInteraction drug : variantAnnotation.getGeneDrugInteraction()) {
                addNotNull(drugs, drug.getDrugName());
            }
        }

        map.put(CHROMOSOME, variantAnnotation.getChromosome());
        map.put(POSITION, variantAnnotation.getStart());
        map.put(REFERENCE, variantAnnotation.getReference());
        map.put(ALTERNATE, variantAnnotation.getAlternate());
        map.put(GENES, genes);
        map.put(TRANSCRIPTS, transcripts);
        map.put(BIOTYPE, biotype);
        map.put(SO, so);
        map.put(POLYPHEN, sortProteinSubstitutionScores(polyphen));
        map.put(POLYPHEN_DESC, polyphenDesc);
        map.put(SIFT, sortProteinSubstitutionScores(sift));
        map.put(SIFT_DESC, siftDesc);
        map.put(TRANSCRIPTION_FLAGS, flags);
        map.put(GENE_TRAITS_ID, geneTraitId);
        map.put(PROTEIN_KEYWORDS, proteinKeywords);
        map.put(GENE_TRAITS_NAME, geneTraitName);
        map.put(DRUG, drugs);
        map.put(XREFS, xrefs);

        if (variantAnnotation.getConservation() != null) {
            for (Score score : variantAnnotation.getConservation()) {
                PhoenixHelper.Column column = VariantPhoenixHelper.getConservationScoreColumn(score.getSource());
                map.put(column, score.getScore());
            }
        }

        if (variantAnnotation.getPopulationFrequencies() != null) {
            for (PopulationFrequency pf : variantAnnotation.getPopulationFrequencies()) {
                PhoenixHelper.Column column = VariantPhoenixHelper.getPopulationFrequencyColumn(pf.getStudy(), pf.getPopulation());
                map.put(column, Arrays.asList(pf.getRefAlleleFreq(), pf.getAltAlleleFreq()));
            }
        }

        if (variantAnnotation.getFunctionalScore() != null) {
            for (Score score : variantAnnotation.getFunctionalScore()) {
                PhoenixHelper.Column column = VariantPhoenixHelper.getFunctionalScoreColumn(score.getSource());
                map.put(column, score.getScore());
            }
        }

        VariantType variantType = Variant.inferType(variantAnnotation.getReference(), variantAnnotation.getAlternate());
        if (StringUtils.isNotBlank(variantAnnotation.getId())) {
            if (variantType.equals(VariantType.SNV)) {
                variantType = VariantType.SNP;
            } else if (variantType.equals(VariantType.MNV)) {
                variantType = VariantType.MNP;
            }
        }
        map.put(TYPE, variantType.toString());

        return map;
    }

    Put buildPut(VariantAnnotation variantAnnotation) {
        Map<PhoenixHelper.Column, ?> map = convert(variantAnnotation);

        byte[] bytesRowKey = generateVariantRowKey(variantAnnotation.getChromosome(), variantAnnotation.getStart(),
                variantAnnotation.getReference(), variantAnnotation.getAlternate());
        Put put = new Put(bytesRowKey);

        map.forEach((column, value) -> add(put, column, value));

        return put;
    }

    private List<Double> sortProteinSubstitutionScores(Set<Double> scores) {
        List<Double> sorted = new ArrayList<>(scores.size());
        Double min = scores.stream().min(Double::compareTo).orElse(-1.0);
        Double max = scores.stream().max(Double::compareTo).orElse(-1.0);
        if (min >= 0) {
            sorted.add(min);
            sorted.add(max);
            scores.remove(min);
            scores.remove(max);
            sorted.addAll(scores);
        }
        return sorted;
    }

}
