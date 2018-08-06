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

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.client.Put;
import org.opencb.biodata.models.variant.VariantBuilder;
import org.opencb.biodata.models.variant.avro.*;
import org.opencb.biodata.tools.Converter;
import org.opencb.opencga.storage.core.variant.annotation.converters.VariantTraitAssociationToEvidenceEntryConverter;
import org.opencb.opencga.storage.hadoop.variant.converters.AbstractPhoenixConverter;
import org.opencb.opencga.storage.hadoop.variant.index.phoenix.PhoenixHelper;
import org.opencb.opencga.storage.hadoop.variant.index.phoenix.VariantPhoenixHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

import static org.opencb.opencga.storage.core.variant.adaptors.VariantQueryUtils.parseConsequenceType;
import static org.opencb.opencga.storage.hadoop.variant.index.phoenix.VariantPhoenixHelper.VariantColumn.*;
import static org.opencb.opencga.storage.hadoop.variant.index.phoenix.VariantPhoenixKeyFactory.generateVariantRowKey;

/**
 * Created on 01/12/15.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class VariantAnnotationToPhoenixConverter extends AbstractPhoenixConverter
        implements Converter<VariantAnnotation, Map<PhoenixHelper.Column, ?>> {

    private VariantTraitAssociationToEvidenceEntryConverter evidenceEntryConverter;

    public VariantAnnotationToPhoenixConverter(byte[] columnFamily) {
        super(columnFamily);
        evidenceEntryConverter = new VariantTraitAssociationToEvidenceEntryConverter();
    }

    private final Logger logger = LoggerFactory.getLogger(VariantAnnotationToPhoenixConverter.class);

    @Override
    public Map<PhoenixHelper.Column, ?> convert(VariantAnnotation variantAnnotation) {

        HashMap<PhoenixHelper.Column, Object> map = new HashMap<>();

        map.put(FULL_ANNOTATION, variantAnnotation.toString());

        Set<String> genes = new HashSet<>();
        Set<String> gnSo = new HashSet<>();
        Set<String> transcripts = new HashSet<>();
        Set<String> flags = new HashSet<>();
        Set<Integer> soList = new HashSet<>();
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
        Set<ClinicalSignificance> clinicalSignificanceSet = new HashSet<>();
        Set<String> xrefs = new HashSet<>();

        addNotNull(xrefs, variantAnnotation.getId());

        List<ConsequenceType> consequenceTypes = variantAnnotation.getConsequenceTypes() == null
                ? Collections.emptyList()
                : variantAnnotation.getConsequenceTypes();
        for (ConsequenceType consequenceType : consequenceTypes) {
            addNotNull(genes, consequenceType.getGeneName());
            addNotNull(genes, consequenceType.getEnsemblGeneId());
            addNotNull(transcripts, consequenceType.getEnsemblTranscriptId());
            addNotNull(biotype, consequenceType.getBiotype());
            addAllNotNull(flags, consequenceType.getTranscriptAnnotationFlags());

            ProteinVariantAnnotation proteinVariantAnnotation = consequenceType.getProteinVariantAnnotation();
            for (SequenceOntologyTerm sequenceOntologyTerm : consequenceType.getSequenceOntologyTerms()) {
                String accession = sequenceOntologyTerm.getAccession();
                int so = parseConsequenceType(accession);
                addNotNull(soList, so);

                if (StringUtils.isNotEmpty(consequenceType.getGeneName())) {
                    gnSo.add(buildGeneSO(consequenceType.getGeneName(), so));
                }
                if (StringUtils.isNotEmpty(consequenceType.getEnsemblGeneId())) {
                    gnSo.add(buildGeneSO(consequenceType.getEnsemblGeneId(), so));
                }
                if (StringUtils.isNotEmpty(consequenceType.getEnsemblTranscriptId())) {
                    gnSo.add(buildGeneSO(consequenceType.getEnsemblTranscriptId(), so));
                }
                if (proteinVariantAnnotation != null) {
                    if (StringUtils.isNotEmpty(proteinVariantAnnotation.getUniprotAccession())) {
                        gnSo.add(buildGeneSO(proteinVariantAnnotation.getUniprotAccession(), so));
                    }
                    if (StringUtils.isNotEmpty(proteinVariantAnnotation.getUniprotName())) {
                        gnSo.add(buildGeneSO(proteinVariantAnnotation.getUniprotName(), so));
                    }
                }

            }
            if (proteinVariantAnnotation != null) {
                if (proteinVariantAnnotation.getSubstitutionScores() != null) {
                    for (Score score : proteinVariantAnnotation.getSubstitutionScores()) {
                        if (score.getSource().equalsIgnoreCase("sift")) {
                            addNotNull(sift, score.getScore());
                            addNotNull(siftDesc, score.getDescription());
                        } else if (score.getSource().equalsIgnoreCase("polyphen")) {
                            addNotNull(polyphen, score.getScore());
                            addNotNull(polyphenDesc, score.getDescription());
                        }
                    }
                }
                if (proteinVariantAnnotation.getKeywords() != null) {
                    proteinKeywords.addAll(proteinVariantAnnotation.getKeywords());
                }
                addNotNull(xrefs, proteinVariantAnnotation.getUniprotName());
                addNotNull(xrefs, proteinVariantAnnotation.getUniprotAccession());
                addNotNull(xrefs, proteinVariantAnnotation.getUniprotVariantId());
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

        // If there are VariantTraitAssociation, and there are none TraitAssociations (EvidenceEntry), convert
        if (variantAnnotation.getVariantTraitAssociation() != null && CollectionUtils.isEmpty(variantAnnotation.getTraitAssociation())) {
            List<EvidenceEntry> evidenceEntries = evidenceEntryConverter.convert(variantAnnotation.getVariantTraitAssociation());
            variantAnnotation.setTraitAssociation(evidenceEntries);
        }

        if (CollectionUtils.isNotEmpty(variantAnnotation.getTraitAssociation())) {
            for (EvidenceEntry evidenceEntry : variantAnnotation.getTraitAssociation()) {
                if (evidenceEntry.getVariantClassification() != null) {
                    ClinicalSignificance clinicalSignificance = evidenceEntry.getVariantClassification().getClinicalSignificance();
                    if (clinicalSignificance != null) {
                        clinicalSignificanceSet.add(clinicalSignificance);
                    }
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
        map.put(GENE_SO, gnSo);
        map.put(SO, soList);
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
        map.put(CLINICAL_SIGNIFICANCE, clinicalSignificanceSet.stream().map(ClinicalSignificance::toString).collect(Collectors.toList()));

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

        VariantType variantType = VariantBuilder.inferType(variantAnnotation.getReference(), variantAnnotation.getAlternate());
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

    public static String buildGeneSO(String gene, Integer so) {
        return gene == null ? null : gene + '_' + so;
    }

    Put buildPut(VariantAnnotation variantAnnotation) {
        Map<PhoenixHelper.Column, ?> map = convert(variantAnnotation);

        byte[] bytesRowKey = generateVariantRowKey(variantAnnotation.getChromosome(), variantAnnotation.getStart(),
                variantAnnotation.getReference(), variantAnnotation.getAlternate());
        Put put = new Put(bytesRowKey);

        for (PhoenixHelper.Column column : VariantPhoenixHelper.PRIMARY_KEY) {
            map.remove(column);
        }
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
