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

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.opencb.biodata.models.variant.VariantBuilder;
import org.opencb.biodata.models.variant.avro.*;
import org.opencb.biodata.tools.commons.Converter;
import org.opencb.commons.utils.CompressionUtils;
import org.opencb.opencga.storage.core.variant.query.VariantQueryUtils;
import org.opencb.opencga.storage.hadoop.variant.adaptors.phoenix.PhoenixHelper;
import org.opencb.opencga.storage.hadoop.variant.adaptors.phoenix.VariantPhoenixSchema;
import org.opencb.opencga.storage.hadoop.variant.converters.AbstractPhoenixConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.*;

import static org.opencb.opencga.storage.core.variant.query.VariantQueryUtils.parseConsequenceType;
import static org.opencb.opencga.storage.hadoop.variant.adaptors.phoenix.VariantPhoenixKeyFactory.generateVariantRowKey;
import static org.opencb.opencga.storage.hadoop.variant.adaptors.phoenix.VariantPhoenixSchema.DEFAULT_HUMAN_POPULATION_FREQUENCIES_COLUMNS;
import static org.opencb.opencga.storage.hadoop.variant.adaptors.phoenix.VariantPhoenixSchema.VariantColumn.*;

/**
 * Created on 01/12/15.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class VariantAnnotationToPhoenixConverter extends AbstractPhoenixConverter
        implements Converter<VariantAnnotation, Map<PhoenixHelper.Column, ?>> {

    public static final int COMPRESS_THRESHOLD = (int) (0.8 * 1024 * 1024); // 0.8MB
    private static final Map<PhoenixHelper.Column, Object> DEFAULT_POP_FREQ;

    static {
        HashMap<PhoenixHelper.Column, Object> map = new HashMap<>(DEFAULT_HUMAN_POPULATION_FREQUENCIES_COLUMNS.size());

        List<Float> value = Arrays.asList(1F, 0F);
        for (PhoenixHelper.Column column : DEFAULT_HUMAN_POPULATION_FREQUENCIES_COLUMNS) {
            map.put(column, value);
        }
        DEFAULT_POP_FREQ = Collections.unmodifiableMap(map);
    }

    private int annotationId;

    @Deprecated
    public VariantAnnotationToPhoenixConverter(byte[] columnFamily) {
        this(columnFamily, -1);
    }

    public VariantAnnotationToPhoenixConverter(byte[] columnFamily, int annotationId) {
        super(columnFamily);
        this.annotationId = annotationId;
    }

    private final Logger logger = LoggerFactory.getLogger(VariantAnnotationToPhoenixConverter.class);

    @Override
    public Map<PhoenixHelper.Column, ?> convert(VariantAnnotation variantAnnotation) {

        HashMap<PhoenixHelper.Column, Object> map = new HashMap<>();

        String json = variantAnnotation.toString();
        if (json.length() > COMPRESS_THRESHOLD) {
            try {
                map.put(FULL_ANNOTATION, CompressionUtils.compress(Bytes.toBytes(json)));
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        } else {
            map.put(FULL_ANNOTATION, json);
        }
        if (annotationId >= 0) {
            map.put(ANNOTATION_ID, annotationId);
        }

        Set<String> genes = new HashSet<>();
        Set<String> geneSo = new HashSet<>();
        Set<String> biotypeSo = new HashSet<>();
        Set<String> geneBiotypeSo = new HashSet<>();
        Set<String> geneBiotype = new HashSet<>();
        Set<String> geneSoFlag = new HashSet<>();
        Set<String> soFlag = new HashSet<>();
        Set<String> transcripts = new HashSet<>();
        Set<String> flags = new HashSet<>();
        Set<Integer> soList = new HashSet<>();
        Set<String> biotype = new HashSet<>();
        Set<Float> polyphen = new HashSet<>();
        Set<Float> sift = new HashSet<>();
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

        List<ConsequenceType> consequenceTypes = variantAnnotation.getConsequenceTypes() == null
                ? Collections.emptyList()
                : variantAnnotation.getConsequenceTypes();
        for (ConsequenceType consequenceType : consequenceTypes) {
            addNotNull(genes, consequenceType.getGeneName());
            addNotNull(genes, consequenceType.getGeneId());
            addNotNull(transcripts, consequenceType.getTranscriptId());
            addNotNull(biotype, consequenceType.getBiotype());
            addAllNotNull(flags, consequenceType.getTranscriptFlags());

            ProteinVariantAnnotation proteinVariantAnnotation = consequenceType.getProteinVariantAnnotation();
            for (SequenceOntologyTerm sequenceOntologyTerm : consequenceType.getSequenceOntologyTerms()) {
                String accession = sequenceOntologyTerm.getAccession();
                int so = parseConsequenceType(accession);
                addNotNull(soList, so);

                if (StringUtils.isNotEmpty(consequenceType.getGeneName())) {
                    geneSo.add(combine(consequenceType.getGeneName(), so));
                    geneSo.add(combine(consequenceType.getGeneId(), so));
                    geneSo.add(combine(consequenceType.getTranscriptId(), so));

                    if (StringUtils.isNotEmpty(consequenceType.getBiotype())) {
                        geneBiotypeSo.add(combine(consequenceType.getGeneName(), consequenceType.getBiotype(), so));
                        geneBiotypeSo.add(combine(consequenceType.getGeneId(), consequenceType.getBiotype(), so));
                        geneBiotypeSo.add(combine(consequenceType.getTranscriptId(), consequenceType.getBiotype(), so));
                    }
                }
                if (StringUtils.isNotEmpty(consequenceType.getBiotype())) {
                    // This is useful when no gene or transcript is passed, for example we want 'LoF' in real 'protein_coding'
                    biotypeSo.add(combine(consequenceType.getBiotype(), so));
                }

                if (proteinVariantAnnotation != null) {
                    geneSo.add(combine(proteinVariantAnnotation.getUniprotAccession(), so));
                    geneSo.add(combine(proteinVariantAnnotation.getUniprotName(), so));
                }

                // Add a combination with the transcript flag
                if (consequenceType.getTranscriptFlags() != null) {
                    for (String flag : consequenceType.getTranscriptFlags()) {
                        if (VariantQueryUtils.IMPORTANT_TRANSCRIPT_FLAGS.contains(flag)) {
                            geneSoFlag.add(combine(consequenceType.getGeneName(), so, flag));
                            geneSoFlag.add(combine(consequenceType.getGeneId(), so, flag));
                            geneSoFlag.add(combine(consequenceType.getTranscriptId(), so, flag));
                            // This is useful when no gene or transcript is used, for example 'LoF' in 'basic' transcripts
                            soFlag.add(combine(so, flag));
                        }
                    }
                }
            }
            geneBiotype.add(combine(consequenceType.getGeneName(), consequenceType.getBiotype()));
            geneBiotype.add(combine(consequenceType.getGeneId(), consequenceType.getBiotype()));
            geneBiotype.add(combine(consequenceType.getTranscriptId(), consequenceType.getBiotype()));

            if (proteinVariantAnnotation != null) {
                if (proteinVariantAnnotation.getSubstitutionScores() != null) {
                    for (Score score : proteinVariantAnnotation.getSubstitutionScores()) {
                        if (score.getSource().equalsIgnoreCase("sift")) {
                            if (score.getScore() != null) {
                                sift.add(score.getScore().floatValue());
                            }
                            addNotNull(siftDesc, score.getDescription());
                        } else if (score.getSource().equalsIgnoreCase("polyphen")) {
                            if (score.getScore() != null) {
                                polyphen.add(score.getScore().floatValue());
                            }
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

        if (CollectionUtils.isNotEmpty(variantAnnotation.getTraitAssociation())) {
            for (EvidenceEntry evidenceEntry : variantAnnotation.getTraitAssociation()) {
                addNotNull(xrefs, evidenceEntry.getId());
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

        // Remove null values, if any
        geneSo.remove(null);
        biotypeSo.remove(null);
        geneBiotypeSo.remove(null);
        geneBiotype.remove(null);
        geneSoFlag.remove(null);
        soFlag.remove(null);

        map.put(CHROMOSOME, variantAnnotation.getChromosome());
        map.put(POSITION, variantAnnotation.getStart());
        map.put(REFERENCE, variantAnnotation.getReference());
        map.put(ALTERNATE, variantAnnotation.getAlternate());
        map.put(GENES, genes);
        map.put(TRANSCRIPTS, transcripts);
        map.put(BIOTYPE, biotype);
        map.put(GENE_SO, geneSo);
        map.put(BIOTYPE_SO, biotypeSo);
        map.put(GENE_BIOTYPE_SO, geneBiotypeSo);
        map.put(GENE_BIOTYPE, geneBiotype);
        map.put(GENE_SO_FLAG, geneSoFlag);
        map.put(SO_FLAG, soFlag);
        map.put(SO, soList);
        map.put(POLYPHEN, sortProteinSubstitutionScores(polyphen));
        map.put(POLYPHEN_DESC, polyphenDesc);
        map.put(SIFT, sortProteinSubstitutionScores(sift));
        map.put(SIFT_DESC, siftDesc);
        map.put(TRANSCRIPT_FLAGS, flags);
        map.put(GENE_TRAITS_ID, geneTraitId);
        map.put(PROTEIN_KEYWORDS, proteinKeywords);
        map.put(GENE_TRAITS_NAME, geneTraitName);
        map.put(DRUG, drugs);
        map.put(XREFS, xrefs);
        map.put(CLINICAL, VariantQueryUtils.buildClinicalCombinations(variantAnnotation));

        if (variantAnnotation.getConservation() != null) {
            for (Score score : variantAnnotation.getConservation()) {
                PhoenixHelper.Column column = VariantPhoenixSchema.getConservationScoreColumn(score.getSource());
                map.put(column, score.getScore());
            }
        }

        map.putAll(DEFAULT_POP_FREQ);
        if (variantAnnotation.getPopulationFrequencies() != null) {
            for (PopulationFrequency pf : variantAnnotation.getPopulationFrequencies()) {
                PhoenixHelper.Column column = VariantPhoenixSchema.getPopulationFrequencyColumn(pf.getStudy(), pf.getPopulation());
                map.put(column, Arrays.asList(pf.getRefAlleleFreq(), pf.getAltAlleleFreq()));
            }
        }

        if (variantAnnotation.getFunctionalScore() != null) {
            for (Score score : variantAnnotation.getFunctionalScore()) {
                PhoenixHelper.Column column = VariantPhoenixSchema.getFunctionalScoreColumn(score.getSource());
                map.put(column, score.getScore());
            }
        }

        VariantType variantType = VariantBuilder.inferType(variantAnnotation.getReference(), variantAnnotation.getAlternate());
//        if (StringUtils.isNotBlank(variantAnnotation.getId())) {
//            if (variantType.equals(VariantType.SNV)) {
//                variantType = VariantType.SNP;
//            } else if (variantType.equals(VariantType.MNV)) {
//                variantType = VariantType.MNP;
//            }
//        }
        map.put(TYPE, variantType.toString());

        return map;
    }

    public static String combine(String geneOrBiotype, int so) {
        return StringUtils.isEmpty(geneOrBiotype) ? null : geneOrBiotype + '_' + so;
    }

    public static String combine(int so, String flag) {
        // FIXME: This will compute a numerical add between so and '_' (i.e. so + 95)
        return StringUtils.isEmpty(flag) ? null : so + '_' + flag;
    }

    public static String combine(String gene, int so, String flag) {
        return StringUtils.isAnyEmpty(gene, flag) ? null : gene + '_' + so + '_' + flag;
    }

    public static String combine(String gene, String biotye, int so) {
        return StringUtils.isAnyEmpty(gene, biotye) ? null : gene + '_' + biotye + '_' + so;
    }

    public static String combine(String gene, String biotye) {
        return StringUtils.isAnyEmpty(gene, biotye) ? null : gene + '_' + biotye;
    }

    Put buildPut(VariantAnnotation variantAnnotation) {
        Map<PhoenixHelper.Column, ?> map = convert(variantAnnotation);

        byte[] bytesRowKey = generateVariantRowKey(variantAnnotation);
        Put put = new Put(bytesRowKey);

        for (PhoenixHelper.Column column : VariantPhoenixSchema.PRIMARY_KEY) {
            map.remove(column);
        }
        map.forEach((column, value) -> {
            try {
                add(put, column, value);
            } catch (Exception e) {
                logger.error("Error adding column " + column.column());
                throw e;
            }
        });

        return put;
    }

    private List<Float> sortProteinSubstitutionScores(Set<Float> scores) {
        List<Float> sorted = new ArrayList<>(scores.size());
        Float min = scores.stream().min(Float::compareTo).orElse(-1.0F);
        Float max = scores.stream().max(Float::compareTo).orElse(-1.0F);
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
