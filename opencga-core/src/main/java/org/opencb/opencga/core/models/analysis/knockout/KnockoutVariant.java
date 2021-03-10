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

package org.opencb.opencga.core.models.analysis.knockout;

import htsjdk.variant.vcf.VCFConstants;
import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.*;

import java.util.*;

public class KnockoutVariant {

    private String id;
    private VariantType type;
    private String genotype;
    private Integer depth;
    private String filter;
    private String qual;
    private KnockoutType knockoutType;
    private List<PopulationFrequency> populationFrequencies;
    private List<SequenceOntologyTerm> sequenceOntologyTerms;
    private List<ClinicalSignificance> clinicalSignificance;

    public enum KnockoutType {
        HOM_ALT,
        COMP_HET,
        HET_ALT,
        DELETION_OVERLAP
    }

    public KnockoutVariant() {
    }

    public KnockoutVariant(Variant variant, StudyEntry study, FileEntry file, SampleEntry sample, VariantAnnotation annotation,
                           ConsequenceType ct, KnockoutType knockoutType) {
        this.id = variant.toString();
        this.type = variant.getType();
        this.genotype = sample.getData().get(0);
        this.depth = getDepth(study, file, sample);
        this.filter = file.getData().get(StudyEntry.FILTER);
        this.qual = file.getData().get(StudyEntry.QUAL);
        this.knockoutType = knockoutType;
        this.sequenceOntologyTerms = ct == null ? null : ct.getSequenceOntologyTerms();
        this.populationFrequencies = annotation.getPopulationFrequencies();
        this.clinicalSignificance = getClinicalSignificance(annotation);
    }

    public KnockoutVariant(String id, VariantType type, String genotype, Integer depth, String filter, String qual,
                           KnockoutType knockoutType, List<SequenceOntologyTerm> sequenceOntologyTerms,
                           List<PopulationFrequency> populationFrequencies, List<ClinicalSignificance> clinicalSignificance) {
        this.id = id;
        this.type = type;
        this.genotype = genotype;
        this.depth = depth;
        this.filter = filter;
        this.qual = qual;
        this.knockoutType = knockoutType;
        this.sequenceOntologyTerms = sequenceOntologyTerms;
        this.populationFrequencies = populationFrequencies;
        this.clinicalSignificance = clinicalSignificance;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("KnockoutVariant{");
        sb.append("id='").append(id).append('\'');
        sb.append(", genotype='").append(genotype).append('\'');
        sb.append(", filter='").append(filter).append('\'');
        sb.append(", qual='").append(qual).append('\'');
        sb.append(", knockoutType=").append(knockoutType);
        sb.append(", populationFrequencies=").append(populationFrequencies);
        sb.append(", sequenceOntologyTerms=").append(sequenceOntologyTerms);
        sb.append('}');
        return sb.toString();
    }

    public String getId() {
        return id;
    }

    public KnockoutVariant setId(String id) {
        this.id = id;
        return this;
    }

    public VariantType getType() {
        return type;
    }

    public KnockoutVariant setType(VariantType type) {
        this.type = type;
        return this;
    }

    public String getGenotype() {
        return genotype;
    }

    public KnockoutVariant setGenotype(String genotype) {
        this.genotype = genotype;
        return this;
    }

    public Integer getDepth() {
        return depth;
    }

    public KnockoutVariant setDepth(Integer depth) {
        this.depth = depth;
        return this;
    }

    public String getFilter() {
        return filter;
    }

    public KnockoutVariant setFilter(String filter) {
        this.filter = filter;
        return this;
    }

    public String getQual() {
        return qual;
    }

    public KnockoutVariant setQual(String qual) {
        this.qual = qual;
        return this;
    }

    public KnockoutType getKnockoutType() {
        return knockoutType;
    }

    public KnockoutVariant setKnockoutType(KnockoutType knockoutType) {
        this.knockoutType = knockoutType;
        return this;
    }

    public List<SequenceOntologyTerm> getSequenceOntologyTerms() {
        return sequenceOntologyTerms;
    }

    public KnockoutVariant setSequenceOntologyTerms(List<SequenceOntologyTerm> sequenceOntologyTerms) {
        this.sequenceOntologyTerms = sequenceOntologyTerms;
        return this;
    }

    public List<PopulationFrequency> getPopulationFrequencies() {
        return populationFrequencies;
    }

    public KnockoutVariant setPopulationFrequencies(List<PopulationFrequency> populationFrequencies) {
        this.populationFrequencies = populationFrequencies;
        return this;
    }

    public List<ClinicalSignificance> getClinicalSignificance() {
        return clinicalSignificance;
    }

    public KnockoutVariant setClinicalSignificance(List<ClinicalSignificance> clinicalSignificance) {
        this.clinicalSignificance = clinicalSignificance;
        return this;
    }


    public static Integer getDepth(StudyEntry study, FileEntry file, SampleEntry sample) {
        Integer dpId = study.getSampleDataKeyPosition(VCFConstants.DEPTH_KEY);
        if (dpId != null) {
            String dpStr = sample.getData().get(dpId);
            if (StringUtils.isNumeric(dpStr)) {
                return Integer.valueOf(dpStr);
            }
        }
        return null;
    }

    public static List<ClinicalSignificance> getClinicalSignificance(VariantAnnotation annotation) {
        if (annotation.getTraitAssociation() != null) {
            Set<ClinicalSignificance> uniqueValues = new HashSet<>();
            for (EvidenceEntry evidenceEntry : annotation.getTraitAssociation()) {
                if (evidenceEntry.getVariantClassification() != null
                        && evidenceEntry.getVariantClassification().getClinicalSignificance() != null) {
                    uniqueValues.add(evidenceEntry.getVariantClassification().getClinicalSignificance());
                }
            }
            return new ArrayList<>(uniqueValues);
        }
        return Collections.emptyList();
    }
}
