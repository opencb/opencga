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
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.*;
import org.opencb.biodata.models.variant.stats.VariantStats;

import java.util.*;

public class KnockoutVariant {

    private String id;
    private String dbSnp;
    private String chromosome;
    private int start;
    private int end;
    private int length;
    private String reference;
    private String alternate;
    private VariantType type;
    private String genotype;
    private Integer depth;
    private String filter;
    private String qual;
    private VariantStats stats;
    private KnockoutType knockoutType;
    private ParentalOrigin parentalOrigin = ParentalOrigin.UNKNOWN;
    private List<PopulationFrequency> populationFrequencies;
    private List<SequenceOntologyTerm> sequenceOntologyTerms;
    private List<ClinicalSignificance> clinicalSignificance;

    public enum KnockoutType {
        HOM_ALT,
        COMP_HET,
        HET_ALT,
        DELETION_OVERLAP
    }
    public enum ParentalOrigin {
        PATERNAL,
        MATERNAL,
        BOTH,
        UNKNOWN
    }

    public KnockoutVariant() {
    }

    public KnockoutVariant(Variant variant, StudyEntry study, FileEntry file, SampleEntry sample, VariantAnnotation annotation,
                           ConsequenceType ct, KnockoutType knockoutType) {
        this.id = variant.toString();
        this.dbSnp = annotation != null ? annotation.getId() : null;
        this.chromosome = variant.getChromosome();
        this.start = variant.getStart();
        this.end = variant.getEnd();
        this.length = variant.getLength();
        this.reference = variant.getReference();
        this.alternate = variant.getAlternate();
        this.type = variant.getType();
        this.genotype = sample != null && CollectionUtils.isNotEmpty(sample.getData()) ? sample.getData().get(0) : null;
        this.depth = getDepth(study, file, sample);
        this.filter = file != null ? file.getData().get(StudyEntry.FILTER) : null;
        this.qual = file != null ? file.getData().get(StudyEntry.QUAL) : null;
        this.stats = study != null ? study.getStats(StudyEntry.DEFAULT_COHORT) : null;
        this.knockoutType = knockoutType;
        this.sequenceOntologyTerms = ct == null ? null : ct.getSequenceOntologyTerms();
        this.populationFrequencies = annotation != null ? annotation.getPopulationFrequencies() : null;
        this.clinicalSignificance = getClinicalSignificance(annotation);
    }

    public KnockoutVariant(String id, String dbSnp, String genotype, Integer depth, String filter, String qual, VariantStats stats,
                           KnockoutType knockoutType, List<SequenceOntologyTerm> sequenceOntologyTerms,
                           List<PopulationFrequency> populationFrequencies, List<ClinicalSignificance> clinicalSignificance) {
        this(new Variant(id), dbSnp, genotype, depth, filter, qual, stats, knockoutType, sequenceOntologyTerms, populationFrequencies, clinicalSignificance);
    }

    public KnockoutVariant(Variant variant, String dbSnp, String genotype, Integer depth, String filter, String qual, VariantStats stats,
                           KnockoutType knockoutType, List<SequenceOntologyTerm> sequenceOntologyTerms,
                           List<PopulationFrequency> populationFrequencies, List<ClinicalSignificance> clinicalSignificance) {
        this.id = variant.toString();
        this.dbSnp = dbSnp;
        this.chromosome = variant.getChromosome();
        this.start = variant.getStart();
        this.end = variant.getEnd();
        this.length = variant.getLength();
        this.reference = variant.getReference();
        this.alternate = variant.getAlternate();
        this.type = variant.getType();
        this.genotype = genotype;
        this.depth = depth;
        this.filter = filter;
        this.qual = qual;
        this.stats = stats;
        this.knockoutType = knockoutType;
        this.sequenceOntologyTerms = sequenceOntologyTerms;
        this.populationFrequencies = populationFrequencies;
        this.clinicalSignificance = clinicalSignificance;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;

        if (o == null || getClass() != o.getClass()) return false;

        KnockoutVariant that = (KnockoutVariant) o;

        return new EqualsBuilder()
                .append(start, that.start)
                .append(end, that.end)
                .append(length, that.length)
                .append(id, that.id)
                .append(dbSnp, that.dbSnp)
                .append(chromosome, that.chromosome)
                .append(reference, that.reference)
                .append(alternate, that.alternate)
                .append(type, that.type)
                .append(genotype, that.genotype)
                .append(depth, that.depth)
                .append(filter, that.filter)
                .append(qual, that.qual)
                .append(stats, that.stats)
                .append(knockoutType, that.knockoutType)
                .append(populationFrequencies, that.populationFrequencies)
                .append(sequenceOntologyTerms, that.sequenceOntologyTerms)
                .append(clinicalSignificance, that.clinicalSignificance)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .append(id)
                .append(dbSnp)
                .append(chromosome)
                .append(start)
                .append(end)
                .append(length)
                .append(reference)
                .append(alternate)
                .append(type)
                .append(genotype)
                .append(depth)
                .append(filter)
                .append(qual)
                .append(stats)
                .append(knockoutType)
                .append(populationFrequencies)
                .append(sequenceOntologyTerms)
                .append(clinicalSignificance)
                .toHashCode();
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("KnockoutVariant{");
        sb.append("id='").append(id).append('\'');
        sb.append(", chromosome='").append(chromosome).append('\'');
        sb.append(", start=").append(start);
        sb.append(", end=").append(end);
        sb.append(", length=").append(length);
        sb.append(", reference='").append(reference).append('\'');
        sb.append(", alternate='").append(alternate).append('\'');
        sb.append(", type=").append(type);
        sb.append(", genotype='").append(genotype).append('\'');
        sb.append(", depth=").append(depth);
        sb.append(", filter='").append(filter).append('\'');
        sb.append(", qual='").append(qual).append('\'');
        sb.append(", knockoutType=").append(knockoutType);
        sb.append(", populationFrequencies=").append(populationFrequencies);
        sb.append(", sequenceOntologyTerms=").append(sequenceOntologyTerms);
        sb.append(", clinicalSignificance=").append(clinicalSignificance);
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

    public String getDbSnp() {
        return dbSnp;
    }

    public KnockoutVariant setDbSnp(String dbSnp) {
        this.dbSnp = dbSnp;
        return this;
    }

    public String getChromosome() {
        return chromosome;
    }

    public KnockoutVariant setChromosome(String chromosome) {
        this.chromosome = chromosome;
        return this;
    }

    public int getStart() {
        return start;
    }

    public KnockoutVariant setStart(int start) {
        this.start = start;
        return this;
    }

    public int getEnd() {
        return end;
    }

    public KnockoutVariant setEnd(int end) {
        this.end = end;
        return this;
    }

    public int getLength() {
        return length;
    }

    public KnockoutVariant setLength(int length) {
        this.length = length;
        return this;
    }

    public String getReference() {
        return reference;
    }

    public KnockoutVariant setReference(String reference) {
        this.reference = reference;
        return this;
    }

    public String getAlternate() {
        return alternate;
    }

    public KnockoutVariant setAlternate(String alternate) {
        this.alternate = alternate;
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

    public VariantStats getStats() {
        return stats;
    }

    public KnockoutVariant setStats(VariantStats stats) {
        this.stats = stats;
        return this;
    }

    public KnockoutType getKnockoutType() {
        return knockoutType;
    }

    public KnockoutVariant setKnockoutType(KnockoutType knockoutType) {
        this.knockoutType = knockoutType;
        return this;
    }

    public ParentalOrigin getParentalOrigin() {
        return parentalOrigin;
    }

    public KnockoutVariant setParentalOrigin(ParentalOrigin parentalOrigin) {
        this.parentalOrigin = parentalOrigin;
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
        if (sample == null || study == null) {
            return null;
        }
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
        if (annotation != null && annotation.getTraitAssociation() != null) {
            Set<ClinicalSignificance> uniqueValues = new HashSet<>();
            for (EvidenceEntry evidenceEntry : annotation.getTraitAssociation()) {
                if (evidenceEntry.getVariantClassification() != null
                        && evidenceEntry.getVariantClassification().getClinicalSignificance() != null) {
                    uniqueValues.add(evidenceEntry.getVariantClassification().getClinicalSignificance());
                }
            }
            return new ArrayList<>(uniqueValues);
        }
        return null;
    }
}
