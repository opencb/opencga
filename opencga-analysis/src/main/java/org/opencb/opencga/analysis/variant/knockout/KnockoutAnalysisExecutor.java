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

package org.opencb.opencga.analysis.variant.knockout;

import com.fasterxml.jackson.databind.ObjectWriter;
import org.opencb.commons.utils.FileUtils;
import org.opencb.opencga.core.common.JacksonUtils;
import org.opencb.opencga.core.models.analysis.knockout.KnockoutByGene;
import org.opencb.opencga.core.models.analysis.knockout.KnockoutByIndividual;
import org.opencb.opencga.core.models.analysis.knockout.KnockoutByIndividual.KnockoutGene;
import org.opencb.opencga.core.models.analysis.knockout.KnockoutVariant;
import org.opencb.opencga.core.tools.OpenCgaToolExecutor;
import org.opencb.opencga.storage.core.metadata.models.Trio;
import org.opencb.opencga.storage.core.variant.query.VariantQueryUtils;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

public abstract class KnockoutAnalysisExecutor extends OpenCgaToolExecutor {

    private String study;
    private List<String> samples;
    private Map<String, Trio> trios;

    private Set<String> proteinCodingGenes;
    private Set<String> otherGenes;
    private String ct;
    private Set<String> cts;
    private String filter;
    private String qual;

    private String disorder;
    private String sampleFileNamePattern;
    private String geneFileNamePattern;
    private String biotype;

    public String getStudy() {
        return study;
    }

    public KnockoutAnalysisExecutor setStudy(String study) {
        this.study = study;
        return this;
    }

    public List<String> getSamples() {
        return samples;
    }

    public KnockoutAnalysisExecutor setSamples(List<String> samples) {
        this.samples = samples;
        return this;
    }

    public Map<String, Trio> getTrios() {
        return trios;
    }

    public KnockoutAnalysisExecutor setTrios(Map<String, Trio> trios) {
        this.trios = trios;
        return this;
    }

    public Set<String> getProteinCodingGenes() {
        return proteinCodingGenes;
    }

    public KnockoutAnalysisExecutor setProteinCodingGenes(Set<String> proteinCodingGenes) {
        this.proteinCodingGenes = proteinCodingGenes;
        return this;
    }

    public Set<String> getOtherGenes() {
        return otherGenes;
    }

    public KnockoutAnalysisExecutor setOtherGenes(Set<String> otherGenes) {
        this.otherGenes = otherGenes;
        return this;
    }

    public KnockoutAnalysisExecutor setBiotype(String biotype) {
        this.biotype = biotype;
        return this;
    }

    public String getBiotype() {
        return biotype;
    }

    public String getCt() {
        return ct;
    }

    public KnockoutAnalysisExecutor setCt(String ct) {
        this.ct = ct;
        if (ct != null && !ct.isEmpty()) {
            cts = new HashSet<>(VariantQueryUtils.parseConsequenceTypes(Arrays.asList(ct.split(","))));
        } else {
            cts = Collections.emptySet();
        }
        return this;
    }

    public Set<String> getCts() {
        return cts;
    }

    public String getFilter() {
        return filter;
    }

    public KnockoutAnalysisExecutor setFilter(String filter) {
        this.filter = filter;
        return this;
    }

    public String getQual() {
        return qual;
    }

    public KnockoutAnalysisExecutor setQual(String qual) {
        this.qual = qual;
        return this;
    }

    public String getDisorder() {
        return disorder;
    }

    public KnockoutAnalysisExecutor setDisorder(String disorder) {
        this.disorder = disorder;
        return this;
    }

    public String getSampleFileNamePattern() {
        return sampleFileNamePattern;
    }

    public KnockoutAnalysisExecutor setSampleFileNamePattern(String sampleFileNamePattern) {
        this.sampleFileNamePattern = sampleFileNamePattern;
        return this;
    }

    public Path getSampleFileName(String sample) {
        if (sample == null) {
            Objects.requireNonNull(sample);
        }
        return Paths.get(sampleFileNamePattern.replace("{sample}", sample));
    }

    public String getGeneFileNamePattern() {
        return geneFileNamePattern;
    }

    public KnockoutAnalysisExecutor setGeneFileNamePattern(String geneFileNamePattern) {
        this.geneFileNamePattern = geneFileNamePattern;
        return this;
    }

    protected Path getGeneFileName(String gene) {
        return Paths.get(geneFileNamePattern.replace("{gene}", gene));
    }

    protected KnockoutByIndividual.GeneKnockoutByIndividualStats getGeneKnockoutBySampleStats(Collection<KnockoutGene> knockoutGenes) {
        KnockoutByIndividual.GeneKnockoutByIndividualStats stats = new KnockoutByIndividual.GeneKnockoutByIndividualStats()
                .setNumGenes(knockoutGenes.size())
                .setNumTranscripts(knockoutGenes.stream().mapToInt(g -> g.getTranscripts().size()).sum());
        for (KnockoutVariant.KnockoutType type : KnockoutVariant.KnockoutType.values()) {
            long count = knockoutGenes.stream().flatMap(g -> g.getTranscripts().stream())
                    .flatMap(t -> t.getVariants().stream())
                    .filter(v -> v.getKnockoutType().equals(type))
                    .map(KnockoutVariant::getId)
                    .collect(Collectors.toSet())
                    .size();
            stats.getByType().put(type, count);
        }
        return stats;
    }

    protected void writeSampleFile(KnockoutByIndividual knockoutByIndividual) throws IOException {
        Path file = getSampleFileName(knockoutByIndividual.getSampleId());
        ObjectWriter writer = JacksonUtils.getDefaultObjectMapper().writerFor(KnockoutByIndividual.class);
        try (BufferedWriter bufferedWriter = FileUtils.newBufferedWriter(file)) {
            writer.writeValue(bufferedWriter, knockoutByIndividual);
        }
    }

    protected KnockoutByIndividual readSampleFile(String sample) throws IOException {
        Path file = getSampleFileName(sample);
        try (BufferedReader reader = FileUtils.newBufferedReader(file)) {
            return JacksonUtils.getDefaultObjectMapper().readValue(reader, KnockoutByIndividual.class);
        }
    }

    protected void writeGeneFile(KnockoutByGene knockoutByGene) throws IOException {
        Path file = getGeneFileName(knockoutByGene.getName());
        ObjectWriter writer = JacksonUtils.getDefaultObjectMapper().writerFor(KnockoutByGene.class);
        try (BufferedWriter bufferedWriter = FileUtils.newBufferedWriter(file)) {
            writer.writeValue(bufferedWriter, knockoutByGene);
        }
    }

    protected KnockoutByGene readGeneFile(String gene) throws IOException {
        Path file = getGeneFileName(gene);
        try (BufferedReader reader = FileUtils.newBufferedReader(file)) {
            return JacksonUtils.getDefaultObjectMapper().readValue(reader, KnockoutByGene.class);
        }
    }
}

