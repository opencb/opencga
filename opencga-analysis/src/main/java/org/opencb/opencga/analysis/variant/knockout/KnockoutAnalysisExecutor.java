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
import org.opencb.opencga.analysis.variant.knockout.result.KnockoutByGene;
import org.opencb.opencga.analysis.variant.knockout.result.KnockoutBySample;
import org.opencb.opencga.analysis.variant.knockout.result.KnockoutBySample.KnockoutGene;
import org.opencb.opencga.analysis.variant.knockout.result.KnockoutVariant;
import org.opencb.opencga.core.common.JacksonUtils;
import org.opencb.opencga.core.tools.OpenCgaToolExecutor;
import org.opencb.opencga.storage.core.metadata.models.Trio;
import org.opencb.opencga.storage.core.variant.query.VariantQueryUtils;

import java.io.File;
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

    protected KnockoutBySample.GeneKnockoutBySampleStats getGeneKnockoutBySampleStats(Collection<KnockoutGene> knockoutGenes) {
        KnockoutBySample.GeneKnockoutBySampleStats stats = new KnockoutBySample.GeneKnockoutBySampleStats()
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

    protected void writeSampleFile(KnockoutBySample knockoutBySample) throws IOException {
        File file = getSampleFileName(knockoutBySample.getSample().getId()).toFile();
        ObjectWriter writer = JacksonUtils.getDefaultObjectMapper().writerFor(KnockoutBySample.class).withDefaultPrettyPrinter();
        writer.writeValue(file, knockoutBySample);
    }

    protected KnockoutBySample readSampleFile(String sample) throws IOException {
        File file = getSampleFileName(sample).toFile();
        return JacksonUtils.getDefaultObjectMapper().readValue(file, KnockoutBySample.class);
    }

    protected void writeGeneFile(KnockoutByGene knockoutByGene) throws IOException {
        File file = getGeneFileName(knockoutByGene.getName()).toFile();
        ObjectWriter writer = JacksonUtils.getDefaultObjectMapper().writerFor(KnockoutByGene.class).withDefaultPrettyPrinter();
        writer.writeValue(file, knockoutByGene);
    }

    protected KnockoutByGene readGeneFile(String gene) throws IOException {
        File file = getGeneFileName(gene).toFile();
        return JacksonUtils.getDefaultObjectMapper().readValue(file, KnockoutByGene.class);
    }
}

