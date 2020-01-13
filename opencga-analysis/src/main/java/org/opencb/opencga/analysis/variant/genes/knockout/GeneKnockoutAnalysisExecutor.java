package org.opencb.opencga.analysis.variant.genes.knockout;

import org.opencb.biodata.models.core.Gene;
import org.opencb.biodata.models.core.Transcript;
import org.opencb.opencga.analysis.variant.genes.knockout.result.GeneKnockoutBySample;
import org.opencb.opencga.analysis.variant.genes.knockout.result.GeneKnockoutBySample.GeneKnockout;
import org.opencb.opencga.core.common.JacksonUtils;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.tools.OpenCgaToolExecutor;
import org.opencb.opencga.storage.core.metadata.models.Trio;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryUtils;

import java.io.*;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

import static org.opencb.opencga.analysis.variant.genes.knockout.result.GeneKnockoutBySample.*;

public abstract class GeneKnockoutAnalysisExecutor extends OpenCgaToolExecutor {

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
    private String fileNamePattern;
    private String biotype;

    public String getStudy() {
        return study;
    }

    public GeneKnockoutAnalysisExecutor setStudy(String study) {
        this.study = study;
        return this;
    }

    public List<String> getSamples() {
        return samples;
    }

    public GeneKnockoutAnalysisExecutor setSamples(List<String> samples) {
        this.samples = samples;
        return this;
    }

    public Map<String, Trio> getTrios() {
        return trios;
    }

    public GeneKnockoutAnalysisExecutor setTrios(Map<String, Trio> trios) {
        this.trios = trios;
        return this;
    }

    public Set<String> getProteinCodingGenes() {
        return proteinCodingGenes;
    }

    public GeneKnockoutAnalysisExecutor setProteinCodingGenes(Set<String> proteinCodingGenes) {
        this.proteinCodingGenes = proteinCodingGenes;
        return this;
    }

    public Set<String> getOtherGenes() {
        return otherGenes;
    }

    public GeneKnockoutAnalysisExecutor setOtherGenes(Set<String> otherGenes) {
        this.otherGenes = otherGenes;
        return this;
    }

    public GeneKnockoutAnalysisExecutor setBiotype(String biotype) {
        this.biotype = biotype;
        return this;
    }

    public String getBiotype() {
        return biotype;
    }

    public String getCt() {
        return ct;
    }

    public GeneKnockoutAnalysisExecutor setCt(String ct) {
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

    public GeneKnockoutAnalysisExecutor setFilter(String filter) {
        this.filter = filter;
        return this;
    }

    public String getQual() {
        return qual;
    }

    public GeneKnockoutAnalysisExecutor setQual(String qual) {
        this.qual = qual;
        return this;
    }

    public String getDisorder() {
        return disorder;
    }

    public GeneKnockoutAnalysisExecutor setDisorder(String disorder) {
        this.disorder = disorder;
        return this;
    }

    public String getFileNamePattern() {
        return fileNamePattern;
    }

    public GeneKnockoutAnalysisExecutor setFileNamePattern(String fileNamePattern) {
        this.fileNamePattern = fileNamePattern;
        return this;
    }

    public Path getFileName(String sample) {
        return Paths.get(fileNamePattern.replace("{sample}", sample));
    }

    protected void printSampleFile(String sample, Map<String, GeneKnockout> knockoutGenes, Trio trio)
            throws IOException {
        Path path = getFileName(sample);
        printSampleFileJson(sample, knockoutGenes, trio, path.toFile());
//            printSampleFileTsv(sample, knockoutGenes, transcriptKnockoutCountMap, trio, file.toFile());
    }

    private void printSampleFileJson(String sample, Map<String, GeneKnockout> knockoutGenes, Trio trio, File file) throws IOException {
        GeneKnockoutBySample geneKnockoutBySample = new GeneKnockoutBySample()
                .setSample(sample)
                .setTrio(trio)
                .setCountByType(new CountByType(
                        knockoutGenes.values().stream().flatMap(g -> g.getTranscripts().stream())
                                .mapToInt(TranscriptKnockout::getHomAltCount).sum(),
                        knockoutGenes.values().stream().flatMap(g -> g.getTranscripts().stream())
                                .mapToInt(TranscriptKnockout::getMultiAllelicCount).sum(),
                        knockoutGenes.values().stream().flatMap(g -> g.getTranscripts().stream())
                                .mapToInt(TranscriptKnockout::getCompHetCount).sum(),
                        knockoutGenes.values().stream().flatMap(g -> g.getTranscripts().stream())
                                .mapToInt(TranscriptKnockout::getDeletionOverlapCount).sum()
                ))
                .setGenesCount(knockoutGenes.size())
                .setTranscriptsCount(knockoutGenes.values().stream().mapToInt(g -> g.getTranscripts().size()).sum())
                .setGenes(knockoutGenes.values());

        JacksonUtils.getDefaultObjectMapper().writerWithDefaultPrettyPrinter().writeValue(file, geneKnockoutBySample);
    }

    private void printSampleFileTsv(String sample, Map<String, Gene> knockoutGenes, Map<String, TranscriptKnockout> transcriptKnockoutCountMap, Trio trio, File file) throws FileNotFoundException {

        try (PrintStream out = new PrintStream(new FileOutputStream(file))) {
            out.println("##SAMPLE=" + sample);
            out.println("##DATE=" + TimeUtils.getDate());
            if (trio != null) {
                out.println("##FAMILY=" + trio.getId());
            }
            out.println("##num_genes=" + knockoutGenes.size());
            out.println("##num_transcripts=" + knockoutGenes.values().stream().mapToInt(g -> g.getTranscripts().size()).sum());
            out.println("#GENE\tGENE_ID\tTRANSCRIPT\tHOM_ALT\tMULTI_ALLELIC\tCOMP_HET\tDELETION_OVERLAP");
            for (Gene gene : knockoutGenes.values()) {
//                    out.print(gene.getName() + "\t" + gene.getId() + "\t");
//                    int i = 0;
//                    for (Transcript transcript : gene.getTranscripts()) {
//                        if (i++ > 0) {
//                            out.print(",");
//                        }
//                        out.print(transcript.getId());
//                    }
//                    out.println();
                for (Transcript transcript : gene.getTranscripts()) {
                    TranscriptKnockout count = transcriptKnockoutCountMap.get(transcript.getId());
                    out.println(gene.getName()
                            + "\t" + gene.getId()
                            + "\t" + transcript.getId()
                            + "\t" + count.getHomAltCount()
                            + "\t" + count.getMultiAllelicCount()
                            + "\t" + count.getCompHetCount()
                            + "\t" + count.getDeletionOverlapCount());
                }
            }
        }
    }

}

