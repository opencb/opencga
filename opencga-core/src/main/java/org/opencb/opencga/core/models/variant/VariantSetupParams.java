package org.opencb.opencga.core.models.variant;

import org.opencb.opencga.core.tools.ToolParams;

public class VariantSetupParams extends ToolParams {

    private Integer expectedSamples;

    private Integer expectedFiles;

    private FileType fileType;

    private String averageFileSize;

    private Integer variantsPerSample;

    private Float averageSamplesPerFile;

    private DataDistribution dataDistribution;

    public VariantSetupParams(VariantSetupParams params) {
        this.expectedSamples = params.expectedSamples;
        this.expectedFiles = params.expectedFiles;
        this.fileType = params.fileType;
        this.averageFileSize = params.averageFileSize;
        this.variantsPerSample = params.variantsPerSample;
        this.averageSamplesPerFile = params.averageSamplesPerFile;
        this.dataDistribution = params.dataDistribution;
    }

    public VariantSetupParams() {
    }

    public enum DataDistribution {
        // Single sample VCF files. One file per sample.
        // e.g.
        //   - Platinum gVCF
        //   - Cancer germline
        //   - RD germline without family calling
        SINGLE_SAMPLE_FILES,
        // Multi samples VCF files. One file with multiple samples.
        // e.g.
        //   - Corpasome
        //   - RD germline with family calling
        MULTI_SAMPLE_FILES,
        // Multiple files per sample. Each file might have multiple samples.
        // e.g.
        //   - Somatic study with multiple callers
        MULTIPLE_FILE_PER_SAMPLE,
        // Large aggregated/joined/merged files. Each file has all samples. Each file contains a specific set of chromosomes.
        // e.g.
        //   - 1000 genomes
        MULTI_SAMPLE_FILES_SPLIT_BY_CHROMOSOME,
        // Large aggregated/joined/merged files. Each file has all samples. Each file contains a specific region.
        MULTI_SAMPLE_FILES_SPLIT_BY_REGION,
    }

    public enum FileType {
        GENOME_VCF,
        GENOME_gVCF,
        EXOME
    }

    public Integer getExpectedSamples() {
        return expectedSamples;
    }

    public VariantSetupParams setExpectedSamples(Integer expectedSamples) {
        this.expectedSamples = expectedSamples;
        return this;
    }

    public Integer getExpectedFiles() {
        return expectedFiles;
    }

    public VariantSetupParams setExpectedFiles(Integer expectedFiles) {
        this.expectedFiles = expectedFiles;
        return this;
    }

    public FileType getFileType() {
        return fileType;
    }

    public VariantSetupParams setFileType(FileType fileType) {
        this.fileType = fileType;
        return this;
    }

    public String getAverageFileSize() {
        return averageFileSize;
    }

    public VariantSetupParams setAverageFileSize(String averageFileSize) {
        this.averageFileSize = averageFileSize;
        return this;
    }

    public Integer getVariantsPerSample() {
        return variantsPerSample;
    }

    public VariantSetupParams setVariantsPerSample(Integer variantsPerSample) {
        this.variantsPerSample = variantsPerSample;
        return this;
    }

    public Float getAverageSamplesPerFile() {
        return averageSamplesPerFile;
    }

    public VariantSetupParams setAverageSamplesPerFile(Float averageSamplesPerFile) {
        this.averageSamplesPerFile = averageSamplesPerFile;
        return this;
    }

    public DataDistribution getDataDistribution() {
        return dataDistribution;
    }

    public VariantSetupParams setDataDistribution(DataDistribution dataDistribution) {
        this.dataDistribution = dataDistribution;
        return this;
    }
}
