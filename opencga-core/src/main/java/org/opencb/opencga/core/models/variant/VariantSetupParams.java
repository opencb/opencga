package org.opencb.opencga.core.models.variant;

import org.opencb.opencga.core.tools.ToolParams;

public class VariantSetupParams extends ToolParams {

    private Integer expectedSamplesNumber;

    private Integer expectedFilesNumber;

    private FileType fileType;

    private Long averageFileSize;

    private Integer numberOfVariantsPerSample;

    private Float samplesPerFile;

    private DataDistribution dataDistribution;

    public VariantSetupParams(VariantSetupParams params) {
        this.expectedSamplesNumber = params.expectedSamplesNumber;
        this.expectedFilesNumber = params.expectedFilesNumber;
        this.fileType = params.fileType;
        this.averageFileSize = params.averageFileSize;
        this.numberOfVariantsPerSample = params.numberOfVariantsPerSample;
        this.samplesPerFile = params.samplesPerFile;
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

    public Integer getExpectedSamplesNumber() {
        return expectedSamplesNumber;
    }

    public VariantSetupParams setExpectedSamplesNumber(Integer expectedSamplesNumber) {
        this.expectedSamplesNumber = expectedSamplesNumber;
        return this;
    }

    public Integer getExpectedFilesNumber() {
        return expectedFilesNumber;
    }

    public VariantSetupParams setExpectedFilesNumber(Integer expectedFilesNumber) {
        this.expectedFilesNumber = expectedFilesNumber;
        return this;
    }

    public FileType getFileType() {
        return fileType;
    }

    public VariantSetupParams setFileType(FileType fileType) {
        this.fileType = fileType;
        return this;
    }

    public Long getAverageFileSize() {
        return averageFileSize;
    }

    public VariantSetupParams setAverageFileSize(Long averageFileSize) {
        this.averageFileSize = averageFileSize;
        return this;
    }

    public Integer getNumberOfVariantsPerSample() {
        return numberOfVariantsPerSample;
    }

    public VariantSetupParams setNumberOfVariantsPerSample(Integer numberOfVariantsPerSample) {
        this.numberOfVariantsPerSample = numberOfVariantsPerSample;
        return this;
    }

    public Float getSamplesPerFile() {
        return samplesPerFile;
    }

    public VariantSetupParams setSamplesPerFile(Float samplesPerFile) {
        this.samplesPerFile = samplesPerFile;
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
