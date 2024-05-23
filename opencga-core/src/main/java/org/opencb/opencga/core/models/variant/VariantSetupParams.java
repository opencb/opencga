package org.opencb.opencga.core.models.variant;

import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.tools.ToolParams;

import java.util.List;

public class VariantSetupParams extends ToolParams {

    @DataField(description = "Expected number of samples that will be loaded. Used to infer some parameters. "
            + "This number is only used as a hint. "
            + "If the real number of samples is different, if it grows beyond expectation, or if , the loader should be able to handle it.",
            required = true)
    private Integer expectedSamples;

    @DataField(description = "Expected number of files that will be loaded. Used to infer some parameters. "
            + "This number is only used as a hint. "
            + "If the real number of files is different, the loader should be able to handle it.", required = true)
    private Integer expectedFiles;

    @DataField(description = "Main type of the files that will be loaded. If the dataset contains multiple types of files,"
            + " provide the one that matches most of the files.")
    private FileType fileType;

    @DataField(description = "Average size of the files that will be loaded. This number is only used as a hint. "
            + "If the real size of the files is different, the loader should be able to handle it. Accepts units. e.g. 435MB, 2GB, 86KB. "
            + "If not provided, the value will be inferred from the file type.")
    private String averageFileSize;

    @DataField(description = "Number of variants per sample. This number is only used as a hint. "
            + "If the real number of variants per sample is different, the loader should be able to handle it. "
            + "If not provided, the value will be inferred from the file type.")
    private Integer variantsPerSample;

    @DataField(description = "Average number of samples per file. This number is only used as a hint. "
            + "If the real number of samples per file is different, the loader should be able to handle it. "
            + "If not provided, the value will be inferred from the expectedSamples and expectedFiles and dataDistribution.")
    private Float averageSamplesPerFile;

    @DataField(description = "Data distribution of the files. This parameter is used to infer the number of samples per file.")
    private DataDistribution dataDistribution;

    @DataField(description = "List of normalization extensions")
    private List<String> normalizeExtensions;

    public VariantSetupParams(VariantSetupParams params) {
        this.expectedSamples = params.expectedSamples;
        this.expectedFiles = params.expectedFiles;
        this.fileType = params.fileType;
        this.averageFileSize = params.averageFileSize;
        this.variantsPerSample = params.variantsPerSample;
        this.averageSamplesPerFile = params.averageSamplesPerFile;
        this.dataDistribution = params.dataDistribution;
        this.normalizeExtensions = params.normalizeExtensions;
    }

    public VariantSetupParams() {
    }

    public enum DataDistribution {
        // Single sample VCF files. One file per sample.
        // e.g.
        //   - Platinum gVCF
        //   - Cancer germline
        //   - RD germline without family calling
        @DataField(description = "Single sample VCF files. One file per sample. e.g. Platinum gVCF, Cancer germline, RD germline without family calling")
        SINGLE_SAMPLE_PER_FILE,

        // Multi samples VCF files. One file with multiple samples.
        // e.g.
        //   - Corpasome
        //   - RD germline with family calling
        @DataField(description = "Multi samples VCF files. One file with multiple samples. e.g. Corpasome, RD germline with family calling")
        MULTIPLE_SAMPLES_PER_FILE,

        // Multiple files per sample. Each file might have multiple samples.
        // e.g.
        //   - Somatic study with multiple callers
        @DataField(description = "Multiple files per sample. Each file might have multiple samples. e.g. Somatic study with multiple callers")
        MULTIPLE_FILES_PER_SAMPLE,

        // Large aggregated/joined/merged files. Each file has all samples. Each file contains a specific set of chromosomes.
        // e.g.
        //   - 1000 genomes
        @DataField(description = "Large aggregated/joined/merged files. Each file has all samples. Each file contains a specific set of chromosomes. e.g. 1000 genomes")
        FILES_SPLIT_BY_CHROMOSOME,

        // Large aggregated/joined/merged files. Each file has all samples. Each file contains an arbitrary region.
        @DataField(description = "Large aggregated/joined/merged files. Each file has all samples. Each file contains an arbitrary region.")
        FILES_SPLIT_BY_REGION,
    }

    public enum FileType {
        @DataField(description = "Whole genome VCF file.")
        GENOME_VCF,
        @DataField(description = "Whole genome gVCF file.")
        GENOME_gVCF,
        @DataField(description = "Exome VCF file.")
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

    public List<String> getNormalizeExtensions() {
        return normalizeExtensions;
    }

    public VariantSetupParams setNormalizeExtensions(List<String> normalizeExtensions) {
        this.normalizeExtensions = normalizeExtensions;
        return this;
    }
}
