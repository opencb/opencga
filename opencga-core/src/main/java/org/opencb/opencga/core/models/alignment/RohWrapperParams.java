package org.opencb.opencga.core.models.alignment;

import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.FieldConstants;
import org.opencb.opencga.core.tools.ToolParams;

import java.util.Map;

import static org.opencb.opencga.core.api.ParamConstants.SAMTOOLS_COMMAND_DESCRIPTION;

public class RohWrapperParams extends ToolParams {
    public static final String DESCRIPTION = "ROH parameters.";

    @DataField(id = "sampleId", description = FieldConstants.ROH_SAMPLE_ID_DESCRIPTION, required = true)
    private String sampleId;

    @DataField(id = "chromosome", description = FieldConstants.ROH_CHROMOSOME_DESCRIPTION, required = true)
    private String chromosome;

    @DataField(id = "filter", description = FieldConstants.ROH_FILTER_DESCRIPTION, defaultValue = "PASS")
    private String filter;

    @DataField(id = "genotypeQuality", description = FieldConstants.ROH_GENOTYPE_QUALITY_DESCRIPTION, defaultValue = "40")
    private Integer genotypeQuality;

    @DataField(id = "skipGenotypeQuality", description = FieldConstants.ROH_SKIP_GENOTYPE_QUALITY_DESCRIPTION)
    private Boolean skipGenotypeQuality;

    @DataField(id = "homozygWindowSnp", description = FieldConstants.ROH_HOMOZYG_WINDOW_SNP_DESCRIPTION, defaultValue = "50")
    private Integer homozygWindowSnp;

    @DataField(id = "homozygWindowHet", description = FieldConstants.ROH_HOMOZYG_WINDOW_HET_DESCRIPTION, defaultValue = "1")
    private Integer homozygWindowHet;

    @DataField(id = "homozygWindowMissing", description = FieldConstants.ROH_HOMOZYG_WINDOW_MISSING_DESCRIPTION, defaultValue = "5")
    private Integer homozygWindowMissing;

    @DataField(id = "homozygWindowThreshold", description = FieldConstants.ROH_HOMOZYG_WINDOW_THRESHOLD_DESCRIPTION, defaultValue = "0.05")
    private Float homozygWindowThreshold;

    @DataField(id = "homozygKb", description = FieldConstants.ROH_HOMOZYG_KB_DESCRIPTION, defaultValue = "1000")
    private Integer homozygKb;

    @DataField(id = "homozygSnp", description = FieldConstants.ROH_HOMOZYG_SNP_DESCRIPTION, defaultValue = "100")
    private Integer homozygSnp;

    @DataField(id = "homozygHet", description = FieldConstants.ROH_HOMOZYG_HET_DESCRIPTION, defaultValue = "unlimited")
    private Integer homozygHet;

    @DataField(id = "homozygDensity", description = FieldConstants.ROH_HOMOZYG_DENSITY_DESCRIPTION, defaultValue = "50")
    private String homozygDensity;

    @DataField(id = "homozygGap", description = FieldConstants.ROH_HOMOZYG_GAP_DESCRIPTION, defaultValue = "1000")
    private String homozygGap;

    @DataField(id = "outdir", description = FieldConstants.JOB_OUT_DIR_DESCRIPTION)
    private String outdir;

    public RohWrapperParams() {
    }

    public RohWrapperParams(String sampleId, String chromosome, String filter, Integer genotypeQuality, Boolean skipGenotypeQuality,
                            Integer homozygWindowSnp, Integer homozygWindowHet, Integer homozygWindowMissing, Float homozygWindowThreshold,
                            Integer homozygKb, Integer homozygSnp, Integer homozygHet, String homozygDensity, String homozygGap,
                            String outdir) {
        this.sampleId = sampleId;
        this.chromosome = chromosome;
        this.filter = filter;
        this.genotypeQuality = genotypeQuality;
        this.skipGenotypeQuality = skipGenotypeQuality;
        this.homozygWindowSnp = homozygWindowSnp;
        this.homozygWindowHet = homozygWindowHet;
        this.homozygWindowMissing = homozygWindowMissing;
        this.homozygWindowThreshold = homozygWindowThreshold;
        this.homozygKb = homozygKb;
        this.homozygSnp = homozygSnp;
        this.homozygHet = homozygHet;
        this.homozygDensity = homozygDensity;
        this.homozygGap = homozygGap;
        this.outdir = outdir;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("RohWrapperParams{");
        sb.append("sampleId='").append(sampleId).append('\'');
        sb.append(", chromosome='").append(chromosome).append('\'');
        sb.append(", filter='").append(filter).append('\'');
        sb.append(", genotypeQuality=").append(genotypeQuality);
        sb.append(", skipGenotypeQuality=").append(skipGenotypeQuality);
        sb.append(", homozygWindowSnp=").append(homozygWindowSnp);
        sb.append(", homozygWindowHet=").append(homozygWindowHet);
        sb.append(", homozygWindowMissing=").append(homozygWindowMissing);
        sb.append(", homozygWindowThreshold=").append(homozygWindowThreshold);
        sb.append(", homozygKb=").append(homozygKb);
        sb.append(", homozygSnp=").append(homozygSnp);
        sb.append(", homozygHet=").append(homozygHet);
        sb.append(", homozygDensity='").append(homozygDensity).append('\'');
        sb.append(", homozygGap='").append(homozygGap).append('\'');
        sb.append(", outdir='").append(outdir).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public String getSampleId() {
        return sampleId;
    }

    public RohWrapperParams setSampleId(String sampleId) {
        this.sampleId = sampleId;
        return this;
    }

    public String getChromosome() {
        return chromosome;
    }

    public RohWrapperParams setChromosome(String chromosome) {
        this.chromosome = chromosome;
        return this;
    }

    public String getFilter() {
        return filter;
    }

    public RohWrapperParams setFilter(String filter) {
        this.filter = filter;
        return this;
    }

    public Integer getGenotypeQuality() {
        return genotypeQuality;
    }

    public RohWrapperParams setGenotypeQuality(Integer genotypeQuality) {
        this.genotypeQuality = genotypeQuality;
        return this;
    }

    public Boolean getSkipGenotypeQuality() {
        return skipGenotypeQuality;
    }

    public RohWrapperParams setSkipGenotypeQuality(Boolean skipGenotypeQuality) {
        this.skipGenotypeQuality = skipGenotypeQuality;
        return this;
    }

    public Integer getHomozygWindowSnp() {
        return homozygWindowSnp;
    }

    public RohWrapperParams setHomozygWindowSnp(Integer homozygWindowSnp) {
        this.homozygWindowSnp = homozygWindowSnp;
        return this;
    }

    public Integer getHomozygWindowHet() {
        return homozygWindowHet;
    }

    public RohWrapperParams setHomozygWindowHet(Integer homozygWindowHet) {
        this.homozygWindowHet = homozygWindowHet;
        return this;
    }

    public Integer getHomozygWindowMissing() {
        return homozygWindowMissing;
    }

    public RohWrapperParams setHomozygWindowMissing(Integer homozygWindowMissing) {
        this.homozygWindowMissing = homozygWindowMissing;
        return this;
    }

    public Float getHomozygWindowThreshold() {
        return homozygWindowThreshold;
    }

    public RohWrapperParams setHomozygWindowThreshold(Float homozygWindowThreshold) {
        this.homozygWindowThreshold = homozygWindowThreshold;
        return this;
    }

    public Integer getHomozygKb() {
        return homozygKb;
    }

    public RohWrapperParams setHomozygKb(Integer homozygKb) {
        this.homozygKb = homozygKb;
        return this;
    }

    public Integer getHomozygSnp() {
        return homozygSnp;
    }

    public RohWrapperParams setHomozygSnp(Integer homozygSnp) {
        this.homozygSnp = homozygSnp;
        return this;
    }

    public Integer getHomozygHet() {
        return homozygHet;
    }

    public RohWrapperParams setHomozygHet(Integer homozygHet) {
        this.homozygHet = homozygHet;
        return this;
    }

    public String getHomozygDensity() {
        return homozygDensity;
    }

    public RohWrapperParams setHomozygDensity(String homozygDensity) {
        this.homozygDensity = homozygDensity;
        return this;
    }

    public String getHomozygGap() {
        return homozygGap;
    }

    public RohWrapperParams setHomozygGap(String homozygGap) {
        this.homozygGap = homozygGap;
        return this;
    }

    public String getOutdir() {
        return outdir;
    }

    public RohWrapperParams setOutdir(String outdir) {
        this.outdir = outdir;
        return this;
    }
}
