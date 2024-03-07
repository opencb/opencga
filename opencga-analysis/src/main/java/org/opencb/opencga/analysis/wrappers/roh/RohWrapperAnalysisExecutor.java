package org.opencb.opencga.analysis.wrappers.roh;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.opencb.commons.annotations.DataField;
import org.opencb.commons.utils.DockerUtils;
import org.opencb.opencga.analysis.variant.mutationalSignature.MutationalSignatureAnalysis;
import org.opencb.opencga.analysis.variant.mutationalSignature.MutationalSignatureLocalAnalysisExecutor;
import org.opencb.opencga.analysis.wrappers.executors.DockerWrapperAnalysisExecutor;
import org.opencb.opencga.analysis.wrappers.samtools.SamtoolsWrapperAnalysis;
import org.opencb.opencga.core.api.FieldConstants;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.tools.annotations.ToolExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

@ToolExecutor(id = RohWrapperAnalysisExecutor.ID,
        tool = SamtoolsWrapperAnalysis.ID,
        source = ToolExecutor.Source.STORAGE,
        framework = ToolExecutor.Framework.LOCAL)
public class RohWrapperAnalysisExecutor extends DockerWrapperAnalysisExecutor {

    public final static String ID = RohWrapperAnalysis.ID + "-local";

    private String study;
    private Path vcfPath;
    private String sampleId;
    private String chromosome;
    private String filter;
    private Integer genotypeQuality;
    private Boolean skipGenotypeQuality;
    private Integer homozygWindowSnp;
    private Integer homozygWindowHet;
    private Integer homozygWindowMissing;
    private Float homozygWindowThreshold;
    private Integer homozygKb;
    private Integer homozygSnp;
    private Integer homozygHet;
    private String homozygDensity;
    private String homozygGap;

    private Path opencgaHome;

    private Logger logger = LoggerFactory.getLogger(this.getClass());

    @Override
    public void run() throws ToolException {
        opencgaHome = Paths.get(getExecutorParams().getString("opencgaHome"));

        try {
            // Build command line to run R script via docker image

            String jobDir = "/jobdir";
            String scriptDir = "/scripts";

            // Input binding
            List<AbstractMap.SimpleEntry<String, String>> inputBindings = new ArrayList<>();
            inputBindings.add(new AbstractMap.SimpleEntry<>(opencgaHome.resolve("analysis/" + RohWrapperAnalysis.ID)
                    .toAbsolutePath().toString(), scriptDir));

            // Output binding
            AbstractMap.SimpleEntry<String, String> outputBinding = new AbstractMap.SimpleEntry<>(getOutDir().toAbsolutePath().toString(),
                    jobDir);

            StringBuilder cli = new StringBuilder();

            cli.append(scriptDir).append("/singles_roh_analysis.sh")
                    .append(" --input-vcf-path ").append("/jobdir/").append(vcfPath.getFileName())
                    .append(" --python-roh-script-path ").append(scriptDir)
                    .append(" --sample-name ").append(getSampleId())
                    .append(" --chromosome ").append(getChromosome())
                    .append(" --output-folder-dir ").append(jobDir);

            if (StringUtils.isNotEmpty(getFilter())) {
                cli.append("--filter ").append(getFilter());
            }
//            if (getSkipGenotypeQuality() != null && !getSkipGenotypeQuality()) {
//            echo "--genotype-quality               INTEGER     GQ (VCF genotype quality annotation field) threshold to filter in variants in the ROH analysis. Default: 40 (GQ>40)."
//            echo "--skip-genotype-quality                      Flag to not use the GQ (VCF genotype quality annotation field) to filter in variants in the ROH analysis. Default: false"
            if (getHomozygWindowSnp() != null) {
                cli.append("--homozyg-window-snp ").append(getHomozygWindowSnp());
            }
            if (getHomozygWindowHet() != null) {
                cli.append("--homozyg-window-het ").append(getHomozygWindowHet());
            }
            if (getHomozygWindowMissing() != null) {
                cli.append("--homozyg-window-missing ").append(getHomozygWindowMissing());
            }
            if (getHomozygWindowThreshold() != null) {
                cli.append("--homozyg-window-threshold ").append(getHomozygWindowThreshold());
            }
            if (getHomozygKb() != null) {
                cli.append("--homozyg-kb ").append(getHomozygKb());
            }
            if (getHomozygWindowSnp() != null) {
                cli.append("--homozyg-snp ").append(getHomozygWindowSnp());
            }
            if (getHomozygHet() != null) {
                cli.append("--homozyg-het ").append(getHomozygHet());
            }
            if (getHomozygDensity() != null) {
                cli.append("--homozyg-density ").append(getHomozygDensity());
            }
            if (getHomozygGap() != null) {
                cli.append("--homozyg-gap ").append(getHomozygGap());
            }

            // Execute R script in docker
            DockerUtils.run(getDockerImageName(), inputBindings, outputBinding, cli.toString(), null);
        } catch (Exception e) {
            throw new ToolException(e);
        }
    }

    public String getStudy() {
        return study;
    }

    public RohWrapperAnalysisExecutor setStudy(String study) {
        this.study = study;
        return this;
    }

    public Path getVcfPath() {
        return vcfPath;
    }

    public RohWrapperAnalysisExecutor setVcfPath(Path vcfPath) {
        this.vcfPath = vcfPath;
        return this;
    }

    public String getSampleId() {
        return sampleId;
    }

    public RohWrapperAnalysisExecutor setSampleId(String sampleId) {
        this.sampleId = sampleId;
        return this;
    }

    public String getChromosome() {
        return chromosome;
    }

    public RohWrapperAnalysisExecutor setChromosome(String chromosome) {
        this.chromosome = chromosome;
        return this;
    }

    public String getFilter() {
        return filter;
    }

    public RohWrapperAnalysisExecutor setFilter(String filter) {
        this.filter = filter;
        return this;
    }

    public Integer getGenotypeQuality() {
        return genotypeQuality;
    }

    public RohWrapperAnalysisExecutor setGenotypeQuality(Integer genotypeQuality) {
        this.genotypeQuality = genotypeQuality;
        return this;
    }

    public Boolean getSkipGenotypeQuality() {
        return skipGenotypeQuality;
    }

    public RohWrapperAnalysisExecutor setSkipGenotypeQuality(Boolean skipGenotypeQuality) {
        this.skipGenotypeQuality = skipGenotypeQuality;
        return this;
    }

    public Integer getHomozygWindowSnp() {
        return homozygWindowSnp;
    }

    public RohWrapperAnalysisExecutor setHomozygWindowSnp(Integer homozygWindowSnp) {
        this.homozygWindowSnp = homozygWindowSnp;
        return this;
    }

    public Integer getHomozygWindowHet() {
        return homozygWindowHet;
    }

    public RohWrapperAnalysisExecutor setHomozygWindowHet(Integer homozygWindowHet) {
        this.homozygWindowHet = homozygWindowHet;
        return this;
    }

    public Integer getHomozygWindowMissing() {
        return homozygWindowMissing;
    }

    public RohWrapperAnalysisExecutor setHomozygWindowMissing(Integer homozygWindowMissing) {
        this.homozygWindowMissing = homozygWindowMissing;
        return this;
    }

    public Float getHomozygWindowThreshold() {
        return homozygWindowThreshold;
    }

    public RohWrapperAnalysisExecutor setHomozygWindowThreshold(Float homozygWindowThreshold) {
        this.homozygWindowThreshold = homozygWindowThreshold;
        return this;
    }

    public Integer getHomozygKb() {
        return homozygKb;
    }

    public RohWrapperAnalysisExecutor setHomozygKb(Integer homozygKb) {
        this.homozygKb = homozygKb;
        return this;
    }

    public Integer getHomozygSnp() {
        return homozygSnp;
    }

    public RohWrapperAnalysisExecutor setHomozygSnp(Integer homozygSnp) {
        this.homozygSnp = homozygSnp;
        return this;
    }

    public Integer getHomozygHet() {
        return homozygHet;
    }

    public RohWrapperAnalysisExecutor setHomozygHet(Integer homozygHet) {
        this.homozygHet = homozygHet;
        return this;
    }

    public String getHomozygDensity() {
        return homozygDensity;
    }

    public RohWrapperAnalysisExecutor setHomozygDensity(String homozygDensity) {
        this.homozygDensity = homozygDensity;
        return this;
    }

    public String getHomozygGap() {
        return homozygGap;
    }

    public RohWrapperAnalysisExecutor setHomozygGap(String homozygGap) {
        this.homozygGap = homozygGap;
        return this;
    }
}
