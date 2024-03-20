package org.opencb.opencga.analysis.wrappers.roh;

import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.utils.DockerUtils;
import org.opencb.opencga.analysis.wrappers.executors.DockerWrapperAnalysisExecutor;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.tools.annotations.ToolExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;

@ToolExecutor(id = RohWrapperAnalysisExecutor.ID,
        tool = RohWrapperAnalysis.ID,
        source = ToolExecutor.Source.STORAGE,
        framework = ToolExecutor.Framework.LOCAL)
public class RohWrapperAnalysisExecutor extends DockerWrapperAnalysisExecutor {

    public static final String ID = RohWrapperAnalysis.ID + "-local";

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

    private Logger logger = LoggerFactory.getLogger(this.getClass());

    @Override
    public void run() throws ToolException {
        Path opencgaHome = Paths.get(getExecutorParams().getString("opencgaHome"));

        Path symbolicLink = null;
        try {
            // Build command line to run R script via docker image
            String jobDir = "/jobdir";
            String scriptDir = "/scripts";

            // Input binding
            List<AbstractMap.SimpleEntry<String, String>> inputBindings = new ArrayList<>();
            inputBindings.add(new AbstractMap.SimpleEntry<>(opencgaHome.resolve("analysis/" + RohWrapperAnalysis.ID)
                    .toAbsolutePath().toString(), scriptDir));

            // Check if the vcf file is in the job dir
            if (!getOutDir().toAbsolutePath().toString().equals(vcfPath.getParent().toAbsolutePath().toString())) {
                // Create symbolic link
                symbolicLink = Files.createSymbolicLink(getOutDir().resolve(vcfPath.getFileName()), vcfPath);
                logger.info("The symbolic link to the vcf was created: {}", symbolicLink);

                // Add to the docker input bindings
                inputBindings.add(new AbstractMap.SimpleEntry<>(vcfPath.getParent().toAbsolutePath().toString(),
                        vcfPath.getParent().toAbsolutePath().toString()));

                // Finally, update the VCF path with the symbolic link to be used by the docker command line
                vcfPath = symbolicLink;
            }


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
            // TODO: manage --genotype-quality and --skip-genotype-quality
            cli.append(" --skip-genotype-quality ");
            if (getHomozygWindowSnp() != null) {
                cli.append(" --homozyg-window-snp ").append(getHomozygWindowSnp());
            }
            if (getHomozygWindowHet() != null) {
                cli.append(" --homozyg-window-het ").append(getHomozygWindowHet());
            }
            if (getHomozygWindowMissing() != null) {
                cli.append(" --homozyg-window-missing ").append(getHomozygWindowMissing());
            }
            if (getHomozygWindowThreshold() != null) {
                cli.append(" --homozyg-window-threshold ").append(getHomozygWindowThreshold());
            }
            if (getHomozygKb() != null) {
                cli.append(" --homozyg-kb ").append(getHomozygKb());
            }
            if (getHomozygWindowSnp() != null) {
                cli.append(" --homozyg-snp ").append(getHomozygWindowSnp());
            }
            if (getHomozygHet() != null) {
                cli.append(" --homozyg-het ").append(getHomozygHet());
            }
            if (getHomozygDensity() != null) {
                cli.append(" --homozyg-density ").append(getHomozygDensity());
            }
            if (getHomozygGap() != null) {
                cli.append(" --homozyg-gap ").append(getHomozygGap());
            }

            // Execute R script in docker
            DockerUtils.run(getDockerImageName() + ":" + getDockerImageVersion(), inputBindings, outputBinding, cli.toString(), null);
        } catch (Exception e) {
            throw new ToolException(e);
        } finally {
            if (symbolicLink != null) {
                try {
                    Files.delete(symbolicLink);
                } catch (IOException e) {
                    logger.warn("Could not delete the symbolic link {}", symbolicLink, e);
                }
            }
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
