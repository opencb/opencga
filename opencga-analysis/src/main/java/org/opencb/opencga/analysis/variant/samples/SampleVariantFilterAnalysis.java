package org.opencb.opencga.analysis.variant.samples;

import org.opencb.biodata.models.commons.Phenotype;
import org.opencb.biodata.models.feature.Genotype;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.tools.OpenCgaToolScopeStudy;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.core.annotations.Tool;
import org.opencb.opencga.core.api.variant.SampleVariantFilterParams;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.sample.Sample;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.storage.core.variant.adaptors.*;
import org.opencb.opencga.storage.core.variant.adaptors.iterators.VariantDBIterator;
import org.opencb.opencga.storage.core.variant.io.VariantWriterFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Collectors;

@Tool(id = SampleVariantFilterAnalysis.ID, description = SampleVariantFilterAnalysis.DESCRIPTION, resource = Enums.Resource.VARIANT)
public class SampleVariantFilterAnalysis extends OpenCgaToolScopeStudy {

    public static final String ID = "sample-variant-filter";
    public static final String DESCRIPTION = "Get samples given a set of variants";

    protected final Logger logger = LoggerFactory.getLogger(SampleVariantFilterAnalysis.class);
    private Query query;
    private List<GenotypeClass> genotypeClasses;
    private Set<String> genotypesSet;
    private File samplesOutputFile;
    private File variantsOutputFile;

    private final SampleVariantFilterParams toolParams = new SampleVariantFilterParams();
    private String studyFqn;

    private Set<String> variants;
    private Collection<String> samples;

    public SampleVariantFilterAnalysis setMaxVariants(int maxVariants) {
        this.toolParams.setMaxVariants(maxVariants);
        return this;
    }

    public SampleVariantFilterAnalysis setSamplesInAllVariants(boolean samplesInAllVariants) {
        this.toolParams.setSamplesInAllVariants(samplesInAllVariants);
        return this;
    }

    public SampleVariantFilterAnalysis setGenotypes(List<String> genotypes) {
        this.toolParams.setGenotypes(genotypes);
        return this;
    }

    @Override
    protected void check() throws Exception {
        super.check();
        toolParams.updateParams(params);
//            VariantStorageManager.getVariantQuery(params);
        query = toolParams.toQuery();
        studyFqn = getStudyFqn();

        if (toolParams.getMaxVariants() > 1000) {
            throw new ToolException("Number of max variants can not be larger than 1000");
        }

        List<String> genotypesList = new ArrayList<>(toolParams.getGenotypes()); // Ensure this.genotypes is untouched
        genotypeClasses = getGenotypeClasses(genotypesList); // This method may modify genotypesList
        genotypesSet = getGenotypesSet(genotypesList);

        samplesOutputFile = getOutDir().resolve("samples.tsv").toFile();
        variantsOutputFile = getOutDir().resolve("variants.vcf.gz").toFile();
    }

    @Override
    protected List<String> getSteps() {
        return Arrays.asList("filterVariants", "filterSamples", "exportVariants");
    }

    @Override
    protected void run() throws Exception {

        step("filterVariants", () -> {
            int maxVariants = toolParams.getMaxVariants();
            variants = new HashSet<>();
            Query query = new Query(this.query);
            query.remove(VariantQueryParam.SAMPLE.key());
//                    .append(VariantQueryParam.SAMPLE.key(), toolParams.getSample());
            try (VariantDBIterator iterator = variantStorageManager.iterator(query, new QueryOptions(VariantField.SUMMARY, true), token)) {
                while (iterator.hasNext()) {
                    Variant variant = iterator.next();
                    variants.add(variant.toString());
                    if (variants.size() > maxVariants) {
                        throw new ToolException("More than " + maxVariants + " variants found for query "
                                + VariantQueryUtils.printQuery(this.query));
                    }
                }
            }
            logger.info("Number of variants: " + variants.size());
            addAttribute("numberOfVariants", variants.size());
            if (variants.isEmpty()) {
                addWarning("Empty list of variants!");
            }
        });

        step("filterSamples", () -> {
            if (variants.isEmpty()) {
                samples = Collections.emptyList();
            } else if (toolParams.isSamplesInAllVariants()) {
                samples = getSamplesInAllVariants();
            } else {
                samples = getSamplesInAnyVariants();
            }
            addAttribute("numberOfSamples", samples.size());
            if (samples.isEmpty()) {
                addWarning("Empty list of samples!");
            }

            printSamplesFile();
        });

        step("exportVariants", () -> {
            if (variants.isEmpty() || samples.isEmpty()) {
                logger.info("Nothing to export");
            } else {
                Query query = new Query()
                        .append(VariantQueryParam.ID.key(), variants)
                        .append(VariantQueryParam.INCLUDE_STUDY.key(), studyFqn)
                        .append(VariantQueryParam.INCLUDE_SAMPLE.key(), samples)
                        .append(VariantQueryParam.UNKNOWN_GENOTYPE.key(), "./.");
                variantStorageManager.exportData(variantsOutputFile.getAbsolutePath(), VariantWriterFactory.VariantOutputFormat.VCF_GZ, null,
                        query, new QueryOptions(), token);
            }
        });
    }

    private void printSamplesFile() throws IOException, CatalogException {
        final String type;
        if (toolParams.isSamplesInAllVariants()) {
            type = "ALL";
        } else {
            type = "ANY";
        }
        try (PrintStream out = new PrintStream(new FileOutputStream(samplesOutputFile))) {
            out.println("## Samples in " + type + " variants with genotypes " + toolParams.getGenotypes());
            out.println("## Number of variants=" + variants.size());
            out.println("#SAMPLE\tINDIVIDUAL\tPHENOTYPES");
            for (String sampleId : samples) {
                Sample sample = catalogManager.getSampleManager().get(studyFqn, sampleId, null, token).first();
                out.println(sampleId + "\t"
                        + sample.getIndividualId() + "\t"
                        + sample.getPhenotypes().stream().map(Phenotype::getId).collect(Collectors.joining(",")));
            }
        }
    }

    public Set<String> getSamplesInAnyVariants() throws Exception {
        Set<String> samples = new HashSet<>();
        Set<String> variants = new HashSet<>();

        iterate(variant -> {
        }, (variant, sample, gt) -> {
            if (isValidGenotype(genotypesSet, genotypeClasses, gt)) {
                samples.add(sample);
                variants.add(variant.toString());
            }
            return true;
        });
        this.variants = variants;

        return samples;
    }

    public Collection<String> getSamplesInAllVariants() throws Exception {
        Set<String> samples = new HashSet<>();

        iterate(variant -> getSamplesSet(variant, samples),
                (variant, sample, gt) -> {
                    // Remove if not a valid genotype
                    if (!isValidGenotype(genotypesSet, genotypeClasses, gt)) {
                        if (samples.remove(sample)) {
                            logger.debug("variant: {}, sample: {}, gt: {}", variant, sample, gt);
                            if (sample.isEmpty()) {
                                return false;
                            }
                        }
                    }
                    return true;
                });
        return samples;
    }

    @FunctionalInterface
    interface GenotypeWalker {

        boolean accept(Variant variant, String sample, String gt);
    }

    protected void iterate(Consumer<Variant> init, GenotypeWalker walker) throws Exception {
        int maxVariants = toolParams.getMaxVariants();
        Query query = new Query()
                .append(VariantQueryParam.ID.key(), variants)
                .append(VariantQueryParam.INCLUDE_SAMPLE.key(), toolParams.getSample())
                .append(VariantQueryParam.INCLUDE_GENOTYPE.key(), true);
        QueryOptions options = new QueryOptions()
                .append(QueryOptions.INCLUDE, Collections.singletonList(VariantField.STUDIES_SAMPLES_DATA))
                .append(QueryOptions.LIMIT, variants.size());

        try (VariantDBIterator iterator = variantStorageManager.iterator(query, options, token)) {
            if (!iterator.hasNext()) {
                return;
            }
            int numVariants = 0;
            while (iterator.hasNext()) {
                Variant variant = iterator.next();
                if (numVariants == 0) {
                    init.accept(variant);
                } else if (numVariants == maxVariants) {
                    throw new VariantQueryException("Error! Limit reached with more than " + maxVariants + " variants!");
                }
                numVariants++;
                StudyEntry studyEntry = variant.getStudies().get(0);
                Integer gtIdx = studyEntry.getFormatPositions().get("GT");
                if (gtIdx == null || gtIdx < 0) {
                    throw new VariantQueryException("Missing GT at variant " + variant);
                }

                int sampleIdx = 0;
                for (String sample : studyEntry.getOrderedSamplesName()) {
                    String gt = studyEntry.getSamplesData().get(sampleIdx).get(gtIdx);
                    if (!walker.accept(variant, sample, gt)) {
                        break;
                    }
                    sampleIdx++;
                }
            }
        }
    }

    private Set<String> getSamplesSet(Variant variant, Set<String> samples) {
        if (variant.getStudies().size() != 1) {
            throw new VariantQueryException("Unable to process with " + variant.getStudies().size() + " studies.");
        }
        samples.addAll(variant.getStudies().get(0).getSamplesName());
        if (samples.isEmpty()) {
            throw new VariantQueryException("Unable to get samples!");
        }
        return samples;
    }

    private boolean isValidGenotype(Set<String> genotypesSet, List<GenotypeClass> genotypeClasses, String gt) {
        return genotypesSet.contains(gt) || !genotypeClasses.isEmpty() && genotypeClasses.stream().anyMatch(gc -> gc.test(gt));
    }

    private List<GenotypeClass> getGenotypeClasses(Collection<String> genotypes) {
        List<GenotypeClass> genotypeClasses = new ArrayList<>();
        Iterator<String> iterator = genotypes.iterator();
        while (iterator.hasNext()) {
            String genotype = iterator.next();
            GenotypeClass genotypeClass = GenotypeClass.from(genotype);
            if (genotypeClass != null) {
                genotypeClasses.add(genotypeClass);
                iterator.remove();
            }
        }
        return genotypeClasses;
    }

    private HashSet<String> getGenotypesSet(List<String> genotypes) {
        HashSet<String> set = new HashSet<>(genotypes);
        for (String gt : genotypes) {
            Genotype genotype = new Genotype(gt);
            if (!genotype.isPhased()) {
                set.addAll(GenotypeClass.getPhasedGenotypes(genotype));
            }
        }
        return set;
    }
}
