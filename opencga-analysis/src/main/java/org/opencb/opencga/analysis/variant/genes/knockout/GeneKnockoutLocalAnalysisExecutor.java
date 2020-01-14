package org.opencb.opencga.analysis.variant.genes.knockout;

import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.opencb.biodata.models.core.Region;
import org.opencb.biodata.models.feature.Genotype;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.AlternateCoordinate;
import org.opencb.biodata.models.variant.avro.ConsequenceType;
import org.opencb.biodata.models.variant.avro.VariantType;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.variant.genes.knockout.result.GeneKnockoutByGene;
import org.opencb.opencga.analysis.variant.genes.knockout.result.GeneKnockoutBySample;
import org.opencb.opencga.analysis.variant.genes.knockout.result.GeneKnockoutBySample.GeneKnockout;
import org.opencb.opencga.analysis.variant.genes.knockout.result.TranscriptKnockout;
import org.opencb.opencga.analysis.variant.genes.knockout.result.VariantKnockout;
import org.opencb.opencga.analysis.variant.manager.VariantCatalogQueryUtils;
import org.opencb.opencga.analysis.variant.manager.VariantStorageManager;
import org.opencb.opencga.analysis.variant.manager.VariantStorageToolExecutor;
import org.opencb.opencga.core.annotations.ToolExecutor;
import org.opencb.opencga.core.common.JacksonUtils;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.storage.core.metadata.models.Trio;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.core.variant.adaptors.iterators.VariantDBIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.function.Predicate;

import static org.opencb.cellbase.core.variant.annotation.VariantAnnotationUtils.PROTEIN_CODING;
import static org.opencb.opencga.analysis.variant.manager.VariantCatalogQueryUtils.COMPOUND_HETEROZYGOUS;
import static org.opencb.opencga.storage.core.variant.adaptors.VariantQueryUtils.ALL;
import static org.opencb.opencga.storage.core.variant.adaptors.VariantQueryUtils.IS;

@ToolExecutor(id = "opencga-local",
        tool = GeneKnockoutAnalysis.ID,
        source = ToolExecutor.Source.STORAGE,
        framework = ToolExecutor.Framework.LOCAL)
public class GeneKnockoutLocalAnalysisExecutor extends GeneKnockoutAnalysisExecutor implements VariantStorageToolExecutor {

    private Logger logger = LoggerFactory.getLogger(this.getClass());
    private VariantStorageManager variantStorageManager;

    @Override
    protected void run() throws Exception {
        variantStorageManager = getVariantStorageManager();
        List<String> biotype = StringUtils.isEmpty(getBiotype()) ? null : Arrays.asList(getBiotype().split(","));
        if (biotype != null && biotype.contains(PROTEIN_CODING)) {
            biotype = new ArrayList<>(biotype);
            biotype.remove(PROTEIN_CODING);
        }

        Query baseQuery = new Query()
                .append(VariantQueryParam.STUDY.key(), getStudy())
                .append(VariantQueryParam.FILTER.key(), getFilter())
                .append(VariantQueryParam.QUAL.key(), getQual());
        for (String sample : getSamples()) {
            StopWatch stopWatch = StopWatch.createStarted();
            logger.info("Processing sample {}", sample);
            Map<String, GeneKnockout> knockoutGenes = new LinkedHashMap<>();
            Trio trio = getTrios().get(sample);

            // Protein coding genes (if any)
            if (getProteinCodingGenes().size() == 1 && getProteinCodingGenes().iterator().next().equals(ALL)) {
                // All protein coding genes
                Query query = new Query(baseQuery)
                        .append(VariantQueryParam.ANNOT_BIOTYPE.key(), PROTEIN_CODING)
                        .append(VariantQueryParam.INCLUDE_SAMPLE.key(), sample)
                        .append(VariantQueryParam.ANNOT_CONSEQUENCE_TYPE.key(), getCt());
                knockouts(query, sample, trio, knockoutGenes,
                        getCts()::contains,
                        b -> b.equals(PROTEIN_CODING),
                        g -> true);
            } else if (!getProteinCodingGenes().isEmpty()) {
                // Set of protein coding genes
                Query query = new Query(baseQuery)
                        .append(VariantQueryParam.GENE.key(), getProteinCodingGenes())
                        .append(VariantQueryParam.ANNOT_BIOTYPE.key(), PROTEIN_CODING)
                        .append(VariantQueryParam.ANNOT_CONSEQUENCE_TYPE.key(), getCt())
                        .append(VariantQueryParam.INCLUDE_SAMPLE.key(), sample);
                knockouts(query, sample, trio, knockoutGenes,
                        getCts()::contains,
                        b -> b.equals(PROTEIN_CODING),
                        getProteinCodingGenes()::contains);
            }

            // Other genes (if any)
            if (!getOtherGenes().isEmpty()) {
                Query query = new Query(baseQuery)
                        .append(VariantQueryParam.ANNOT_BIOTYPE.key(), biotype)
                        .append(VariantQueryParam.GENE.key(), getOtherGenes())
                        .append(VariantQueryParam.INCLUDE_SAMPLE.key(), sample);
                knockouts(query, sample, trio, knockoutGenes,
                        ct -> true,  // Accept any CT
                        biotype == null ? (b -> !b.equals(PROTEIN_CODING)) : new HashSet<>(biotype)::contains,
                        getOtherGenes()::contains);
            }

            if (knockoutGenes.isEmpty()) {
                logger.info("No results for sample {}", sample);
            } else {
                GeneKnockoutBySample bySample = buildGeneKnockoutBySample(sample, knockoutGenes);
                printSampleFile(bySample);
            }
            logger.info("Sample {} processed in {}", sample, TimeUtils.durationToString(stopWatch));
            logger.info("-----------------------------------------------------------");
        }

        transposeSampleToGeneOutputFiles();
    }

    private void transposeSampleToGeneOutputFiles() throws IOException {
        Map<String, GeneKnockoutByGene> byGeneMap = new HashMap<>();
        for (String sample : getSamples()) {
            Path fileName = getSampleFileName(sample);
            ObjectReader reader = JacksonUtils.getDefaultObjectMapper().readerFor(GeneKnockoutBySample.class);
            if (Files.exists(fileName)) {
                GeneKnockoutBySample bySample = reader.readValue(fileName.toFile());
                for (GeneKnockoutBySample.GeneKnockout gene : bySample.getGenes()) {
                    GeneKnockoutByGene byGene = byGeneMap.computeIfAbsent(gene.getName(),
                            id -> new GeneKnockoutByGene().setId(gene.getId()).setName(gene.getName()).setSamples(new LinkedList<>()));

                    GeneKnockoutByGene.KnockoutSample knockoutSample = new GeneKnockoutByGene.KnockoutSample()
                            .setId(bySample.getSample().getId())
                            .setTranscripts(gene.getTranscripts());
                    byGene.getSamples().add(knockoutSample);
                }
            }
        }

        ObjectWriter writer = JacksonUtils.getDefaultObjectMapper().writerFor(GeneKnockoutByGene.class).withDefaultPrettyPrinter();
        for (Map.Entry<String, GeneKnockoutByGene> entry : byGeneMap.entrySet()) {
            String gene = entry.getKey();
            writer.writeValue(getGeneFileName(gene).toFile(), entry.getValue());
        }
    }

    private void knockouts(Query query, String sample, Trio trio, Map<String, GeneKnockout> knockoutGenes,
                           Predicate<String> ctFilter,
                           Predicate<String> biotypeFilter,
                           Predicate<String> geneFilter) throws Exception {
        homAltKnockouts(sample, knockoutGenes, query, ctFilter, biotypeFilter, geneFilter);

        multiAllelicKnockouts(sample, knockoutGenes, query, ctFilter, biotypeFilter, geneFilter);

        if (trio != null) {
            compHetKnockouts(sample, trio, knockoutGenes, query, ctFilter, biotypeFilter, geneFilter);
        }

        structuralKnockouts(sample, knockoutGenes, query, ctFilter, biotypeFilter, geneFilter);
    }

    protected void homAltKnockouts(String sample,
                                   Map<String, GeneKnockout> knockoutGenes,
                                   Query query,
                                   Predicate<String> ctFilter,
                                   Predicate<String> biotypeFilter,
                                   Predicate<String> geneFilter)
            throws Exception {
        query = new Query(query)
                .append(VariantQueryParam.GENOTYPE.key(), sample + IS + "1/1");

        int numVariants = iterate(query, v -> {
            for (ConsequenceType consequenceType : v.getAnnotation().getConsequenceTypes()) {
                if (validCt(consequenceType, ctFilter, biotypeFilter, geneFilter)) {
                    addGene(v.toString(), v.getStudies().get(0).getSamplesData().get(0).get(0),
                            consequenceType, knockoutGenes, VariantKnockout.KnockoutType.HOM_ALT);
                }
            }
        });
        logger.debug("Read " + numVariants + " HOM_ALT variants from sample " + sample);
    }

    protected void multiAllelicKnockouts(String sample,
                                         Map<String, GeneKnockout> knockoutGenes,
                                         Query query,
                                         Predicate<String> ctFilter,
                                         Predicate<String> biotypeFilter,
                                         Predicate<String> geneFilter)
            throws Exception {

        query = new Query(query)
                .append(VariantQueryParam.INCLUDE_GENOTYPE.key(), true)
                .append(VariantQueryParam.GENOTYPE.key(), sample + IS + "1/2");

        Map<String, VariantKnockout> variants = new HashMap<>();
        iterate(query, variant -> {
            Variant secVar = getSecondaryVariant(variant);
            VariantKnockout variantKnockout = new VariantKnockout(
                    variant.toString(),
                    variant.getStudies().get(0).getSamplesData().get(0).get(0), null, null,
                    VariantKnockout.KnockoutType.MULTI_ALLELIC, null
            );
            if (variants.put(variant.toString(), variantKnockout) == null) {
                // Variant not seen
                if (variant.overlapWith(secVar, true)) {
                    // Add overlapping variant.
                    // If the overlapping variant is ever seen (so it also matches the filter criteria),
                    // the gene will be selected as knockout.
                    variants.put(secVar.toString(), new VariantKnockout());
                }
            } else {
                // The variant was already seen. i.e. there was a variant with this variant as secondary alternate
                for (ConsequenceType consequenceType : variant.getAnnotation().getConsequenceTypes()) {
                    if (validCt(consequenceType, ctFilter, biotypeFilter, geneFilter)) {
                        String gt = variant.getStudies().get(0).getSamplesData().get(0).get(0);
                        addGene(variant.toString(), gt, consequenceType, knockoutGenes,
                                VariantKnockout.KnockoutType.MULTI_ALLELIC);
                        String secVarGt = variants.get(secVar.toString()).getGenotype();
                        addGene(secVar.toString(), secVarGt, consequenceType, knockoutGenes,
                                VariantKnockout.KnockoutType.MULTI_ALLELIC);
                    }
                }
            }
        });
    }

    private Variant getSecondaryVariant(Variant variant) {
        Genotype gt = new Genotype(variant.getStudies().get(0).getSamplesData().get(0).get(0));
        Variant secVar = null;
        for (int allelesIdx : gt.getAllelesIdx()) {
            if (allelesIdx > 1) {
                AlternateCoordinate alt = variant.getStudies().get(0).getSecondaryAlternates().get(allelesIdx - 2);
                secVar = new Variant(
                        alt.getChromosome() == null ? variant.getChromosome() : alt.getChromosome(),
                        alt.getStart() == null ? variant.getStart() : alt.getStart(),
                        alt.getEnd() == null ? variant.getEnd() : alt.getEnd(),
                        alt.getReference() == null ? variant.getReference() : alt.getReference(),
                        alt.getAlternate() == null ? variant.getAlternate() : alt.getAlternate());
            }
        }
        return secVar;
    }

    protected void compHetKnockouts(String sample, Trio family,
                                    Map<String, GeneKnockout> knockoutGenes,
                                    Query query,
                                    Predicate<String> ctFilter,
                                    Predicate<String> biotypeFilter,
                                    Predicate<String> geneFilter)
            throws Exception {
        query = new Query(query)
                .append(VariantCatalogQueryUtils.FAMILY.key(), family.getId())
                .append(VariantCatalogQueryUtils.FAMILY_DISORDER.key(), getDisorder())
                .append(VariantCatalogQueryUtils.FAMILY_PROBAND.key(), sample)
                .append(VariantCatalogQueryUtils.FAMILY_SEGREGATION.key(), COMPOUND_HETEROZYGOUS)
                .append(VariantQueryParam.INCLUDE_SAMPLE.key(), family.toList());

        int numVariants = iterate(query, v -> {
            for (ConsequenceType consequenceType : v.getAnnotation().getConsequenceTypes()) {
                if (validCt(consequenceType, ctFilter, biotypeFilter, geneFilter)) {
                    addGene(v.toString(), v.getStudies().get(0).getSamplesData().get(0).get(0),
                            consequenceType, knockoutGenes, VariantKnockout.KnockoutType.COMP_HET);
                }
            }
        });
        logger.debug("Read " + numVariants + " COMP_HET variants from sample " + sample);
    }

    protected void structuralKnockouts(String sample,
                                       Map<String, GeneKnockout> knockoutGenes,
                                       Query baseQuery,
                                       Predicate<String> ctFilter,
                                       Predicate<String> biotypeFilter,
                                       Predicate<String> geneFilter) throws Exception {
        Query query = new Query(baseQuery)
                .append(VariantQueryParam.SAMPLE.key(), sample)
                .append(VariantQueryParam.TYPE.key(), VariantType.DELETION);
//                .append(VariantQueryParam.ANNOT_CONSEQUENCE_TYPE.key(), LOF + "," + VariantAnnotationUtils.FEATURE_TRUNCATION);
//        Set<String> cts = new HashSet<>(LOF_SET);
//        cts.add(VariantAnnotationUtils.FEATURE_TRUNCATION);

        iterate(query, svVariant -> {
            Set<String> transcripts = new HashSet<>(svVariant.getAnnotation().getConsequenceTypes().size());
            for (ConsequenceType consequenceType : svVariant.getAnnotation().getConsequenceTypes()) {
                if (validCt(consequenceType, ctFilter, biotypeFilter, geneFilter)) {
                    transcripts.add(consequenceType.getEnsemblTranscriptId());
                }
            }
            Query thisSvQuery = new Query(baseQuery)
                    .append(VariantQueryParam.SAMPLE.key(), sample)
                    .append(VariantQueryParam.REGION.key(), new Region(svVariant.getChromosome(), svVariant.getStart(), svVariant.getEnd()));

            iterate(thisSvQuery, variant -> {
                if (variant.sameGenomicVariant(svVariant)) {
                    return;
                }
                for (ConsequenceType consequenceType : variant.getAnnotation().getConsequenceTypes()) {
                    if (validCt(consequenceType, ctFilter, biotypeFilter, geneFilter)) {
                        if (transcripts.contains(consequenceType.getEnsemblTranscriptId())) {
                            addGene(variant.toString(), svVariant.getStudies().get(0).getSamplesData().get(0).get(0), consequenceType, knockoutGenes,
                                    VariantKnockout.KnockoutType.DELETION_OVERLAP);
                            addGene(svVariant.toString(), variant.getStudies().get(0).getSamplesData().get(0).get(0), consequenceType, knockoutGenes,
                                    VariantKnockout.KnockoutType.DELETION_OVERLAP);
                        }
                    }
                }
            });
        });
    }

    private interface VariantConsumer {
        void accept(Variant v) throws Exception;
    }

    private int iterate(Query query, VariantConsumer c)
            throws Exception {
        int numVariants;
        try (VariantDBIterator iterator = variantStorageManager.iterator(query, new QueryOptions(), getToken())) {
            while (iterator.hasNext()) {
                c.accept(iterator.next());
            }
            numVariants = iterator.getCount();
        }
        return numVariants;
    }

    private void addGene(String variant, String gt, ConsequenceType consequenceType,
                         Map<String, GeneKnockout> knockoutGenes,
                         VariantKnockout.KnockoutType knockoutType) {
        GeneKnockout gene = knockoutGenes.computeIfAbsent(consequenceType.getGeneName(), GeneKnockout::new);
        gene.setId(consequenceType.getEnsemblGeneId());
//        gene.setBiotype(consequenceType.getBiotype());
        if (StringUtils.isNotEmpty(consequenceType.getEnsemblTranscriptId())) {
            TranscriptKnockout t = gene.addTranscript(consequenceType.getEnsemblTranscriptId());
            t.setBiotype(consequenceType.getBiotype());
            t.addVariant(new VariantKnockout(variant, gt, null, null, knockoutType, consequenceType.getSequenceOntologyTerms()));
        }
    }

    private boolean validCt(ConsequenceType consequenceType,
                            Predicate<String> ctFilter,
                            Predicate<String> biotypeFilter,
                            Predicate<String> geneFilter) {
        if (StringUtils.isEmpty(consequenceType.getGeneName())) {
            return false;
        }
        if (StringUtils.isEmpty(consequenceType.getEnsemblTranscriptId())) {
            return false;
        }
        if (!geneFilter.test(consequenceType.getGeneName())) {
            return false;
        }
        if (!biotypeFilter.test(consequenceType.getBiotype())) {
            return false;
        }
        if (consequenceType.getSequenceOntologyTerms().stream().noneMatch(so -> ctFilter.test(so.getName()))) {
            return false;
        }
        return true;
    }

}
