package org.opencb.opencga.analysis.variant.genes.knockout;

import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.models.core.Region;
import org.opencb.biodata.models.feature.Genotype;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.AlternateCoordinate;
import org.opencb.biodata.models.variant.avro.ConsequenceType;
import org.opencb.biodata.models.variant.avro.VariantType;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.variant.genes.knockout.result.GeneKnockoutBySample;
import org.opencb.opencga.analysis.variant.genes.knockout.result.GeneKnockoutBySample.GeneKnockout;
import org.opencb.opencga.analysis.variant.manager.VariantCatalogQueryUtils;
import org.opencb.opencga.analysis.variant.manager.VariantStorageManager;
import org.opencb.opencga.analysis.variant.manager.VariantStorageToolExecutor;
import org.opencb.opencga.core.annotations.ToolExecutor;
import org.opencb.opencga.storage.core.metadata.models.Trio;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.core.variant.adaptors.iterators.VariantDBIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
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

        Query baseQuery = new Query()
                .append(VariantQueryParam.STUDY.key(), getStudy())
                .append(VariantQueryParam.FILTER.key(), getFilter())
                .append(VariantQueryParam.QUAL.key(), getQual());
        for (String sample : getSamples()) {
            Map<String, GeneKnockout> knockoutGenes = new LinkedHashMap<>();
            Trio trio = getTrios().get(sample);

            // Protein coding genes (if any)
            if (getProteinCodingGenes().size() == 1 && getProteinCodingGenes().iterator().next().equals(ALL)) {
                // All protein coding genes
                Query query = new Query(baseQuery)
                        .append(VariantQueryParam.ANNOT_BIOTYPE.key(), PROTEIN_CODING)
                        .append(VariantQueryParam.ANNOT_CONSEQUENCE_TYPE.key(), getCt());
                knockouts(query, sample, trio, knockoutGenes, b -> b.equals(PROTEIN_CODING), g -> true);
            } else if (!getProteinCodingGenes().isEmpty()) {
                // Set of protein coding genes
                Query query = new Query(baseQuery)
                        .append(VariantQueryParam.GENE.key(), getProteinCodingGenes())
                        .append(VariantQueryParam.ANNOT_BIOTYPE.key(), PROTEIN_CODING)
                        .append(VariantQueryParam.ANNOT_CONSEQUENCE_TYPE.key(), getCt());
                knockouts(query, sample, trio, knockoutGenes, b -> b.equals(PROTEIN_CODING), getProteinCodingGenes()::contains);
            }

            // Other genes (if any)
            if (!getOtherGenes().isEmpty()) {
                Query query = new Query(baseQuery)
                        .append(VariantQueryParam.GENE.key(), getOtherGenes());
                knockouts(query, sample, trio, knockoutGenes, b -> !b.equals(PROTEIN_CODING), getOtherGenes()::contains);
            }

            printSampleFile(sample, knockoutGenes, trio);
        }
    }

    private void knockouts(Query query, String sample, Trio trio, Map<String, GeneKnockout> knockoutGenes,
                           Predicate<String> validBiotype,
                           Predicate<String> validGene) throws Exception {
        homAltKnockouts(sample, knockoutGenes, query, validBiotype, validGene);

        multiAllelicKnockouts(sample, knockoutGenes, query, validBiotype, validGene);

        if (trio != null) {
            compHetKnockouts(sample, trio, knockoutGenes, query, validBiotype, validGene);
        }

        structuralKnockouts(sample, knockoutGenes, query, validBiotype, validGene);
    }

    protected void homAltKnockouts(String sample,
                                   Map<String, GeneKnockout> knockoutGenes,
                                   Query query,
                                   Predicate<String> validBiotype,
                                   Predicate<String> validGene)
            throws Exception {
        query = new Query(query)
                .append(VariantQueryParam.GENOTYPE.key(), sample + IS + "1/1");

        int numVariants = iterate(query, v -> {
            for (ConsequenceType consequenceType : v.getAnnotation().getConsequenceTypes()) {
                if (validCt(consequenceType)
                        && validBiotype.test(consequenceType.getBiotype())
                        && validGene.test(consequenceType.getGeneName())) {
                    addGene(v.toString(), consequenceType, knockoutGenes, GeneKnockoutBySample.TranscriptKnockout::addHomAlt);
                }
            }
        });
        logger.debug("Read " + numVariants + " HOM_ALT variants from sample " + sample);
    }

    protected void multiAllelicKnockouts(String sample,
                                         Map<String, GeneKnockout> knockoutGenes,
                                         Query query,
                                         Predicate<String> validBiotype,
                                         Predicate<String> validGene)
            throws Exception {

        query = new Query(query)
                .append(VariantQueryParam.INCLUDE_GENOTYPE.key(), true)
                .append(VariantQueryParam.GENOTYPE.key(), sample + IS + "1/2");

        Set<String> variants = new HashSet<>();
        iterate(query, variant -> {
            Variant secVar = getSecondaryVariant(variant);
            if (variants.add(variant.toString())) {
                // Variant not seen
                if (variant.overlapWith(secVar, true)) {
                    // Add overlapping variant.
                    // If the overlapping variant is ever seen (so it also matches the filter criteria),
                    // the gene will be selected as knockout.
                    variants.add(secVar.toString());
                }
            } else {
                // The variant was already seen. i.e. there was a variant with this variant as secondary alternate
                for (ConsequenceType consequenceType : variant.getAnnotation().getConsequenceTypes()) {
                    if (validCt(consequenceType)
                            && validBiotype.test(consequenceType.getBiotype())
                            && validGene.test(consequenceType.getGeneName())) {
                        addGene(variant.toString(), consequenceType, knockoutGenes,
                                GeneKnockoutBySample.TranscriptKnockout::addMultiAllelic);
                        addGene(secVar.toString(), consequenceType, knockoutGenes,
                                GeneKnockoutBySample.TranscriptKnockout::addMultiAllelic);
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
                                    Predicate<String> validBiotype,
                                    Predicate<String> validGene)
            throws Exception {
        query = new Query(query)
                .append(VariantCatalogQueryUtils.FAMILY.key(), family.getId())
                .append(VariantCatalogQueryUtils.FAMILY_DISORDER.key(), getDisorder())
                .append(VariantCatalogQueryUtils.FAMILY_PROBAND.key(), sample)
                .append(VariantCatalogQueryUtils.FAMILY_SEGREGATION.key(), COMPOUND_HETEROZYGOUS);

        int numVariants = iterate(query, v -> {
            for (ConsequenceType consequenceType : v.getAnnotation().getConsequenceTypes()) {
                if (validCt(consequenceType)
                        && validBiotype.test(consequenceType.getBiotype())
                        && validGene.test(consequenceType.getGeneName())) {
                    addGene(v.toString(), consequenceType, knockoutGenes, GeneKnockoutBySample.TranscriptKnockout::addCompHet);
                }
            }
        });
        logger.debug("Read " + numVariants + " COMP_HET variants from sample " + sample);
    }

    protected void structuralKnockouts(String sample,
                                       Map<String, GeneKnockout> knockoutGenes,
                                       Query baseQuery,
                                       Predicate<String> validBiotype,
                                       Predicate<String> validGene) throws Exception {
        Query query = new Query(baseQuery)
                .append(VariantQueryParam.SAMPLE.key(), sample)
                .append(VariantQueryParam.TYPE.key(), VariantType.DELETION);
//                .append(VariantQueryParam.ANNOT_CONSEQUENCE_TYPE.key(), LOF + "," + VariantAnnotationUtils.FEATURE_TRUNCATION);
//        Set<String> cts = new HashSet<>(LOF_SET);
//        cts.add(VariantAnnotationUtils.FEATURE_TRUNCATION);

        iterate(query, svVariant -> {
            Set<String> transcripts = new HashSet<>(svVariant.getAnnotation().getConsequenceTypes().size());
            for (ConsequenceType consequenceType : svVariant.getAnnotation().getConsequenceTypes()) {
                if (validCt(consequenceType)
                        && validBiotype.test(consequenceType.getBiotype())
                        && validGene.test(consequenceType.getGeneName())) {
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
                    if (validCt(consequenceType)
                            && validBiotype.test(consequenceType.getBiotype())
                            && validGene.test(consequenceType.getGeneName())) {
                        if (transcripts.contains(consequenceType.getEnsemblTranscriptId())) {
                            addGene(variant.toString(), consequenceType, knockoutGenes, GeneKnockoutBySample.TranscriptKnockout::addDelOverlap);
                            addGene(svVariant.toString(), consequenceType, knockoutGenes, GeneKnockoutBySample.TranscriptKnockout::addDelOverlap);
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

    private void addGene(String variant, ConsequenceType consequenceType,
                         Map<String, GeneKnockout> knockoutGenes,
                         BiConsumer<GeneKnockoutBySample.TranscriptKnockout, String> f) {
        GeneKnockout gene = knockoutGenes.computeIfAbsent(consequenceType.getGeneName(), GeneKnockout::new);
        gene.setId(consequenceType.getEnsemblGeneId());
        gene.setBiotype(consequenceType.getBiotype());
        if (StringUtils.isNotEmpty(consequenceType.getEnsemblTranscriptId())) {
            GeneKnockoutBySample.TranscriptKnockout t = gene.addTranscript(consequenceType.getEnsemblTranscriptId());
            f.accept(t, variant);
        }
    }

    private boolean validCt(ConsequenceType consequenceType) {
        return validCt(consequenceType, getCts());
    }

    private boolean validCt(ConsequenceType consequenceType, Set<String> ctFilter) {
        if (StringUtils.isEmpty(consequenceType.getGeneName())) {
            return false;
        }
        if (StringUtils.isEmpty(consequenceType.getEnsemblTranscriptId())) {
            return false;
        }
        if (!PROTEIN_CODING.equals(consequenceType.getBiotype())) {
            return false;
        }
        if (consequenceType.getSequenceOntologyTerms().stream().noneMatch(so -> ctFilter.contains(so.getName()))) {
            return false;
        }
        return true;
    }

}
