/*
 * Copyright 2015-2020 OpenCB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.opencb.opencga.analysis.variant.knockout;

import com.google.common.collect.Iterables;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.opencb.biodata.models.core.Region;
import org.opencb.biodata.models.variant.Genotype;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.*;
import org.opencb.commons.datastore.core.DataResult;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.variant.manager.VariantCatalogQueryUtils;
import org.opencb.opencga.analysis.variant.manager.VariantStorageManager;
import org.opencb.opencga.analysis.variant.manager.VariantStorageToolExecutor;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.models.analysis.knockout.KnockoutByGene;
import org.opencb.opencga.core.models.analysis.knockout.KnockoutByIndividual;
import org.opencb.opencga.core.models.analysis.knockout.KnockoutByIndividual.KnockoutGene;
import org.opencb.opencga.core.models.analysis.knockout.KnockoutTranscript;
import org.opencb.opencga.core.models.analysis.knockout.KnockoutVariant;
import org.opencb.opencga.core.tools.annotations.ToolExecutor;
import org.opencb.opencga.storage.core.metadata.models.Trio;
import org.opencb.opencga.storage.core.variant.adaptors.GenotypeClass;
import org.opencb.opencga.storage.core.variant.adaptors.VariantField;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.core.variant.adaptors.iterators.VariantDBIterator;
import org.opencb.opencga.storage.core.variant.query.VariantQueryUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.function.Predicate;

import static org.opencb.biodata.models.clinical.ClinicalProperty.ModeOfInheritance.COMPOUND_HETEROZYGOUS;
import static org.opencb.opencga.storage.core.variant.annotation.VariantAnnotationConstants.PROTEIN_CODING;
import static org.opencb.opencga.storage.core.variant.query.VariantQueryUtils.ALL;
import static org.opencb.opencga.storage.core.variant.query.VariantQueryUtils.IS;

@ToolExecutor(id = "opencga-local",
        tool = KnockoutAnalysis.ID,
        source = ToolExecutor.Source.STORAGE,
        framework = ToolExecutor.Framework.LOCAL)
public class KnockoutLocalAnalysisExecutor extends KnockoutAnalysisExecutor implements VariantStorageToolExecutor {

    private Logger logger = LoggerFactory.getLogger(this.getClass());
    private VariantStorageManager variantStorageManager;
    private boolean allProteinCoding;
    private List<String> biotype;

    @Override
    protected void run() throws Exception {
        variantStorageManager = getVariantStorageManager();
        biotype = StringUtils.isEmpty(getBiotype()) ? null : Arrays.asList(getBiotype().split(","));
        if (biotype != null && biotype.contains(PROTEIN_CODING)) {
            biotype = new ArrayList<>(biotype);
            biotype.remove(PROTEIN_CODING);
        }
        allProteinCoding = getProteinCodingGenes().size() == 1 && getProteinCodingGenes().iterator().next().equals(ALL);

        String executionMethod = getExecutorParams().getString("executionMethod", "auto");
        boolean bySample;
        switch (executionMethod) {
            case "bySample":
                bySample = true;
                break;
            case "byGene":
                if (allProteinCoding) {
                    throw new IllegalArgumentException("Unable to execute '" + executionMethod + "' "
                            + "when analysing all protein coding genes");
                }
                bySample = false;
                break;
            case "auto":
            case "":
                if (allProteinCoding) {
                    bySample = true;
                } else if (getSamples().size() < (getProteinCodingGenes().size() + getOtherGenes().size())) {
                    bySample = true;
                } else {
                    bySample = false;
                }
                break;
            default:
                throw new IllegalArgumentException("Unknown executionMethod '" + executionMethod + "'");
        }
//        if (bySample || (auto && (allProteinCoding || (getProteinCodingGenes().size() + getOtherGenes().size()) >= getSamples().size()))) {
        if (bySample) {
            logger.info("Execute knockout analysis by sample");
            addAttribute("executionMethod", "bySample");
            new KnockoutBySampleExecutor().run();
        } else {
            logger.info("Execute knockout analysis by gene");
            addAttribute("executionMethod", "byGene");
            new KnockoutByGeneExecutor().run();
        }
    }

    private class KnockoutBySampleExecutor {
        public void run() throws Exception {
            Query baseQuery = new Query()
                    .append(VariantQueryParam.STUDY.key(), getStudy())
                    .append(VariantQueryParam.FILTER.key(), getFilter())
                    .append(VariantQueryParam.QUAL.key(), getQual());
            for (String sample : getSamples()) {
                StopWatch stopWatch = StopWatch.createStarted();
                logger.info("Processing sample {}", sample);
                Map<String, KnockoutGene> knockoutGenes = new LinkedHashMap<>();
                Trio trio = getTrios().get(sample);

                // Protein coding genes (if any)
                if (allProteinCoding) {
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
                    KnockoutByIndividual.GeneKnockoutByIndividualStats stats = getGeneKnockoutBySampleStats(knockoutGenes.values());
                    writeSampleFile(new KnockoutByIndividual()
                            .setSampleId(sample)
                            .setStats(stats)
                            .setGenes(knockoutGenes.values()));
                }
                logger.info("Sample {} processed in {}", sample, TimeUtils.durationToString(stopWatch));
                logger.info("-----------------------------------------------------------");
            }

            transposeSampleToGeneOutputFiles();
        }

        private void transposeSampleToGeneOutputFiles() throws IOException {
            int samplesBatchSize = 200;// Maths.max(1, 2000 - getSamples().size());
            int numBatches = (int) Math.ceil((float) getSamples().size() / samplesBatchSize);

            for (int batch = 0; batch < numBatches; batch++) {
                Map<String, KnockoutByGene> byGeneMap = new HashMap<>();
                List<String> samplesBatch = getSamples().subList(batch * samplesBatchSize,
                        Math.min(getSamples().size(), (batch + 1) * samplesBatchSize));
                for (String sample : samplesBatch) {
                    Path fileName = getSampleFileName(sample);
                    if (Files.exists(fileName)) {
                        KnockoutByIndividual byIndividual = readSampleFile(sample);
                        for (KnockoutGene gene : byIndividual.getGenes()) {
                            KnockoutByGene byGene;
                            // Create new gene if absent
                            byGene = byGeneMap.computeIfAbsent(gene.getName(),
                                    id -> new KnockoutByGene().setId(gene.getId()).setName(gene.getName()).setIndividuals(new LinkedList<>()));
                            KnockoutByGene.KnockoutIndividual knockoutIndividual = new KnockoutByGene.KnockoutIndividual()
                                    .setId(byIndividual.getId())
                                    .setSampleId(byIndividual.getSampleId())
                                    .setTranscripts(gene.getTranscripts());
                            byGene.getIndividuals().add(knockoutIndividual);
                        }
                    }
                }
                for (Map.Entry<String, KnockoutByGene> entry : byGeneMap.entrySet()) {
                    String gene = entry.getKey();
                    if (Files.exists(getGeneFileName(gene))) {
                        KnockoutByGene knockoutByGene = readGeneFile(gene);
                        knockoutByGene.getIndividuals().addAll(entry.getValue().getIndividuals());
                        writeGeneFile(knockoutByGene);
                    } else {
                        writeGeneFile(entry.getValue());
                    }
                }
                logger.info("Transpose sample to gene. Batch {}/{} of {} samples with {} genes",
                        batch + 1, numBatches, samplesBatch.size(), byGeneMap.size());
            }
        }

        private void knockouts(Query query, String sample, Trio trio, Map<String, KnockoutGene> knockoutGenes,
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
                                       Map<String, KnockoutGene> knockoutGenes,
                                       Query query,
                                       Predicate<String> ctFilter,
                                       Predicate<String> biotypeFilter,
                                       Predicate<String> geneFilter)
                throws Exception {
            query = new Query(query)
                    .append(VariantQueryParam.INCLUDE_SAMPLE_DATA.key(), "GT")
                    .append(VariantQueryParam.GENOTYPE.key(), sample + IS + "1/1");

            int numVariants = iterate(query, v -> {
                StudyEntry studyEntry = v.getStudies().get(0);
                SampleEntry sampleEntry = studyEntry.getSample(0);
                FileEntry fileEntry = studyEntry.getFiles().get(sampleEntry.getFileIndex());
                for (ConsequenceType consequenceType : v.getAnnotation().getConsequenceTypes()) {
                    if (validCt(consequenceType, ctFilter, biotypeFilter, geneFilter)) {
                        addGene(v.toString(), sampleEntry.getData().get(0),
                                fileEntry, consequenceType, knockoutGenes, KnockoutVariant.KnockoutType.HOM_ALT,
                                v.getAnnotation().getPopulationFrequencies());
                    }
                }
            });
            logger.debug("Read {} HOM_ALT variants from sample {}", numVariants, sample);
        }

        protected void multiAllelicKnockouts(String sample,
                                             Map<String, KnockoutGene> knockoutGenes,
                                             Query query,
                                             Predicate<String> ctFilter,
                                             Predicate<String> biotypeFilter,
                                             Predicate<String> geneFilter)
                throws Exception {

            query = new Query(query)
                    .append(VariantQueryParam.INCLUDE_SAMPLE_DATA.key(), "GT")
                    .append(VariantQueryParam.GENOTYPE.key(), sample + IS + "1/2");

            Map<String, KnockoutVariant> variants = new HashMap<>();
            iterate(query, variant -> {
                Variant secVar = getSecondaryVariant(variant);
                StudyEntry studyEntry = variant.getStudies().get(0);
                SampleEntry sampleEntry = studyEntry.getSample(0);
                FileEntry fileEntry = studyEntry.getFiles().get(sampleEntry.getFileIndex());
                KnockoutVariant knockoutVariant = new KnockoutVariant(
                        variant.toString(),
                        sampleEntry.getData().get(0),
                        fileEntry.getData().get(StudyEntry.FILTER),
                        fileEntry.getData().get(StudyEntry.QUAL),
                        KnockoutVariant.KnockoutType.HET_ALT,
                        null,
                        variant.getAnnotation().getPopulationFrequencies()
                );
                if (variants.put(variant.toString(), knockoutVariant) == null) {
                    // Variant not seen
                    if (variant.overlapWith(secVar, true)) {
                        // Add overlapping variant.
                        // If the overlapping variant is ever seen (so it also matches the filter criteria),
                        // the gene will be selected as knockout.
                        variants.put(secVar.toString(), new KnockoutVariant());
                    }
                } else {
                    KnockoutVariant secKnockoutVar = variants.get(secVar.toString());
                    // The variant was already seen. i.e. there was a variant with this variant as secondary alternate
                    for (ConsequenceType consequenceType : variant.getAnnotation().getConsequenceTypes()) {
                        if (validCt(consequenceType, ctFilter, biotypeFilter, geneFilter)) {
                            String gt = studyEntry.getSampleData(0).get(0);
                            addGene(variant.toString(), gt, knockoutVariant.getFilter(), knockoutVariant.getQual(),
                                    consequenceType, knockoutGenes, KnockoutVariant.KnockoutType.HET_ALT,
                                    variant.getAnnotation().getPopulationFrequencies()
                            );
                            addGene(secVar.toString(), secKnockoutVar.getGenotype(), secKnockoutVar.getFilter(), secKnockoutVar.getQual(),
                                    consequenceType, knockoutGenes, KnockoutVariant.KnockoutType.HET_ALT,
                                    secKnockoutVar.getPopulationFrequencies()
                            );
                        }
                    }
                }
            });
        }

        protected void compHetKnockouts(String sample, Trio family,
                                        Map<String, KnockoutGene> knockoutGenes,
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
                    .append(VariantQueryParam.INCLUDE_SAMPLE.key(), family.toList())
                    .append(VariantQueryParam.INCLUDE_SAMPLE_DATA.key(), "GT");

            int numVariants = iterate(query, v -> {
                StudyEntry studyEntry = v.getStudies().get(0);
                SampleEntry sampleEntry = studyEntry.getSample(0);
                FileEntry fileEntry = studyEntry.getFiles().get(sampleEntry.getFileIndex());
                for (ConsequenceType consequenceType : v.getAnnotation().getConsequenceTypes()) {
                    if (validCt(consequenceType, ctFilter, biotypeFilter, geneFilter)) {
                        addGene(v.toString(), sampleEntry.getData().get(0),
                                fileEntry, consequenceType, knockoutGenes, KnockoutVariant.KnockoutType.COMP_HET,
                                v.getAnnotation().getPopulationFrequencies());
                    }
                }
            });
            logger.debug("Read " + numVariants + " COMP_HET variants from sample " + sample);
        }

        protected void structuralKnockouts(String sample,
                                           Map<String, KnockoutGene> knockoutGenes,
                                           Query baseQuery,
                                           Predicate<String> ctFilter,
                                           Predicate<String> biotypeFilter,
                                           Predicate<String> geneFilter) throws Exception {
            Query query = new Query(baseQuery)
                    .append(VariantQueryParam.SAMPLE.key(), sample)
                    .append(VariantQueryParam.TYPE.key(), VariantType.DELETION)
                    .append(VariantQueryParam.INCLUDE_SAMPLE_DATA.key(), "GT");
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

                SampleEntry svSample = svVariant.getStudies().get(0).getSample(0);
                FileEntry svFileEntry = svVariant.getStudies().get(0).getFiles().get(svSample.getFileIndex());

                iterate(thisSvQuery, variant -> {
                    if (variant.sameGenomicVariant(svVariant)) {
                        return;
                    }
                    StudyEntry studyEntry = variant.getStudies().get(0);
                    SampleEntry sampleEntry = studyEntry.getSample(0);
                    FileEntry fileEntry = studyEntry.getFiles().get(sampleEntry.getFileIndex());
                    for (ConsequenceType consequenceType : variant.getAnnotation().getConsequenceTypes()) {
                        if (validCt(consequenceType, ctFilter, biotypeFilter, geneFilter)) {
                            if (transcripts.contains(consequenceType.getEnsemblTranscriptId())) {
                                addGene(variant.toString(), svSample.getData().get(0), svFileEntry, consequenceType, knockoutGenes,
                                        KnockoutVariant.KnockoutType.DELETION_OVERLAP,
                                        variant.getAnnotation().getPopulationFrequencies());
                                addGene(svVariant.toString(), sampleEntry.getData().get(0), fileEntry, consequenceType, knockoutGenes,
                                        KnockoutVariant.KnockoutType.DELETION_OVERLAP,
                                        variant.getAnnotation().getPopulationFrequencies());
                            }
                        }
                    }
                });
            });
        }
    }

    private class KnockoutByGeneExecutor {

        protected void run() throws Exception {
            Query baseQuery = new Query()
                    .append(VariantQueryParam.STUDY.key(), getStudy())
                    .append(VariantQueryParam.INCLUDE_SAMPLE.key(), getSamples())
                    .append(VariantQueryParam.INCLUDE_GENOTYPE.key(), true);

            for (String gene : getProteinCodingGenes()) {
                knockoutGene(new Query(baseQuery)
                                .append(VariantQueryParam.GENE.key(), gene)
                                .append(VariantQueryParam.ANNOT_CONSEQUENCE_TYPE.key(), getCts())
                                .append(VariantQueryParam.ANNOT_BIOTYPE.key(), PROTEIN_CODING), gene,
                        getCts()::contains,
                        PROTEIN_CODING::equals);
            }
            for (String gene : getOtherGenes()) {
                knockoutGene(new Query(baseQuery)
                                .append(VariantQueryParam.GENE.key(), gene)
                                .append(VariantQueryParam.STUDY.key(), getStudy())
                                .append(VariantQueryParam.ANNOT_BIOTYPE.key(), biotype), gene,
                        c -> true,
                        biotype == null ? b -> !b.equals(PROTEIN_CODING) : new HashSet<>(biotype)::contains);
            }

            transposeGeneToSampleOutputFiles();
        }

        private void knockoutGene(Query baseQuery, String gene, Predicate<String> ctFilter, Predicate<String> biotypeFilter) throws Exception {
            KnockoutByGene knockout;
            StopWatch stopWatch = StopWatch.createStarted();
            if (getGeneFileName(gene).toFile().exists()) {
                knockout = readGeneFile(gene);
            } else {
                knockout = new KnockoutByGene();
            }
            knockout.setName(gene);

            Map<String, Integer> compHetCandidateSamples = new HashMap<>();
            Map<String, Integer> multiAllelicCandidateSamples = new HashMap<>();
            Map<String, Integer> deletionCandidateSamples = new HashMap<>();

            Query query = new Query(baseQuery)
                    .append(VariantQueryParam.INCLUDE_SAMPLE.key(), VariantQueryUtils.NONE)
                    .append(VariantQueryParam.INCLUDE_FILE.key(), VariantQueryUtils.NONE);
            int numVariants = iterate(query, new QueryOptions(VariantField.SUMMARY, true), v -> {
                int limit = 1000;
                int skip = 0;
                int numSamples;
                do {
                    DataResult<Variant> result = variantStorageManager.getSampleData(v.toString(), getStudy(),
                            new QueryOptions(QueryOptions.LIMIT, limit)
                                    .append(QueryOptions.SKIP, skip)
                                    .append(VariantQueryParam.INCLUDE_SAMPLE.key(), getSamples())
                                    .append(VariantQueryParam.INCLUDE_FILE.key(), null)
                                    .append(VariantQueryParam.GENOTYPE.key(), GenotypeClass.MAIN_ALT), getToken());
                    Variant variant = result.first();
                    numSamples = variant.getStudies().get(0).getSamples().size();
                    skip += numSamples;
                    logger.info("[{}] - Found {} samples at variant {}", gene,
                            numSamples, v.toString());

                    VariantAnnotation annotation = variant.getAnnotation();
                    List<ConsequenceType> cts = new ArrayList<>(annotation.getConsequenceTypes().size());
                    for (ConsequenceType ct : annotation.getConsequenceTypes()) {
                        if (validCt(ct, ctFilter, biotypeFilter, gene::equals)) {
                            cts.add(ct);
                        }
                    }
                    if (cts.isEmpty()) {
                        logger.info("[{}] - Skip variant {}. CT filter not match", gene, variant.toString());
                        return;
                    }
                    knockout.setId(cts.get(0).getEnsemblGeneId());
                    StudyEntry studyEntry = variant.getStudies().get(0);
                    for (SampleEntry sampleEntry : studyEntry.getSamples()) {
                        List<String> data = sampleEntry.getData();
                        String genotype = data.get(0);
                        String sample = sampleEntry.getSampleId();
                        if (GenotypeClass.MAIN_ALT.test(genotype)) {
                            if (GenotypeClass.HOM_ALT.test(genotype)) {
                                FileEntry fileEntry = studyEntry.getFiles().get(sampleEntry.getFileIndex());
                                KnockoutByGene.KnockoutIndividual knockoutIndividual = knockout.getIndividualBySampleId(sample);
//                                knockoutIndividual.setId(sample);
                                knockoutIndividual.setSampleId(sample);
                                for (ConsequenceType ct : cts) {
                                    KnockoutTranscript knockoutTranscript = knockoutIndividual.getTranscript(ct.getEnsemblTranscriptId());
                                    knockoutTranscript.setBiotype(ct.getBiotype());
                                    knockoutTranscript.setStrand(ct.getStrand());
                                    knockoutTranscript.addVariant(new KnockoutVariant(
                                            variant.toString(), genotype,
                                            fileEntry.getData().get(StudyEntry.FILTER),
                                            fileEntry.getData().get(StudyEntry.QUAL),
                                            KnockoutVariant.KnockoutType.HOM_ALT,
                                            ct.getSequenceOntologyTerms(),
                                            annotation.getPopulationFrequencies()
                                    ));
                                }
                            } else if (GenotypeClass.HET_REF.test(genotype)) {
                                if (variant.getType().equals(VariantType.DELETION)) {
                                    deletionCandidateSamples.merge(sample, 1, Integer::sum);
                                }
                                compHetCandidateSamples.merge(sample, 1, Integer::sum);
                            } else if (GenotypeClass.HET_ALT.test(genotype)) {
                                multiAllelicCandidateSamples.merge(sample, 1, Integer::sum);
                            }
                        }
                    }
                } while (numSamples == limit);
            });

            logger.info("Found {} candidate variants for gene {}", numVariants, gene);

            for (Map.Entry<String, Integer> entry : compHetCandidateSamples.entrySet()) {
                if (entry.getValue() >= 2) {
                    // Check compound heterozygous for other samples
                    compHetKnockout(baseQuery, knockout, entry.getKey(), ctFilter, biotypeFilter);
                }
            }
            for (Map.Entry<String, Integer> entry : multiAllelicCandidateSamples.entrySet()) {
                if (entry.getValue() >= 2) {
                    // Check multiAllelic for other samples
                    multiAllelicKnockout(baseQuery, knockout, entry.getKey(), ctFilter, biotypeFilter);
                }
            }
            for (Map.Entry<String, Integer> entry : deletionCandidateSamples.entrySet()) {
                // Check overlapping deletions for other samples if there is any large deletion
                structuralKnockouts(baseQuery, knockout, entry.getKey(), ctFilter, biotypeFilter);
            }
            logger.info("Gene {} processed in {}", gene, TimeUtils.durationToString(stopWatch));
            logger.info("-----------------------------------------------------------");
            writeGeneFile(knockout);
        }

        private void compHetKnockout(Query baseQuery, KnockoutByGene knockoutByGene, String sampleId,
                                     Predicate<String> ctFilter, Predicate<String> biotypeFilter) throws Exception {
            Trio trio = getTrios().get(sampleId);
            if (trio == null) {
                return;
            }
            Query query = new Query(baseQuery)
                    .append(VariantQueryParam.GENE.key(), knockoutByGene.getName())
                    .append(VariantQueryParam.INCLUDE_SAMPLE.key(), trio.toList())
                    .append(VariantCatalogQueryUtils.FAMILY.key(), trio.getId())
                    .append(VariantQueryParam.INCLUDE_SAMPLE_DATA.key(), "GT")
                    .append(VariantQueryParam.INCLUDE_FILE.key(), null)
//                            .append(VariantCatalogQueryUtils.FAMILY_DISORDER.key(), getDisorder())
                    .append(VariantCatalogQueryUtils.FAMILY_PROBAND.key(), sampleId)
                    .append(VariantCatalogQueryUtils.FAMILY_SEGREGATION.key(), COMPOUND_HETEROZYGOUS);
            try (VariantDBIterator iterator = variantStorageManager.iterator(query, new QueryOptions(), getToken())) {
                while (iterator.hasNext()) {
                    Variant variant = iterator.next();
                    StudyEntry studyEntry = variant.getStudies().get(0);
                    SampleEntry sampleEntry = studyEntry.getSample(0);
                    FileEntry fileEntry = studyEntry.getFiles().get(sampleEntry.getFileIndex());

                    KnockoutByGene.KnockoutIndividual knockoutIndividual = knockoutByGene.getIndividualBySampleId(sampleId);
                    for (ConsequenceType ct : variant.getAnnotation().getConsequenceTypes()) {
                        if (validCt(ct, ctFilter, biotypeFilter, knockoutByGene.getName()::equals)) {
                            KnockoutTranscript knockoutTranscript = knockoutIndividual.getTranscript(ct.getEnsemblTranscriptId());
                            knockoutTranscript.setBiotype(ct.getBiotype());
                            knockoutTranscript.setStrand(ct.getStrand());
                            knockoutTranscript.addVariant(new KnockoutVariant(
                                    variant.toString(), sampleEntry.getData().get(0),
                                    fileEntry.getData().get(StudyEntry.FILTER),
                                    fileEntry.getData().get(StudyEntry.QUAL),
                                    KnockoutVariant.KnockoutType.COMP_HET,
                                    ct.getSequenceOntologyTerms(),
                                    variant.getAnnotation().getPopulationFrequencies()));
                        }
                    }
                }
            }
        }

        private void multiAllelicKnockout(Query baseQuery, KnockoutByGene knockout, String sampleId,
                                          Predicate<String> ctFilter, Predicate<String> biotypeFilter) throws Exception {
            Query query = new Query(baseQuery)
                    .append(VariantQueryParam.INCLUDE_GENOTYPE.key(), true)
                    .append(VariantQueryParam.INCLUDE_SAMPLE.key(), sampleId)
                    .append(VariantQueryParam.INCLUDE_SAMPLE_DATA.key(), "GT")
                    .append(VariantQueryParam.INCLUDE_FILE.key(), null)
                    .append(VariantQueryParam.GENOTYPE.key(), sampleId + IS + "1/2");

            Map<String, KnockoutVariant> variants = new HashMap<>();
            iterate(query, variant -> {
                StudyEntry studyEntry = variant.getStudies().get(0);
                SampleEntry sampleEntry = studyEntry.getSample(0);
                FileEntry fileEntry = studyEntry.getFiles().get(sampleEntry.getFileIndex());

                Variant secVar = getSecondaryVariant(variant);
                KnockoutVariant knockoutVariant = new KnockoutVariant(
                        variant.toString(),
                        variant.getStudies().get(0).getSampleData(0).get(0),
                        fileEntry.getData().get(StudyEntry.FILTER),
                        fileEntry.getData().get(StudyEntry.QUAL),
                        KnockoutVariant.KnockoutType.HET_ALT, null, variant.getAnnotation().getPopulationFrequencies()
                );
                if (variants.put(variant.toString(), knockoutVariant) == null) {
                    // Variant not seen
                    if (variant.overlapWith(secVar, true)) {
                        // Add overlapping variant.
                        // If the overlapping variant is ever seen (so it also matches the filter criteria),
                        // the gene will be selected as knockout.
                        variants.put(secVar.toString(), new KnockoutVariant());
                    }
                } else {
                    KnockoutByGene.KnockoutIndividual knockoutIndividual = knockout.getIndividualBySampleId(sampleId);
                    // The variant was already seen. i.e. there was a variant with this variant as secondary alternate
                    for (ConsequenceType ct : variant.getAnnotation().getConsequenceTypes()) {
                        if (validCt(ct, ctFilter, biotypeFilter, knockout.getName()::equals)) {
                            KnockoutTranscript knockoutTranscript = knockoutIndividual.getTranscript(ct.getEnsemblTranscriptId());
                            knockoutTranscript.setBiotype(ct.getBiotype());
                            knockoutTranscript.setStrand(ct.getStrand());

                            String gt = sampleEntry.getData().get(0);
                            knockoutTranscript.addVariant(new KnockoutVariant(
                                    variant.toString(), gt, null, null,
                                    KnockoutVariant.KnockoutType.HET_ALT,
                                    ct.getSequenceOntologyTerms(),
                                    variant.getAnnotation().getPopulationFrequencies()));

                            KnockoutVariant secKnockoutVar = variants.get(secVar.toString());
                            knockoutTranscript.addVariant(new KnockoutVariant(
                                    secKnockoutVar.getId(),
                                    secKnockoutVar.getGenotype(),
                                    secKnockoutVar.getFilter(),
                                    secKnockoutVar.getQual(),
                                    KnockoutVariant.KnockoutType.HET_ALT,
                                    ct.getSequenceOntologyTerms(),
                                    variant.getAnnotation().getPopulationFrequencies()));
                        }
                    }
                }
            });
        }

        protected void structuralKnockouts(Query baseQuery, KnockoutByGene knockout, String sampleId,
                                           Predicate<String> ctFilter, Predicate<String> biotypeFilter) throws Exception {
            Query query = new Query(baseQuery)
                    .append(VariantQueryParam.SAMPLE.key(), sampleId)
                    .append(VariantQueryParam.INCLUDE_SAMPLE.key(), sampleId)
                    .append(VariantQueryParam.INCLUDE_SAMPLE_DATA.key(), "GT")
                    .append(VariantQueryParam.TYPE.key(), VariantType.DELETION);

            KnockoutByGene.KnockoutIndividual knockoutIndividual = knockout.getIndividualBySampleId(sampleId);

            iterate(query, svVariant -> {
                Set<String> transcripts = new HashSet<>(svVariant.getAnnotation().getConsequenceTypes().size());
                for (ConsequenceType consequenceType : svVariant.getAnnotation().getConsequenceTypes()) {
                    if (validCt(consequenceType, ctFilter, biotypeFilter, knockout.getName()::equals)) {
                        transcripts.add(consequenceType.getEnsemblTranscriptId());
                    }
                }

                List<String> svSampleData = svVariant.getStudies().get(0).getSampleData(0);
                FileEntry svFileEntry = svVariant.getStudies().get(0).getFiles().get(Integer.parseInt(svSampleData.get(1)));

                Query thisSvQuery = new Query(baseQuery)
                        .append(VariantQueryParam.SAMPLE.key(), sampleId)
                        .append(VariantQueryParam.INCLUDE_SAMPLE.key(), sampleId)
                        .append(VariantQueryParam.INCLUDE_SAMPLE_DATA.key(), "GT")
                        .append(VariantQueryParam.REGION.key(), new Region(svVariant.getChromosome(), svVariant.getStart(), svVariant.getEnd()));

                iterate(thisSvQuery, variant -> {
                    if (variant.sameGenomicVariant(svVariant)) {
                        return;
                    }
                    for (ConsequenceType ct : variant.getAnnotation().getConsequenceTypes()) {
                        if (validCt(ct, ctFilter, biotypeFilter, knockout.getName()::equals)) {
                            if (transcripts.contains(ct.getEnsemblTranscriptId())) {
                                KnockoutTranscript knockoutTranscript = knockoutIndividual.getTranscript(ct.getEnsemblTranscriptId());
                                knockoutTranscript.setBiotype(ct.getBiotype());
                                knockoutTranscript.setStrand(ct.getStrand());

                                StudyEntry studyEntry = variant.getStudies().get(0);
                                SampleEntry sampleEntry = studyEntry.getSample(0);
                                FileEntry fileEntry = studyEntry.getFiles().get(sampleEntry.getFileIndex());

                                knockoutTranscript.addVariant(new KnockoutVariant(
                                        variant.toString(), sampleEntry.getData().get(0),
                                        fileEntry.getData().get(StudyEntry.FILTER),
                                        fileEntry.getData().get(StudyEntry.QUAL),
                                        KnockoutVariant.KnockoutType.DELETION_OVERLAP,
                                        ct.getSequenceOntologyTerms(),
                                        variant.getAnnotation().getPopulationFrequencies()));

                                knockoutTranscript.addVariant(new KnockoutVariant(
                                        svVariant.toString(), svSampleData.get(0),
                                        svFileEntry.getData().get(StudyEntry.FILTER),
                                        svFileEntry.getData().get(StudyEntry.QUAL),
                                        KnockoutVariant.KnockoutType.DELETION_OVERLAP,
                                        ct.getSequenceOntologyTerms(),
                                        variant.getAnnotation().getPopulationFrequencies()));
                            }
                        }
                    }
                });
            });
        }

        private void transposeGeneToSampleOutputFiles() throws IOException {
            Map<String, KnockoutByIndividual> byIndividualMap = new HashMap<>();
            for (String gene : Iterables.concat(getOtherGenes(), getProteinCodingGenes())) {
                Path fileName = getGeneFileName(gene);
                if (Files.exists(fileName)) {
                    KnockoutByGene byGene = readGeneFile(gene);
                    for (KnockoutByGene.KnockoutIndividual sample : byGene.getIndividuals()) {
                        KnockoutByIndividual byIndividual = byIndividualMap
                                .computeIfAbsent(sample.getSampleId(), s -> new KnockoutByIndividual())
                                .setSampleId(sample.getSampleId());

                        byIndividual.getGene(gene)
                                .setBiotype(byGene.getBiotype())
                                .setStrand(byGene.getStrand())
                                .setChromosome(byGene.getChromosome())
                                .setStart(byGene.getStart())
                                .setEnd(byGene.getEnd())
                                .addTranscripts(sample.getTranscripts());

                    }
                }
            }
//            buildGeneKnockoutBySample(sample, knockoutGenes);
            for (Map.Entry<String, KnockoutByIndividual> entry : byIndividualMap.entrySet()) {
                KnockoutByIndividual knockoutByIndividual = entry.getValue();
                KnockoutByIndividual.GeneKnockoutByIndividualStats stats = getGeneKnockoutBySampleStats(knockoutByIndividual.getGenes());
                knockoutByIndividual.setStats(stats);
                writeSampleFile(knockoutByIndividual);
            }
        }
    }

    public interface VariantConsumer {
        void accept(Variant v) throws Exception;
    }

    private int iterate(Query query, VariantConsumer c)
            throws Exception {
        return iterate(query, new QueryOptions(), c);
    }

    private int iterate(Query query, QueryOptions queryOptions, VariantConsumer c)
            throws Exception {
        int numVariants;
        try (VariantDBIterator iterator = variantStorageManager.iterator(query, queryOptions, getToken())) {
            while (iterator.hasNext()) {
                c.accept(iterator.next());
            }
            numVariants = iterator.getCount();
        }
        return numVariants;
    }

    private void addGene(String variant, String gt, FileEntry fileEntry, ConsequenceType consequenceType,
                         Map<String, KnockoutGene> knockoutGenes,
                         KnockoutVariant.KnockoutType knockoutType, List<PopulationFrequency> populationFrequencies) {
        addGene(variant, gt, fileEntry.getData().get(StudyEntry.FILTER), fileEntry.getData().get(StudyEntry.QUAL),
                consequenceType, knockoutGenes, knockoutType, populationFrequencies);
    }

    private void addGene(String variant, String gt, String filter, String qual, ConsequenceType consequenceType,
                         Map<String, KnockoutGene> knockoutGenes,
                         KnockoutVariant.KnockoutType knockoutType, List<PopulationFrequency> populationFrequencies) {
        KnockoutGene gene = knockoutGenes.computeIfAbsent(consequenceType.getGeneName(), KnockoutGene::new);
        gene.setId(consequenceType.getEnsemblGeneId());
        gene.setBiotype(consequenceType.getBiotype());
        gene.setStrand(consequenceType.getStrand());
        if (StringUtils.isNotEmpty(consequenceType.getEnsemblTranscriptId())) {
            KnockoutTranscript t = gene.getTranscript(consequenceType.getEnsemblTranscriptId());
            t.setBiotype(consequenceType.getBiotype());
            t.setStrand(consequenceType.getStrand());
            t.addVariant(new KnockoutVariant(variant, gt, filter, qual, knockoutType,
                    consequenceType.getSequenceOntologyTerms(), populationFrequencies));
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

    private Variant getSecondaryVariant(Variant variant) {
        Genotype gt = new Genotype(variant.getStudies().get(0).getSampleData(0).get(0));
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

}
