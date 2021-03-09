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

import com.fasterxml.jackson.core.util.MinimalPrettyPrinter;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.SequenceWriter;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.RegexFileFilter;
import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.models.clinical.interpretation.DiseasePanel;
import org.opencb.biodata.models.core.Gene;
import org.opencb.biodata.models.core.Transcript;
import org.opencb.opencga.core.tools.annotations.ToolParams;
import org.opencb.opencga.storage.core.variant.annotation.VariantAnnotationConstants;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.tools.OpenCgaToolScopeStudy;
import org.opencb.opencga.catalog.db.api.IndividualDBAdaptor;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.common.JacksonUtils;
import org.opencb.opencga.core.models.analysis.knockout.KnockoutByGene;
import org.opencb.opencga.core.models.analysis.knockout.KnockoutByIndividual;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.family.Family;
import org.opencb.opencga.core.models.individual.Individual;
import org.opencb.opencga.core.models.panel.Panel;
import org.opencb.opencga.core.models.variant.KnockoutAnalysisParams;
import org.opencb.opencga.core.tools.annotations.Tool;
import org.opencb.opencga.storage.core.metadata.models.Trio;
import org.opencb.opencga.storage.core.utils.CellBaseUtils;
import org.opencb.opencga.storage.core.variant.query.VariantQueryUtils;

import java.io.BufferedWriter;
import java.io.File;
import java.nio.file.Path;
import java.util.*;
import java.util.function.Predicate;
import java.util.stream.Collectors;

@Tool(id= KnockoutAnalysis.ID, description = KnockoutAnalysis.DESCRIPTION, resource = Enums.Resource.VARIANT)
public class KnockoutAnalysis extends OpenCgaToolScopeStudy {
    public static final String ID = "knockout";
    public static final String DESCRIPTION = "Obtains the list of knocked out genes for each sample.";
    protected static final String KNOCKOUT_INDIVIDUALS_JSON = "knockout.individuals.json.gz";
    protected static final String KNOCKOUT_GENES_JSON = "knockout.genes.json.gz";

    @ToolParams
    protected final KnockoutAnalysisParams analysisParams = new KnockoutAnalysisParams();

    @Override
    protected List<String> getSteps() {
        return Arrays.asList(
                "list-genes",
                "list-families",
                getId(),
                "add-metadata-to-output-files");
    }

    @Override
    protected void check() throws Exception {
        super.check();
        executorParams.put("executionMethod", params.getString("executionMethod", "auto"));

        if (CollectionUtils.isEmpty(analysisParams.getSample())
                || analysisParams.getSample().size() == 1 && analysisParams.getSample().get(0).equals(ParamConstants.ALL)) {
            analysisParams.setSample(new ArrayList<>(getVariantStorageManager().getIndexedSamples(getStudy(), getToken())));
        }

        if (StringUtils.isEmpty(analysisParams.getBiotype())) {
            if (CollectionUtils.isEmpty(analysisParams.getGene()) && CollectionUtils.isEmpty(analysisParams.getPanel())) {
                analysisParams.setBiotype(VariantAnnotationConstants.PROTEIN_CODING);
            }
        }

        if (StringUtils.isEmpty(analysisParams.getConsequenceType())) {
            analysisParams.setConsequenceType(VariantQueryUtils.LOF);
        }

        super.check();
    }

    @Override
    protected void run() throws Exception {

        List<String> proteinCodingGenes = new LinkedList<>();
        List<String> otherGenes = new LinkedList<>();

        step("list-genes", () -> {
            CellBaseUtils cellBaseUtils = getVariantStorageManager().getCellBaseUtils(getStudy(), token);
            QueryOptions queryOptions = new QueryOptions(QueryOptions.INCLUDE, "id,name,chromosome,start,end,biotype,transcripts.biotype");

            boolean allProteinCoding = false;
            if (CollectionUtils.isEmpty(analysisParams.getGene()) && CollectionUtils.isEmpty(analysisParams.getPanel())) {
                // No genes or panel given.
                // Get genes by biotype
                List<String> biotypes = new ArrayList<>(Arrays.asList(analysisParams.getBiotype().split(",")));
                if (biotypes.contains(VariantAnnotationConstants.PROTEIN_CODING)) {
                    allProteinCoding = true;
                    proteinCodingGenes.add(VariantQueryUtils.ALL);
                    biotypes.remove(VariantAnnotationConstants.PROTEIN_CODING);
                }
                if (!biotypes.isEmpty()) {
                    Query query = new Query(org.opencb.cellbase.core.ParamConstants.TRANSCRIPT_BIOTYPES_PARAM, String.join(",", biotypes));
                    for (Gene gene : cellBaseUtils.getCellBaseClient().getGeneClient().search(query, queryOptions).allResults()) {
                        otherGenes.add(gene.getName());
                    }
                }
            } else {
                Set<String> genes = new HashSet<>();
                Predicate<String> biotypeFilter;
                if (StringUtils.isNotEmpty(analysisParams.getBiotype())) {
                    biotypeFilter = new HashSet<>(Arrays.asList(analysisParams.getBiotype().split(",")))::contains;
                } else {
                    biotypeFilter = s -> true;
                }
                if (CollectionUtils.isNotEmpty(analysisParams.getGene())) {
                    genes.addAll(analysisParams.getGene());
                }
                if (CollectionUtils.isNotEmpty(analysisParams.getPanel())) {
                    List<Panel> panels = getCatalogManager()
                            .getPanelManager()
                            .get(getStudy(), analysisParams.getPanel(), new QueryOptions(), getToken())
                            .getResults();
                    for (Panel panel : panels) {
                        for (DiseasePanel.GenePanel gene : panel.getGenes()) {
                            genes.add(gene.getName());
                        }
                    }
                }
                for (Gene gene : cellBaseUtils.getCellBaseClient().getGeneClient().get(new ArrayList<>(genes), queryOptions).allResults()) {
                    Set<String> biotypes = gene.getTranscripts().stream()
                            .map(Transcript::getBiotype)
                            .filter(biotypeFilter)
                            .collect(Collectors.toSet());
                    if (biotypes.contains(VariantAnnotationConstants.PROTEIN_CODING)) {
                        proteinCodingGenes.add(gene.getName());
                    }
                    if (biotypes.size() == 1 && !biotypes.contains(VariantAnnotationConstants.PROTEIN_CODING) || biotypes.size() > 1) {
                        otherGenes.add(gene.getName());
                    }
                }
            }
            if (allProteinCoding) {
                addAttribute("proteinCodingGenesCount", VariantQueryUtils.ALL);
            } else {
                addAttribute("proteinCodingGenesCount", proteinCodingGenes.size());
            }
            addAttribute("otherGenesCount", otherGenes.size());

//            addAttribute("proteinCodingGenes", proteinCodingGenes);
//            addAttribute("otherGenes", otherGenes);
        });

        Map<String, Trio> triosMap = new HashMap<>(analysisParams.getSample().size());
        step("list-families", () -> {
            Query familyQuery = new Query(IndividualDBAdaptor.QueryParams.SAMPLES.key(), analysisParams.getSample());
            for (Family family : getCatalogManager().getFamilyManager()
                    .search(getStudy(), familyQuery, new QueryOptions(), getToken()).getResults()) {
                if (family == null || StringUtils.isEmpty(family.getId())) {
                    continue;
                }
                List<List<String>> trios = variantStorageManager.getTriosFromFamily(getStudy(), family, true, getToken());
                for (List<String> trio : trios) {
                    String child = trio.get(2);
                    if (analysisParams.getSample().contains(child)) {
                        String father = trio.get(0);
                        String mother = trio.get(1);
                        triosMap.put(child, new Trio(family.getId(),
                                "-".equals(father) ? null : father,
                                "-".equals(mother) ? null : mother,
                                child));
                    }
                }
            }
            List<String> samplesWithoutFamily = new LinkedList<>();
            for (String sample : analysisParams.getSample()) {
                if (!triosMap.containsKey(sample)) {
                    samplesWithoutFamily.add(sample);
                }
            }
            if (samplesWithoutFamily.size() < 10) {
                samplesWithoutFamily.sort(String::compareTo);
                String warning = "Missing family for samples " + samplesWithoutFamily + ". Unable to get compoundHeterozygous.";
                logger.warn(warning);
                addWarning(warning);
            } else {
                String warning = "Missing family for " + samplesWithoutFamily.size() + " samples. Unable to get compoundHeterozygous.";
                logger.warn(warning);
                addWarning(warning);
            }
        });

        step(() -> {
            setUpStorageEngineExecutor(getStudy());
//            MappingIterator<String> objectMappingIterator = JacksonUtils.getDefaultObjectMapper().reader().readValues(genesFile);
//            List<String> genes = objectMappingIterator.readAll(new ArrayList<>());
            getToolExecutor(KnockoutAnalysisExecutor.class)
                    .setStudy(getStudy())
                    .setSamples(analysisParams.getSample())
                    .setSampleFileNamePattern(getScratchDir().resolve("knockout.sample.{sample}.json.gz").toString())
                    .setGeneFileNamePattern(getScratchDir().resolve("knockout.gene.{gene}.json.gz").toString())
                    .setProteinCodingGenes(new HashSet<>(proteinCodingGenes))
                    .setOtherGenes(new HashSet<>(otherGenes))
                    .setBiotype(analysisParams.getBiotype())
                    .setFilter(analysisParams.getFilter())
                    .setQual(analysisParams.getQual())
                    .setCt(analysisParams.getConsequenceType())
                    .setTrios(triosMap)
                    .setSkipGenesFile(analysisParams.isSkipGenesFile())
                    .execute();
        });

        step("add-metadata-to-output-files", () -> {
            Map<String, String> sampleIdToIndividualIdMap = new HashMap<>();
            ObjectReader reader = JacksonUtils.getDefaultObjectMapper().readerFor(KnockoutByIndividual.class);

            try (BufferedWriter bufferedWriter = org.opencb.commons.utils.FileUtils.newBufferedWriter(getIndividualsOutputFile());
                 SequenceWriter writer = JacksonUtils.getDefaultObjectMapper()
//                    .writerWithDefaultPrettyPrinter()
                    .writer(new MinimalPrettyPrinter("\n"))
                    .forType(KnockoutByIndividual.class)
                    .writeValues(bufferedWriter)) {
                int samples = 0;
                int samplesWithoutIndividual = 0;
                for (File file : FileUtils.listFiles(getScratchDir().toFile(), new RegexFileFilter("knockout.sample..*.json.gz"), null)) {
                    KnockoutByIndividual knockoutByIndividual = reader.readValue(org.opencb.commons.utils.FileUtils.newBufferedReader(file.toPath()));
                    samples++;
                    Individual individual = catalogManager
                            .getIndividualManager()
                            .search(getStudy(), new Query(IndividualDBAdaptor.QueryParams.SAMPLES.key(), knockoutByIndividual.getSampleId()),
                                    new QueryOptions(QueryOptions.INCLUDE, Arrays.asList(
                                            IndividualDBAdaptor.QueryParams.ID.key(),
                                            IndividualDBAdaptor.QueryParams.SEX.key(),
                                            IndividualDBAdaptor.QueryParams.FATHER.key(),
                                            IndividualDBAdaptor.QueryParams.MOTHER.key(),
                                            IndividualDBAdaptor.QueryParams.PHENOTYPES.key(),
                                            IndividualDBAdaptor.QueryParams.DISORDERS.key()
                                    )),
                                    getToken())
                            .first();
                    if (individual == null) {
                        samplesWithoutIndividual++;
                        logger.warn("Individual not found for sample '{}'", knockoutByIndividual.getSampleId());
                        // Empty value for missing VariantFileIndexerOperationManagerindividuals
                        sampleIdToIndividualIdMap.put(knockoutByIndividual.getSampleId(), "");
                    } else {
                        sampleIdToIndividualIdMap.put(knockoutByIndividual.getSampleId(), individual.getId());
                        knockoutByIndividual.setId(individual.getId());
                        if (individual.getFather() != null) {
                            knockoutByIndividual.setFatherId(individual.getFather().getId());
                        }
                        if (individual.getMother() != null) {
                            knockoutByIndividual.setMotherId(individual.getMother().getId());
                        }
                        knockoutByIndividual.setSex(individual.getSex());
                        knockoutByIndividual.setDisorders(individual.getDisorders());
                        knockoutByIndividual.setPhenotypes(individual.getPhenotypes());
                    }
                    writer.write(knockoutByIndividual);
                }
                addAttribute("samples", samples);
                // get unique individuals
                Set<String> individuals = new HashSet<>(sampleIdToIndividualIdMap.values());
                individuals.remove("");
                addAttribute("individuals", individuals.size());
                addAttribute("samplesWithoutIndividual", samplesWithoutIndividual);
            }

            if (analysisParams.isSkipGenesFile()) {
                logger.info("Skip genes file generation");
            } else {
                reader = JacksonUtils.getDefaultObjectMapper().readerFor(KnockoutByGene.class);
                try (BufferedWriter bufferedWriter = org.opencb.commons.utils.FileUtils.newBufferedWriter(getGenesOutputFile());
                     SequenceWriter writer = JacksonUtils.getDefaultObjectMapper()
//                    .writerWithDefaultPrettyPrinter()
                             .writer(new MinimalPrettyPrinter("\n"))
                             .forType(KnockoutByGene.class)
                             .writeValues(bufferedWriter)) {
                    CellBaseUtils cellBaseUtils = getVariantStorageManager().getCellBaseUtils(getStudy(), getToken());
                    for (File file : FileUtils.listFiles(getScratchDir().toFile(), new RegexFileFilter("knockout.gene..*.json.gz"), null)) {
                        KnockoutByGene knockoutByGene = reader.readValue(org.opencb.commons.utils.FileUtils.newBufferedReader(file.toPath()));
                        QueryOptions queryOptions = new QueryOptions(QueryOptions.EXCLUDE, "transcripts,annotation.expression");
                        Gene gene = cellBaseUtils.getCellBaseClient().getGeneClient()
                                .search(new Query("name", knockoutByGene.getName()), queryOptions).firstResult();
                        knockoutByGene.setId(gene.getId());
                        knockoutByGene.setName(gene.getName());
                        knockoutByGene.setChromosome(gene.getChromosome());
                        knockoutByGene.setStart(gene.getStart());
                        knockoutByGene.setEnd(gene.getEnd());
                        knockoutByGene.setStrand(gene.getStrand());
                        knockoutByGene.setBiotype(gene.getBiotype());
                        knockoutByGene.setAnnotation(gene.getAnnotation());
                        for (KnockoutByGene.KnockoutIndividual individual : knockoutByGene.getIndividuals()) {
                            if (individual.getId() == null) {
                                String individualId = sampleIdToIndividualIdMap.get(individual.getSampleId());
                                if (individualId == null) {
                                    Individual individualObj = catalogManager
                                            .getIndividualManager()
                                            .search(getStudy(),
                                                    new Query(IndividualDBAdaptor.QueryParams.SAMPLES.key(), individual.getSampleId()),
                                                    new QueryOptions(QueryOptions.INCLUDE, IndividualDBAdaptor.QueryParams.ID.key()),
                                                    getToken())
                                            .first();
                                    if (individualObj == null) {
                                        // Empty value for missing individuals
                                        individualId = "";
                                    } else {
                                        individualId = individualObj.getId();
                                    }
                                    sampleIdToIndividualIdMap.put(individual.getSampleId(), individualId);
                                }
                                if (!individualId.isEmpty()) {
                                    individual.setId(individualId);
                                }
                            }
                        }
                        writer.write(knockoutByGene);
                    }
                }
            }
        });
    }

    private Path getIndividualsOutputFile() {
        return getOutDir().resolve(KNOCKOUT_INDIVIDUALS_JSON);
    }

    private Path getGenesOutputFile() {
        return getOutDir().resolve(KNOCKOUT_GENES_JSON);
    }

}
