package org.opencb.opencga.analysis.variant.knockout;

import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.RegexFileFilter;
import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.models.clinical.interpretation.DiseasePanel;
import org.opencb.biodata.models.core.Gene;
import org.opencb.biodata.models.core.Transcript;
import org.opencb.cellbase.core.api.GeneDBAdaptor;
import org.opencb.cellbase.core.variant.annotation.VariantAnnotationUtils;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.tools.OpenCgaToolScopeStudy;
import org.opencb.opencga.analysis.variant.knockout.result.KnockoutByGene;
import org.opencb.opencga.analysis.variant.knockout.result.KnockoutBySample;
import org.opencb.opencga.catalog.db.api.IndividualDBAdaptor;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.common.JacksonUtils;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.family.Family;
import org.opencb.opencga.core.models.individual.Individual;
import org.opencb.opencga.core.models.panel.Panel;
import org.opencb.opencga.core.models.sample.Sample;
import org.opencb.opencga.core.models.variant.KnockoutAnalysisParams;
import org.opencb.opencga.core.tools.annotations.Tool;
import org.opencb.opencga.storage.core.metadata.models.Trio;
import org.opencb.opencga.storage.core.utils.CellBaseUtils;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryUtils;

import java.io.File;
import java.util.*;
import java.util.function.Predicate;
import java.util.stream.Collectors;

@Tool(id= KnockoutAnalysis.ID, description = KnockoutAnalysis.DESCRIPTION, resource = Enums.Resource.VARIANT)
public class KnockoutAnalysis extends OpenCgaToolScopeStudy {
    public static final String ID = "knockout";
    public static final String DESCRIPTION = "Obtains the list of knocked out genes for each sample.";

    private KnockoutAnalysisParams analysisParams = new KnockoutAnalysisParams();
    private String studyFqn;

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
        analysisParams.updateParams(params);
        studyFqn = getStudyFqn();
        executorParams.put("executionMethod", params.getString("executionMethod", "auto"));

        if (CollectionUtils.isEmpty(analysisParams.getSample())
                || analysisParams.getSample().size() == 1 && analysisParams.getSample().get(0).equals(ParamConstants.ALL)) {
            analysisParams.setSample(new ArrayList<>(getVariantStorageManager().getIndexedSamples(studyFqn, getToken())));
        }

        if (StringUtils.isEmpty(analysisParams.getBiotype())) {
            if (CollectionUtils.isEmpty(analysisParams.getGene()) && CollectionUtils.isEmpty(analysisParams.getPanel())) {
                analysisParams.setBiotype(VariantAnnotationUtils.PROTEIN_CODING);
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
            CellBaseUtils cellBaseUtils = getVariantStorageManager().getCellBaseUtils(studyFqn, token);
            QueryOptions queryOptions = new QueryOptions(QueryOptions.INCLUDE, "id,name,chromosome,start,end,biotype,transcripts.biotype");

            boolean allProteinCoding = false;
            if (CollectionUtils.isEmpty(analysisParams.getGene()) && CollectionUtils.isEmpty(analysisParams.getPanel())) {
                // No genes or panel given.
                // Get genes by biotype
                List<String> biotypes = new ArrayList<>(Arrays.asList(analysisParams.getBiotype().split(",")));
                if (biotypes.contains(VariantAnnotationUtils.PROTEIN_CODING)) {
                    allProteinCoding = true;
                    proteinCodingGenes.add(VariantQueryUtils.ALL);
                    biotypes.remove(VariantAnnotationUtils.PROTEIN_CODING);
                }
                if (!biotypes.isEmpty()) {
                    Query query = new Query(GeneDBAdaptor.QueryParams.TRANSCRIPT_BIOTYPE.key(), String.join(",", biotypes));
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
                            .get(studyFqn, analysisParams.getPanel(), new QueryOptions(), getToken())
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
                    if (biotypes.contains(VariantAnnotationUtils.PROTEIN_CODING)) {
                        proteinCodingGenes.add(gene.getName());
                    }
                    if (biotypes.size() == 1 && !biotypes.contains(VariantAnnotationUtils.PROTEIN_CODING) || biotypes.size() > 1) {
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
                    .search(studyFqn, familyQuery, new QueryOptions(), getToken()).getResults()) {
                if (family == null || StringUtils.isEmpty(family.getId())) {
                    continue;
                }
                List<List<String>> trios = variantStorageManager.getTriosFromFamily(studyFqn, family, true, getToken());
                for (List<String> trio : trios) {
                    String child = trio.get(2);
                    if (analysisParams.getSample().contains(child)) {
                        triosMap.put(child, new Trio(family.getId(), trio.get(0), trio.get(1), child));
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
            setUpStorageEngineExecutor(studyFqn);
//            MappingIterator<String> objectMappingIterator = JacksonUtils.getDefaultObjectMapper().reader().readValues(genesFile);
//            List<String> genes = objectMappingIterator.readAll(new ArrayList<>());
            getToolExecutor(KnockoutAnalysisExecutor.class)
                    .setStudy(studyFqn)
                    .setSamples(analysisParams.getSample())
                    .setSampleFileNamePattern(getOutDir().resolve("knockout.sample.{sample}.json").toString())
                    .setGeneFileNamePattern(getOutDir().resolve("knockout.gene.{gene}.json").toString())
                    .setProteinCodingGenes(new HashSet<>(proteinCodingGenes))
                    .setOtherGenes(new HashSet<>(otherGenes))
                    .setBiotype(analysisParams.getBiotype())
                    .setFilter(analysisParams.getFilter())
                    .setQual(analysisParams.getQual())
                    .setCt(analysisParams.getConsequenceType())
                    .setTrios(triosMap)
                    .execute();
        });

        step("add-metadata-to-output-files", () -> {
            ObjectReader reader = JacksonUtils.getDefaultObjectMapper().readerFor(KnockoutBySample.class);
            ObjectWriter writer = JacksonUtils.getDefaultObjectMapper().writerWithDefaultPrettyPrinter().forType(KnockoutBySample.class);
            for (File file : FileUtils.listFiles(getOutDir().toFile(), new RegexFileFilter("knockout.sample..*.json"), null)) {
                KnockoutBySample knockoutBySample = reader.readValue(file);
                Sample sample = catalogManager
                        .getSampleManager()
                        .get(studyFqn, knockoutBySample.getSample().getId(), new QueryOptions(), getToken())
                        .first();
                sample.getAttributes().remove("OPENCGA_INDIVIDUAL");
                knockoutBySample.setSample(sample);
                if (StringUtils.isNotEmpty(sample.getIndividualId())) {
                    Individual individual = catalogManager
                            .getIndividualManager()
                            .get(studyFqn,
                                    sample.getIndividualId(),
                                    new QueryOptions(QueryOptions.EXCLUDE, IndividualDBAdaptor.QueryParams.SAMPLES.key()),
                                    getToken())
                            .first();
                    if (individual.getFather() != null && individual.getFather().getId() == null) {
                        individual.setFather(null);
                    }
                    if (individual.getMother() != null && individual.getMother().getId() == null) {
                        individual.setMother(null);
                    }
                    knockoutBySample.setIndividual(individual);
                }
                writer.writeValue(file, knockoutBySample);
            }


            reader = JacksonUtils.getDefaultObjectMapper().readerFor(KnockoutByGene.class);
            writer = JacksonUtils.getDefaultObjectMapper().writerWithDefaultPrettyPrinter().forType(KnockoutByGene.class);
            for (File file : FileUtils.listFiles(getOutDir().toFile(), new RegexFileFilter("knockout.gene..*.json"), null)) {
                KnockoutByGene knockoutByGene = reader.readValue(file);
                QueryOptions queryOptions = new QueryOptions(QueryOptions.EXCLUDE, "annotation.expression");
                Gene gene = getVariantStorageManager().getCellBaseUtils(studyFqn, getToken()).getCellBaseClient().getGeneClient()
                        .search(new Query(GeneDBAdaptor.QueryParams.NAME.key(), knockoutByGene.getName()), queryOptions).firstResult();
                knockoutByGene.setGene(gene);
                writer.writeValue(file, knockoutByGene);
            }
        });
    }

}
