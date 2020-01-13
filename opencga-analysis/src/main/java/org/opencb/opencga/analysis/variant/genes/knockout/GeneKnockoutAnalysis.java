package org.opencb.opencga.analysis.variant.genes.knockout;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.models.clinical.interpretation.DiseasePanel;
import org.opencb.biodata.models.core.Gene;
import org.opencb.biodata.models.core.Transcript;
import org.opencb.cellbase.core.api.GeneDBAdaptor;
import org.opencb.cellbase.core.variant.annotation.VariantAnnotationUtils;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.tools.OpenCgaToolScopeStudy;
import org.opencb.opencga.catalog.db.api.IndividualDBAdaptor;
import org.opencb.opencga.core.annotations.Tool;
import org.opencb.opencga.core.exception.ToolException;
import org.opencb.opencga.core.models.Family;
import org.opencb.opencga.core.models.Panel;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.storage.core.metadata.models.Trio;
import org.opencb.opencga.storage.core.utils.CellBaseUtils;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryUtils;

import java.util.*;
import java.util.function.Predicate;
import java.util.stream.Collectors;

@Tool(id= GeneKnockoutAnalysis.ID, description = GeneKnockoutAnalysis.DESCRIPTION, resource = Enums.Resource.VARIANT)
public class GeneKnockoutAnalysis extends OpenCgaToolScopeStudy {
    public static final String ID = "gene-knockout";
    public static final String DESCRIPTION = "";

    private GeneKnockoutAnalysisParams analysisParams = new GeneKnockoutAnalysisParams();
    private String studyFqn;

    @Override
    protected List<String> getSteps() {
        return Arrays.asList("list-genes", "list-families", getId());
    }

    @Override
    protected void check() throws Exception {
        analysisParams.updateParams(params);
        studyFqn = getStudyFqn();

        if (StringUtils.isEmpty(analysisParams.getBiotype())) {
            if (CollectionUtils.isEmpty(analysisParams.getGene()) && CollectionUtils.isEmpty(analysisParams.getPanel())) {
                analysisParams.setBiotype(VariantAnnotationUtils.PROTEIN_CODING);
            }
        }
//        else {
//            if (CollectionUtils.isNotEmpty(analysisParams.getGene()) || CollectionUtils.isNotEmpty(analysisParams.getPanel())) {
//                throw new ToolException("Unable to combine parameters 'gene' and 'panel' with 'biotype'");
//            }
//        }

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
            for (String sample : analysisParams.getSample()) {
                Query familyQuery = new Query(IndividualDBAdaptor.QueryParams.SAMPLES.key(), sample);
                Family family = getCatalogManager().getFamilyManager()
                        .search(studyFqn, familyQuery, new QueryOptions(), getToken()).first();
                if (family == null || StringUtils.isEmpty(family.getId())) {
                    continue;
                }
                List<List<String>> trios = variantStorageManager.getTriosFromFamily(studyFqn, family, true, getToken());
                for (List<String> trio : trios) {
                    if (trio.get(2).equals(sample)) {
                        triosMap.put(sample, new Trio(family.getId(), trio.get(0), trio.get(1), trio.get(2)));
                        break;
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
            getToolExecutor(GeneKnockoutAnalysisExecutor.class)
                    .setStudy(studyFqn)
                    .setSamples(analysisParams.getSample())
                    .setFileNamePattern(getOutDir().resolve("knockout_genes.{sample}.json").toString())
                    .setProteinCodingGenes(new HashSet<>(proteinCodingGenes))
                    .setOtherGenes(new HashSet<>(otherGenes))
                    .setBiotype(analysisParams.getBiotype())
                    .setFilter(analysisParams.getFilter())
                    .setQual(analysisParams.getQual())
                    .setCt(analysisParams.getConsequenceType())
                    .setTrios(triosMap)
                    .execute();
        });
    }

}
