/*
 * Copyright 2015-2017 OpenCB
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

package org.opencb.opencga.analysis.clinical;

import htsjdk.variant.vcf.VCFConstants;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.opencb.biodata.models.clinical.interpretation.*;
import org.opencb.biodata.models.clinical.interpretation.ClinicalProperty.RoleInCancer;
import org.opencb.biodata.models.clinical.interpretation.exceptions.InterpretationAnalysisException;
import org.opencb.biodata.models.clinical.pedigree.Pedigree;
import org.opencb.biodata.models.commons.Disorder;
import org.opencb.biodata.models.commons.Software;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.tools.clinical.TieringReportedVariantCreator;
import org.opencb.biodata.tools.pedigree.ModeOfInheritance;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.exceptions.AnalysisException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.FamilyManager;
import org.opencb.opencga.core.common.JacksonUtils;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.models.ClinicalAnalysis;
import org.opencb.opencga.core.models.Individual;
import org.opencb.opencga.core.models.Panel;
import org.opencb.opencga.core.results.VariantQueryResult;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.stream.Collectors;

import static org.opencb.biodata.models.clinical.interpretation.ClinicalProperty.ModeOfInheritance.*;

public class TieringAnalysis extends FamilyAnalysis {

    private final static Query dominantQuery;
    private final static Query recessiveQuery;

    static {
        recessiveQuery = new Query()
                .append(VariantQueryParam.ANNOT_BIOTYPE.key(), "protein_coding,IG_C_gene,IG_D_gene,IG_J_gene,IG_V_gene,"
                        + "nonsense_mediated_decay,non_stop_decay,TR_C_gene,TR_D_gene,TR_J_gene,TR_V_gene")
                .append(VariantQueryParam.ANNOT_POPULATION_ALTERNATE_FREQUENCY.key(), "1kG_phase3:AFR<0.01;1kG_phase3:AMR<0.01;"
                        + "1kG_phase3:EAS<0.01;1kG_phase3:EUR<0.01;1kG_phase3:SAS<0.01;GNOMAD_EXOMES:AFR<0.01;GNOMAD_EXOMES:AMR<0.01;"
                        + "GNOMAD_EXOMES:EAS<0.01;GNOMAD_EXOMES:FIN<0.01;GNOMAD_EXOMES:NFE<0.01;GNOMAD_EXOMES:ASJ<0.01;"
                        + "GNOMAD_EXOMES:OTH<0.01")
                .append(VariantQueryParam.ANNOT_CONSEQUENCE_TYPE.key(), "SO:0001893,SO:0001574,SO:0001575,SO:0001587,SO:0001589,SO:0001578,"
                        + "SO:0001582,SO:0001889,SO:0001821,SO:0001822,SO:0001583,SO:0001630,SO:0001626");

        dominantQuery = new Query()
                .append(VariantQueryParam.ANNOT_BIOTYPE.key(), "protein_coding,IG_C_gene,IG_D_gene,IG_J_gene,IG_V_gene,"
                        + "nonsense_mediated_decay,non_stop_decay,TR_C_gene,TR_D_gene,TR_J_gene,TR_V_gene")
                .append(VariantQueryParam.ANNOT_POPULATION_ALTERNATE_FREQUENCY.key(), "1kG_phase3:AFR<0.002;1kG_phase3:AMR<0.002;"
                        + "1kG_phase3:EAS<0.002;1kG_phase3:EUR<0.002;1kG_phase3:SAS<0.002;GNOMAD_EXOMES:AFR<0.001;GNOMAD_EXOMES:AMR<0.001;"
                        + "GNOMAD_EXOMES:EAS<0.001;GNOMAD_EXOMES:FIN<0.001;GNOMAD_EXOMES:NFE<0.001;GNOMAD_EXOMES:ASJ<0.001;"
                        + "GNOMAD_EXOMES:OTH<0.002")
                .append(VariantQueryParam.ANNOT_CONSEQUENCE_TYPE.key(), "SO:0001893,SO:0001574,SO:0001575,SO:0001587,SO:0001589,SO:0001578,"
                        + "SO:0001582,SO:0001889,SO:0001821,SO:0001822,SO:0001583,SO:0001630,SO:0001626");
    }

    public TieringAnalysis(String clinicalAnalysisId, List<String> diseasePanelIds, String studyStr, Map<String, RoleInCancer> roleInCancer,
                           Map<String, List<String>> actionableVariants, ObjectMap options, String opencgaHome, String token) {
        super(clinicalAnalysisId, diseasePanelIds, roleInCancer, actionableVariants, options, studyStr, opencgaHome, token);
    }

    @Override
    public InterpretationResult execute() throws AnalysisException, InterruptedException {
        StopWatch watcher = StopWatch.createStarted();

        // Get and check clinical analysis and proband
        ClinicalAnalysis clinicalAnalysis = getClinicalAnalysis();
        Individual proband = getProband(clinicalAnalysis);

        // Get disease panels from IDs
        List<Panel> diseasePanels = getDiseasePanelsFromIds(diseasePanelIds);

        // Get pedigree
        Pedigree pedigree = FamilyManager.getPedigreeFromFamily(clinicalAnalysis.getFamily());

        // Get the map of individual - sample id and update proband information (to be able to navigate to the parents and their
        // samples easily)
        Map<String, String> sampleMap = getSampleMap(clinicalAnalysis, proband);

        Map<ClinicalProperty.ModeOfInheritance, VariantQueryResult<Variant>> resultMap = new HashMap<>();

        ExecutorService threadPool = Executors.newFixedThreadPool(5);

        List<Future<Boolean>> futureList = new ArrayList<>(5);
        futureList.add(threadPool.submit(getNamedThread(MONOALLELIC.name(),
                () -> query(pedigree, clinicalAnalysis.getDisorder(), sampleMap, MONOALLELIC, resultMap))));
        futureList.add(threadPool.submit(getNamedThread(XLINKED_MONOALLELIC.name(),
                () -> query(pedigree, clinicalAnalysis.getDisorder(), sampleMap, XLINKED_MONOALLELIC, resultMap))));
        futureList.add(threadPool.submit(getNamedThread(YLINKED.name(),
                () -> query(pedigree, clinicalAnalysis.getDisorder(), sampleMap, YLINKED, resultMap))));
        futureList.add(threadPool.submit(getNamedThread(BIALLELIC.name(),
                () -> query(pedigree, clinicalAnalysis.getDisorder(), sampleMap, BIALLELIC, resultMap))));
        futureList.add(threadPool.submit(getNamedThread(XLINKED_BIALLELIC.name(),
                () -> query(pedigree, clinicalAnalysis.getDisorder(), sampleMap, XLINKED_BIALLELIC, resultMap))));
        threadPool.shutdown();

        threadPool.awaitTermination(1, TimeUnit.MINUTES);
        if (!threadPool.isTerminated()) {
            for (Future<Boolean> future : futureList) {
                future.cancel(true);
            }
        }

        List<ReportedVariant> reportedVariants;
        List<Variant> variantList = new ArrayList<>();
        Map<String, List<ClinicalProperty.ModeOfInheritance>> variantMoIMap = new HashMap<>();

        for (Map.Entry<ClinicalProperty.ModeOfInheritance, VariantQueryResult<Variant>> entry : resultMap.entrySet()) {
            logger.debug("MOI: {}; variant size: {}; variant ids: {}", entry.getKey(), entry.getValue().getResult().size(),
                    entry.getValue().getResult().stream().map(Variant::toString).collect(Collectors.joining(",")));

            for (Variant variant : entry.getValue().getResult()) {
                if (!variantMoIMap.containsKey(variant.getId())) {
                    variantMoIMap.put(variant.getId(), new ArrayList<>());
                    variantList.add(variant);
                }
                variantMoIMap.get(variant.getId()).add(entry.getKey());
            }
        }

        // Create instance of reported variant creator to obtain reported variants
        List<DiseasePanel> biodataDiseasePanelList = diseasePanels.stream().map(Panel::getDiseasePanel).collect(Collectors.toList());
        TieringReportedVariantCreator creator = new TieringReportedVariantCreator(biodataDiseasePanelList, roleInCancer, actionableVariants,
                clinicalAnalysis.getDisorder(), null, ClinicalProperty.Penetrance.COMPLETE);
        try {
            reportedVariants = creator.create(variantList, variantMoIMap);
        } catch (InterpretationAnalysisException e) {
            throw new AnalysisException(e.getMessage(), e);
        }

        logger.debug("Variant size: {}", variantList.size());
        logger.debug("Reported variant size: {}", reportedVariants.size());

        // Reported low coverage
        List<ReportedLowCoverage> reportedLowCoverages = new ArrayList<>();
        if (config.getBoolean("lowRegionCoverage", false)) {
            reportedLowCoverages = getReportedLowCoverage(clinicalAnalysis, diseasePanels);
        }

        // Create Interpretation
        Interpretation interpretation = new Interpretation()
                .setId("OpenCGA-Tiering-" + TimeUtils.getTime())
                .setAnalyst(getAnalyst(token))
                .setClinicalAnalysisId(clinicalAnalysisId)
                .setCreationDate(TimeUtils.getTime())
                .setPanels(biodataDiseasePanelList)
                .setFilters(null) //TODO
                .setSoftware(new Software().setName("Tiering"))
                .setReportedVariants(reportedVariants)
                .setReportedLowCoverages(reportedLowCoverages);

        // Return interpretation result
        int numResults = CollectionUtils.isEmpty(reportedVariants) ? 0 : reportedVariants.size();
        return new InterpretationResult(
                interpretation,
                Math.toIntExact(watcher.getTime()),
                new HashMap<>(),
                Math.toIntExact(watcher.getTime()), // DB time
                numResults,
                numResults,
                "", // warning message
                ""); // error message
    }

    private <T> Callable<T> getNamedThread(String name, Callable<T> c) {
        String parentThreadName = Thread.currentThread().getName();
        return () -> {
            Thread.currentThread().setName(parentThreadName + "-" + name);
            return c.call();
        };
    }

    private Boolean query(Pedigree pedigree, Disorder disorder, Map<String, String> sampleMap, ClinicalProperty.ModeOfInheritance moi,
                          Map<ClinicalProperty.ModeOfInheritance, VariantQueryResult<Variant>> resultMap) {
        Query query;
        Map<String, List<String>> genotypes;
        switch (moi) {
            case MONOALLELIC:
                query = new Query(dominantQuery);
                genotypes = ModeOfInheritance.dominant(pedigree, disorder, false);
                break;
            case YLINKED:
                query = new Query(dominantQuery)
                        .append(VariantQueryParam.REGION.key(), "Y");
                genotypes = ModeOfInheritance.yLinked(pedigree, disorder);
                break;
            case XLINKED_MONOALLELIC:
                query = new Query(dominantQuery)
                        .append(VariantQueryParam.REGION.key(), "X");
                genotypes = ModeOfInheritance.xLinked(pedigree, disorder, true);
                break;
            case BIALLELIC:
                query = new Query(recessiveQuery);
                genotypes = ModeOfInheritance.recessive(pedigree, disorder, false);
                break;
            case XLINKED_BIALLELIC:
                query = new Query(recessiveQuery)
                        .append(VariantQueryParam.REGION.key(), "X");
                genotypes = ModeOfInheritance.xLinked(pedigree, disorder, false);
                break;
            default:
                logger.error("Mode of inheritance not yet supported: {}", moi);
                return false;
        }
        query.append(VariantQueryParam.INCLUDE_GENOTYPE.key(), true)
                .append(VariantQueryParam.STUDY.key(), studyStr)
                .append(VariantQueryParam.FILTER.key(), VCFConstants.PASSES_FILTERS_v4)
                .append(VariantQueryParam.UNKNOWN_GENOTYPE.key(), "./.");

        if (MapUtils.isEmpty(genotypes)) {
            logger.warn("Map of genotypes is empty for {}" + moi);
            return false;
        }
        putGenotypes(genotypes, sampleMap, query);

        logger.debug("MoI: {}; Query: {}", moi, query.safeToString());
        try {
            resultMap.put(moi, variantStorageManager.get(query, QueryOptions.empty(), token));
        } catch (CatalogException | StorageEngineException | IOException e) {
            logger.error(e.getMessage(), e);
            return false;
        }
        return true;
    }

    private void putGenotypes(Map<String, List<String>> genotypes, Map<String, String> sampleMap, Query query) {
        query.put(VariantQueryParam.GENOTYPE.key(),
                StringUtils.join(genotypes.entrySet().stream()
                        .filter(entry -> sampleMap.containsKey(entry.getKey()))
                        .map(entry -> sampleMap.get(entry.getKey()) + ":" + StringUtils.join(entry.getValue(), VariantQueryUtils.OR))
                        .collect(Collectors.toList()), ";"));
        try {
            logger.debug("Query: {}", JacksonUtils.getDefaultObjectMapper().writer().writeValueAsString(query));
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }

    }
}
