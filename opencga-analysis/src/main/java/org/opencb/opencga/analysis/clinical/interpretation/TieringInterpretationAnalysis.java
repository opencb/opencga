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

package org.opencb.opencga.analysis.clinical.interpretation;

import htsjdk.variant.vcf.VCFConstants;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.opencb.biodata.models.clinical.interpretation.*;
import org.opencb.biodata.models.clinical.pedigree.Pedigree;
import org.opencb.biodata.models.commons.Disorder;
import org.opencb.biodata.models.commons.Software;
import org.opencb.biodata.models.core.Region;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.tools.clinical.ReportedVariantCreator;
import org.opencb.biodata.tools.pedigree.ModeOfInheritance;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.commons.utils.ListUtils;
import org.opencb.opencga.analysis.clinical.ClinicalUtils;
import org.opencb.opencga.analysis.clinical.CompoundHeterozygousAnalysis;
import org.opencb.opencga.analysis.clinical.DeNovoAnalysis;
import org.opencb.opencga.catalog.db.api.ProjectDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.FamilyManager;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.models.ClinicalAnalysis;
import org.opencb.opencga.core.models.Individual;
import org.opencb.opencga.core.models.Project;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;

import static org.opencb.biodata.models.clinical.interpretation.ClinicalProperty.ModeOfInheritance.*;

public class TieringInterpretationAnalysis extends FamilyInterpretationAnalysis {

    protected ClinicalProperty.Penetrance penetrance;

    private ObjectMap rvCreatorDependencies;
    private ObjectMap rvCreatorConfig;

    private final static Query dominantQuery;
    private final static Query recessiveQuery;
    private final static Query mitochondrialQuery;

    static {
        recessiveQuery = new Query()
                .append(VariantQueryParam.ANNOT_BIOTYPE.key(), ModeOfInheritance.proteinCoding)
                .append(VariantQueryParam.ANNOT_POPULATION_ALTERNATE_FREQUENCY.key(), "1kG_phase3:AFR<0.01;1kG_phase3:AMR<0.01;"
                        + "1kG_phase3:EAS<0.01;1kG_phase3:EUR<0.01;1kG_phase3:SAS<0.01;GNOMAD_EXOMES:AFR<0.01;GNOMAD_EXOMES:AMR<0.01;"
                        + "GNOMAD_EXOMES:EAS<0.01;GNOMAD_EXOMES:FIN<0.01;GNOMAD_EXOMES:NFE<0.01;GNOMAD_EXOMES:ASJ<0.01;"
                        + "GNOMAD_EXOMES:OTH<0.01")
                .append(VariantQueryParam.STATS_MAF.key(), "ALL<0.01")
                .append(VariantQueryParam.ANNOT_CONSEQUENCE_TYPE.key(), ModeOfInheritance.extendedLof);

        dominantQuery = new Query()
                .append(VariantQueryParam.ANNOT_BIOTYPE.key(), ModeOfInheritance.proteinCoding)
                .append(VariantQueryParam.ANNOT_POPULATION_ALTERNATE_FREQUENCY.key(), "1kG_phase3:AFR<0.002;1kG_phase3:AMR<0.002;"
                        + "1kG_phase3:EAS<0.002;1kG_phase3:EUR<0.002;1kG_phase3:SAS<0.002;GNOMAD_EXOMES:AFR<0.001;GNOMAD_EXOMES:AMR<0.001;"
                        + "GNOMAD_EXOMES:EAS<0.001;GNOMAD_EXOMES:FIN<0.001;GNOMAD_EXOMES:NFE<0.001;GNOMAD_EXOMES:ASJ<0.001;"
                        + "GNOMAD_EXOMES:OTH<0.002")
                .append(VariantQueryParam.STATS_MAF.key(), "ALL<0.001")
                .append(VariantQueryParam.ANNOT_CONSEQUENCE_TYPE.key(), ModeOfInheritance.extendedLof);

        mitochondrialQuery = new Query()
                .append(VariantQueryParam.ANNOT_BIOTYPE.key(), ModeOfInheritance.proteinCoding)
                .append(VariantQueryParam.ANNOT_POPULATION_ALTERNATE_FREQUENCY.key(), "1kG_phase3:AFR<0.002;1kG_phase3:AMR<0.002;"
                        + "1kG_phase3:EAS<0.002;1kG_phase3:EUR<0.002;1kG_phase3:SAS<0.002;")
                .append(VariantQueryParam.ANNOT_CONSEQUENCE_TYPE.key(), ModeOfInheritance.extendedLof)
                .append(VariantQueryParam.STATS_MAF.key(), "ALL<0.01")
                .append(VariantQueryParam.REGION.key(), "M,Mt,mt,m,MT");
    }


    public TieringInterpretationAnalysis(String clinicalAnalysisId, String studyId, List<String> diseasePanelIds,
                                         ClinicalProperty.Penetrance penetrance, ObjectMap options, String opencgaHome, String sessionId) {
        super(clinicalAnalysisId, studyId, diseasePanelIds, options, opencgaHome, sessionId);
        this.penetrance = penetrance;
    }

    @Override
    public InterpretationResult execute() throws Exception {
        StopWatch watcher = StopWatch.createStarted();

        Query query = new Query(ProjectDBAdaptor.QueryParams.STUDY.key(), studyId);
        QueryOptions options = new QueryOptions(QueryOptions.INCLUDE, ProjectDBAdaptor.QueryParams.ORGANISM.key());
        QueryResult<Project> projectQueryResult = catalogManager.getProjectManager().get(query, options, sessionId);

        if (projectQueryResult.getNumResults() != 1) {
            throw new CatalogException("Project not found for study " + studyId + ". Found " + projectQueryResult.getNumResults()
                    + " projects.");
        }
        String assembly = projectQueryResult.first().getOrganism().getAssembly();

        // Get and check clinical analysis and proband
        ClinicalAnalysis clinicalAnalysis = getClinicalAnalysis();
        Individual proband = getProband(clinicalAnalysis);

        // Get disease panels from IDs
        List<DiseasePanel> diseasePanels = getDiseasePanelsFromIds(diseasePanelIds);

        // Get pedigree
        Pedigree pedigree = FamilyManager.getPedigreeFromFamily(clinicalAnalysis.getFamily(), proband.getId());

        // Discard members from the pedigree that do not have any samples. If we don't do this, we will always assume
        ClinicalUtils.removeMembersWithoutSamples(pedigree, clinicalAnalysis.getFamily());

        // Get the map of individual - sample id and update proband information (to be able to navigate to the parents and their
        // samples easily)
        Map<String, String> sampleMap = getSampleMap(clinicalAnalysis, proband);

        Map<ClinicalProperty.ModeOfInheritance, List<ReportedVariant>> resultMap = new HashMap<>();


        rvCreatorDependencies = new ObjectMap();
        rvCreatorDependencies.put(org.opencb.biodata.tools.clinical.ClinicalUtils.ACTIONABLE_VARIANT_MANAGER, actionableVariantManager);
        rvCreatorDependencies.put(org.opencb.biodata.tools.clinical.ClinicalUtils.ROLE_IN_CANCER_MANAGER, roleInCancerManager);

        rvCreatorConfig = new ObjectMap();
        rvCreatorConfig.put(org.opencb.biodata.tools.clinical.ClinicalUtils.ASSEMBLY, assembly);
        rvCreatorConfig.put(org.opencb.biodata.tools.clinical.ClinicalUtils.DISORDER, clinicalAnalysis.getDisorder());
        rvCreatorConfig.put(org.opencb.biodata.tools.clinical.ClinicalUtils.PANELS, diseasePanels);
        rvCreatorConfig.put(org.opencb.biodata.tools.clinical.ClinicalUtils.PENETRANCE, penetrance);
        GelBasedTieringCalculator tieringCalculator = new GelBasedTieringCalculator();
        rvCreatorConfig.put(org.opencb.biodata.tools.clinical.ClinicalUtils.SET_TIER, true);
        rvCreatorConfig.put(org.opencb.biodata.tools.clinical.ClinicalUtils.TIERING, tieringCalculator);


        // Prepare thread pool
        ExecutorService threadPool = Executors.newFixedThreadPool(8);

        List<Future<Boolean>> futureList = new ArrayList<>(8);
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
        futureList.add(threadPool.submit(getNamedThread(MITOCHONDRIAL.name(),
                () -> query(pedigree, clinicalAnalysis.getDisorder(), sampleMap, MITOCHONDRIAL, resultMap))));
        futureList.add(threadPool.submit(getNamedThread(COMPOUND_HETEROZYGOUS.name(), () -> compoundHeterozygous(resultMap))));
        futureList.add(threadPool.submit(getNamedThread(DE_NOVO.name(), () -> deNovo(resultMap))));
        futureList.add(threadPool.submit(getNamedThread("REGION", () -> region(diseasePanels, sampleMap.values(),
                assembly, resultMap))));
        threadPool.shutdown();

        threadPool.awaitTermination(2, TimeUnit.MINUTES);
        if (!threadPool.isTerminated()) {
            for (Future<Boolean> future : futureList) {
                future.cancel(true);
            }
        }

        // And finally merge reported variants from each MoI to get the primary findings,
        // we are going to use a map, where key = variant ID, value = reported variant
        Map<String, ReportedVariant> reportedVariantMap = new HashMap<>();
        for (List<ReportedVariant> reportedVariants : resultMap.values()) {
            if (CollectionUtils.isNotEmpty(reportedVariants)) {
                for (ReportedVariant reportedVariant : reportedVariants) {
                    if (reportedVariantMap.containsKey(reportedVariant.toStringSimple())) {
                        // Reported variant already processed, we have to add the new reported events
                        reportedVariantMap.get(reportedVariant.getId()).getEvidences().addAll(reportedVariant.getEvidences());
                    } else {
                        // Reported variant not processed yet, add the reported variant to the map
                        reportedVariantMap.put(reportedVariant.toStringSimple(), reportedVariant);
                    }
                }
            }
        }

        List<ReportedVariant> primaryFindings = new ArrayList<>(reportedVariantMap.values());

        // Secondary findings
        ObjectMap config = new ObjectMap(rvCreatorConfig);
        config.put(org.opencb.biodata.tools.clinical.ClinicalUtils.MODE_OF_INHERITANCE, UNKNOWN);
        config.put(org.opencb.biodata.tools.clinical.ClinicalUtils.SET_TIER, false);
        ReportedVariantCreator creator = new ReportedVariantCreator(rvCreatorDependencies, config);
        List<ReportedVariant> secondaryFindings = getSecondaryFindings(clinicalAnalysis, new ArrayList<>(sampleMap.keySet()), creator);

        logger.debug("Primary findings, size: {}", primaryFindings.size());
        logger.debug("Secondary findings, size: {}", secondaryFindings.size());

        // Reported low coverage
        List<ReportedLowCoverage> reportedLowCoverages = new ArrayList<>();
        if (options.getBoolean("lowRegionCoverage", false)) {
            reportedLowCoverages = getReportedLowCoverage(clinicalAnalysis, diseasePanels);
        }

        // Create Interpretation
        Interpretation interpretation = new Interpretation()
                .setId("OpenCGA-Tiering-" + TimeUtils.getTime())
                .setAnalyst(getAnalyst(sessionId))
                .setClinicalAnalysisId(clinicalAnalysisId)
                .setCreationDate(TimeUtils.getTime())
                .setPanels(diseasePanels)
                .setFilters(null) //TODO
                .setSoftware(new Software().setName("Tiering"))
                .setPrimaryFindings(primaryFindings)
                .setSecondaryFindings(secondaryFindings)
                .setLowCoverageRegions(reportedLowCoverages);

        // Return interpretation result
        int numResults = CollectionUtils.isEmpty(primaryFindings) ? 0 : primaryFindings.size();
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

    private Boolean compoundHeterozygous(Map<ClinicalProperty.ModeOfInheritance, List<ReportedVariant>> resultMap) {
        Query query = new Query(recessiveQuery);
        CompoundHeterozygousAnalysis analysis = new CompoundHeterozygousAnalysis(clinicalAnalysisId, studyId, query, options, opencgaHome,
                sessionId);
        try {
            Map<String, List<Variant>> chVariantMap = analysis.execute().getResult();
            if (MapUtils.isNotEmpty(chVariantMap)) {

                ObjectMap config = new ObjectMap(rvCreatorConfig);
                config.put(org.opencb.biodata.tools.clinical.ClinicalUtils.MODE_OF_INHERITANCE, COMPOUND_HETEROZYGOUS);
                ReportedVariantCreator creator = new ReportedVariantCreator(rvCreatorDependencies, config);

                resultMap.put(COMPOUND_HETEROZYGOUS, getCompoundHeterozygousReportedVariants(chVariantMap, creator));
            }
        } catch (Exception e) {
            logger.error("{}", e.getMessage(), e);
            return false;
        }

        return true;
    }

    private Boolean deNovo(Map<ClinicalProperty.ModeOfInheritance, List<ReportedVariant>> resultMap) {
        Query query = new Query(dominantQuery);
        DeNovoAnalysis analysis = new DeNovoAnalysis(clinicalAnalysisId, studyId, query, options, opencgaHome, sessionId);
        try {
            List<Variant> variants = analysis.execute().getResult();

            if (ListUtils.isNotEmpty(variants)) {
                ObjectMap config = new ObjectMap(rvCreatorConfig);
                config.put(org.opencb.biodata.tools.clinical.ClinicalUtils.MODE_OF_INHERITANCE, DE_NOVO);
                ReportedVariantCreator creator = new ReportedVariantCreator(rvCreatorDependencies, config);

                resultMap.put(DE_NOVO, creator.createReportedVariants(variants));
            }
        } catch (Exception e) {
            logger.error("{}", e.getMessage(), e);
            return false;
        }

        return true;
    }

    private Boolean region(List<DiseasePanel> diseasePanelList, Collection<String> samples, String assembly,
                           Map<ClinicalProperty.ModeOfInheritance, List<ReportedVariant>> resultMap) {
        List<Region> regions = new ArrayList<>();
        if (diseasePanelList == null || diseasePanelList.isEmpty()) {
            return true;
        }

        for (DiseasePanel diseasePanel : diseasePanelList) {
            if (diseasePanel.getRegions() != null) {
                for (DiseasePanel.RegionPanel region : diseasePanel.getRegions()) {
                    for (DiseasePanel.Coordinate coordinate : region.getCoordinates()) {
                        if (coordinate.getAssembly().equalsIgnoreCase(assembly)) {
                            regions.add(Region.parseRegion(coordinate.getLocation()));
                        }
                    }
                }
            }
        }

        if (regions.isEmpty()) {
            logger.debug("Panel doesn't have any regions. Skipping region query.");
            return true;
        }

        Query query = new Query()
                .append(VariantQueryParam.REGION.key(), regions)
                .append(VariantQueryParam.INCLUDE_GENOTYPE.key(), true)
                .append(VariantQueryParam.STUDY.key(), studyId)
                .append(VariantQueryParam.FILTER.key(), VCFConstants.PASSES_FILTERS_v4)
                .append(VariantQueryParam.UNKNOWN_GENOTYPE.key(), "./.")
                .append(VariantQueryParam.SAMPLE.key(), samples);

        logger.debug("Region query: {}", query.safeToString());

        try {
            List<Variant> variants = variantStorageManager.get(query, QueryOptions.empty(), sessionId).getResult();

            if (ListUtils.isNotEmpty(variants)) {
                ObjectMap config = new ObjectMap(rvCreatorConfig);
                config.put(org.opencb.biodata.tools.clinical.ClinicalUtils.MODE_OF_INHERITANCE, UNKNOWN);
                ReportedVariantCreator creator = new ReportedVariantCreator(rvCreatorDependencies, config);

                // In the map<MoI, variant list>, we set moi to UNKNOWN
                resultMap.put(UNKNOWN, creator.createReportedVariants(variants));
            }
        } catch (Exception e) {
            logger.error("{}", e.getMessage(), e);
            return false;
        }

        return true;
    }

    private Boolean query(Pedigree pedigree, Disorder disorder, Map<String, String> sampleMap, ClinicalProperty.ModeOfInheritance moi,
                          Map<ClinicalProperty.ModeOfInheritance, List<ReportedVariant>> resultMap) {
        Query query;
        Map<String, List<String>> genotypes;
        switch (moi) {
            case MONOALLELIC:
                query = new Query(dominantQuery);
                genotypes = ModeOfInheritance.dominant(pedigree, disorder, penetrance);
                break;
            case YLINKED:
                query = new Query(dominantQuery)
                        .append(VariantQueryParam.REGION.key(), "Y");
                genotypes = ModeOfInheritance.yLinked(pedigree, disorder, penetrance);
                break;
            case XLINKED_MONOALLELIC:
                query = new Query(dominantQuery)
                        .append(VariantQueryParam.REGION.key(), "X");
                genotypes = ModeOfInheritance.xLinked(pedigree, disorder, true, penetrance);
                break;
            case BIALLELIC:
                query = new Query(recessiveQuery);
                genotypes = ModeOfInheritance.recessive(pedigree, disorder, penetrance);
                break;
            case XLINKED_BIALLELIC:
                query = new Query(recessiveQuery)
                        .append(VariantQueryParam.REGION.key(), "X");
                genotypes = ModeOfInheritance.xLinked(pedigree, disorder, false, penetrance);
                break;
            case MITOCHONDRIAL:
                query = new Query(mitochondrialQuery);
                genotypes = ModeOfInheritance.mitochondrial(pedigree, disorder, penetrance);
                filterOutHealthyGenotypes(genotypes);
                break;
            default:
                logger.error("Mode of inheritance not yet supported: {}", moi);
                return false;
        }
        query.append(VariantQueryParam.INCLUDE_GENOTYPE.key(), true)
                .append(VariantQueryParam.STUDY.key(), studyId)
                .append(VariantQueryParam.FILTER.key(), VCFConstants.PASSES_FILTERS_v4)
                .append(VariantQueryParam.UNKNOWN_GENOTYPE.key(), "./.");

        if (ModeOfInheritance.isEmptyMapOfGenotypes(genotypes)) {
            logger.warn("Map of genotypes is empty for {}", moi);
            return false;
        }
        addGenotypeFilter(genotypes, sampleMap, query);

        logger.debug("MoI: {}; Query: {}", moi, query.safeToString());
        try {
            // Variant query
            List<Variant> variants = variantStorageManager.get(query, QueryOptions.empty(), sessionId).getResult();

            // Create reported variant creator
            ObjectMap config = new ObjectMap(rvCreatorConfig);
            config.put(org.opencb.biodata.tools.clinical.ClinicalUtils.MODE_OF_INHERITANCE, moi);
            ReportedVariantCreator creator = new ReportedVariantCreator(rvCreatorDependencies, config);

            resultMap.put(moi, creator.createReportedVariants(variants));
        } catch (CatalogException | StorageEngineException | IOException e) {
            logger.error(e.getMessage(), e);
            return false;
        }
        return true;
    }

    private void filterOutHealthyGenotypes(Map<String, List<String>> genotypes) {
        List<String> filterOutKeys = new ArrayList<>();
        for (String key : genotypes.keySet()) {
            List<String> gts = genotypes.get(key);
            boolean filterOut = true;
            for (String gt : gts) {
                if (gt.contains("1")) {
                    filterOut = false;
                }
            }
            if (filterOut) {
                filterOutKeys.add(key);
            }
        }
        for (String filterOutKey : filterOutKeys) {
            genotypes.remove(filterOutKey);
        }
    }

}
