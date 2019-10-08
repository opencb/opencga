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
import org.opencb.biodata.models.clinical.interpretation.ClinicalProperty.RoleInCancer;
import org.opencb.biodata.models.clinical.interpretation.exceptions.InterpretationAnalysisException;
import org.opencb.biodata.models.clinical.pedigree.Pedigree;
import org.opencb.biodata.models.commons.Disorder;
import org.opencb.biodata.models.commons.Software;
import org.opencb.biodata.models.core.Region;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.tools.clinical.TieringReportedVariantCreator;
import org.opencb.biodata.tools.pedigree.ModeOfInheritance;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.commons.utils.ListUtils;
import org.opencb.opencga.analysis.AnalysisResult;
import org.opencb.opencga.analysis.clinical.ClinicalUtils;
import org.opencb.opencga.analysis.clinical.CompoundHeterozygousAnalysis;
import org.opencb.opencga.analysis.clinical.DeNovoAnalysis;
import org.opencb.opencga.analysis.exceptions.AnalysisException;
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
import java.util.stream.Collectors;

import static org.opencb.biodata.models.clinical.interpretation.ClinicalProperty.ModeOfInheritance.*;

public class TieringInterpretationAnalysis extends FamilyInterpretationAnalysis {

    protected ClinicalProperty.Penetrance penetrance;

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
    protected void exec() throws org.opencb.oskar.analysis.exceptions.AnalysisException {
    }

    public InterpretationResult compute() throws Exception {
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

        Map<ClinicalProperty.ModeOfInheritance, List<Variant>> resultMap = new HashMap<>();
        Map<String, List<Variant>> chVariantMap = new HashMap<>();

        List<Variant> regionVariants = new ArrayList<>();

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
        futureList.add(threadPool.submit(getNamedThread(COMPOUND_HETEROZYGOUS.name(), () -> compoundHeterozygous(chVariantMap))));
        futureList.add(threadPool.submit(getNamedThread(DE_NOVO.name(), () -> deNovo(resultMap))));
        futureList.add(threadPool.submit(getNamedThread("REGION", () -> region(diseasePanels, sampleMap.values(),
                assembly, regionVariants))));
        threadPool.shutdown();

        threadPool.awaitTermination(2, TimeUnit.MINUTES);
        if (!threadPool.isTerminated()) {
            for (Future<Boolean> future : futureList) {
                future.cancel(true);
            }
        }

        List<Variant> variantList = new ArrayList<>();
        Map<String, List<ClinicalProperty.ModeOfInheritance>> variantMoIMap = new HashMap<>();

        for (Map.Entry<ClinicalProperty.ModeOfInheritance, List<Variant>> entry : resultMap.entrySet()) {
            logger.debug("MOI: {}; variant size: {}; variant ids: {}", entry.getKey(), entry.getValue().size(),
                    entry.getValue().stream().map(Variant::toString).collect(Collectors.joining(",")));

            for (Variant variant : entry.getValue()) {
                if (!variantMoIMap.containsKey(variant.getId())) {
                    variantMoIMap.put(variant.getId(), new ArrayList<>());
                    variantList.add(variant);
                }
                variantMoIMap.get(variant.getId()).add(entry.getKey());
            }
        }

        // Add region variants to variantList and variantMoIMap
        for (Variant variant : regionVariants) {
            if (!variantMoIMap.containsKey(variant.getId())) {
                variantMoIMap.put(variant.getId(), new ArrayList<>());
                variantList.add(variant);
            }
            // We add these variants with the ModeOfInheritance UNKNOWN
            variantMoIMap.get(variant.getId()).add(UNKNOWN);
        }

        // Primary findings,
        List<ReportedVariant> primaryFindings;
        TieringReportedVariantCreator creator = new TieringReportedVariantCreator(diseasePanels, roleInCancerManager.getRoleInCancer(),
                actionableVariantManager.getActionableVariants(assembly), clinicalAnalysis.getDisorder(), null, penetrance,
                assembly);
        try {
            primaryFindings = creator.create(variantList, variantMoIMap);
        } catch (InterpretationAnalysisException e) {
            throw new AnalysisException(e.getMessage(), e);
        }

        // Add compound heterozyous variants
        primaryFindings.addAll(getCompoundHeterozygousReportedVariants(chVariantMap, creator));
        primaryFindings = creator.mergeReportedVariants(primaryFindings);


        // Secondary findings, if clinical consent is TRUE
        List<ReportedVariant> secondaryFindings = getSecondaryFindings(clinicalAnalysis, new ArrayList<>(sampleMap.keySet()), creator);

        logger.debug("Reported variant size: {}", primaryFindings.size());

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

    private Boolean compoundHeterozygous(Map<String, List<Variant>> resultMap) {
        Query query = new Query(recessiveQuery);
        CompoundHeterozygousAnalysis analysis = new CompoundHeterozygousAnalysis(clinicalAnalysisId, studyId, query, options, opencgaHome,
                sessionId);
        try {
            AnalysisResult<Map<String, List<Variant>>> execute = analysis.compute();
            if (MapUtils.isNotEmpty(execute.getResult())) {
                resultMap.putAll(execute.getResult());
            }
        } catch (Exception e) {
            logger.error("{}", e.getMessage(), e);
            return false;
        }

        return true;
    }

    private Boolean deNovo(Map<ClinicalProperty.ModeOfInheritance, List<Variant>> resultMap) {
        Query query = new Query(dominantQuery);
        DeNovoAnalysis analysis = new DeNovoAnalysis(clinicalAnalysisId, studyId, query, options, opencgaHome, sessionId);
        try {
            AnalysisResult<List<Variant>> execute = analysis.compute();
            if (ListUtils.isNotEmpty(execute.getResult())) {
                resultMap.put(DE_NOVO, execute.getResult());
            }
        } catch (Exception e) {
            logger.error("{}", e.getMessage(), e);
            return false;
        }

        return true;
    }

    private Boolean region(List<DiseasePanel> diseasePanelList, Collection<String> samples, String assembly, List<Variant> result) {
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
            result.addAll(variantStorageManager.get(query, QueryOptions.empty(), sessionId).getResult());
        } catch (Exception e) {
            logger.error("{}", e.getMessage(), e);
            return false;
        }

        return true;
    }

    private Boolean query(Pedigree pedigree, Disorder disorder, Map<String, String> sampleMap, ClinicalProperty.ModeOfInheritance moi,
                          Map<ClinicalProperty.ModeOfInheritance, List<Variant>> resultMap) {
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
            resultMap.put(moi, variantStorageManager.get(query, QueryOptions.empty(), sessionId).getResult());
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
