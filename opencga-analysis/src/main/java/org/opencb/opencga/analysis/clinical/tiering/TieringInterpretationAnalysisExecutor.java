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

package org.opencb.opencga.analysis.clinical.tiering;

import htsjdk.variant.vcf.VCFConstants;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.models.clinical.ClinicalProperty;
import org.opencb.biodata.models.clinical.interpretation.ClinicalVariant;
import org.opencb.biodata.models.clinical.interpretation.exceptions.InterpretationAnalysisException;
import org.opencb.biodata.models.clinical.pedigree.Pedigree;
import org.opencb.biodata.models.core.Region;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.tools.clinical.tiering.TieringClinicalVariantCreator;
import org.opencb.biodata.tools.clinical.tiering.TieringConfiguration;
import org.opencb.biodata.tools.pedigree.ModeOfInheritance;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.clinical.ClinicalInterpretationAnalysisExecutor;
import org.opencb.opencga.analysis.clinical.ClinicalInterpretationManager;
import org.opencb.opencga.analysis.clinical.ClinicalUtils;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.FamilyManager;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.clinical.ClinicalAnalysis;
import org.opencb.opencga.core.models.individual.Individual;
import org.opencb.opencga.core.tools.OpenCgaToolExecutor;
import org.opencb.opencga.core.tools.annotations.ToolExecutor;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.core.variant.query.VariantQueryUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

import static org.opencb.biodata.models.clinical.ClinicalProperty.ModeOfInheritance.*;

@Deprecated
@ToolExecutor(id = "opencga-local",
        tool = TieringInterpretationAnalysisTool.ID,
        source = ToolExecutor.Source.STORAGE,
        framework = ToolExecutor.Framework.LOCAL)
public class TieringInterpretationAnalysisExecutor extends OpenCgaToolExecutor implements ClinicalInterpretationAnalysisExecutor {

    public static final String REGION = "REGION";
    private String study;
    private ClinicalAnalysis clinicalAnalysis;
    private TieringConfiguration tieringConfiguration;

    private String token;
    private ClinicalInterpretationManager clinicalInterpretationManager;

    private Logger logger = LoggerFactory.getLogger(this.getClass());

    @Override
    public void run() throws ToolException {
        token = getToken();
        clinicalInterpretationManager = getClinicalInterpretationManager();

        // Get proband
        Individual proband = ClinicalUtils.getProband(clinicalAnalysis);

        // Get pedigree
        Pedigree pedigree = FamilyManager.getPedigreeFromFamily(clinicalAnalysis.getFamily(), proband.getId());

        // Discard members from the pedigree that do not have any samples. If we don't do this, we will always assume
        ClinicalUtils.removeMembersWithoutSamples(pedigree, clinicalAnalysis.getFamily());

        // Get the map of individual - sample id and update proband information (to be able to navigate to the parents and their
        // samples easily)
        Map<String, String> sampleMap = ClinicalUtils.getSampleMap(clinicalAnalysis, proband);

        Map<ClinicalProperty.ModeOfInheritance, List<Variant>> resultMap = new EnumMap<>(ClinicalProperty.ModeOfInheritance.class);
        Map<String, List<Variant>> chVariantMap = new HashMap<>();

        List<Variant> regionVariants = new ArrayList<>();

        // Execute queries
        executeQueries(pedigree, sampleMap, resultMap, chVariantMap, regionVariants);

        List<Variant> variantList = new ArrayList<>();
        Map<String, List<ClinicalProperty.ModeOfInheritance>> variantMoIMap = new HashMap<>();

        for (Map.Entry<ClinicalProperty.ModeOfInheritance, List<Variant>> entry : resultMap.entrySet()) {
            logger.info("MoI: {}, num. variants = {}", entry.getKey().name(), entry.getValue().size());
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

        // Tiering clinical variant creator
        String assembly;
        try {
            assembly = clinicalInterpretationManager.getAssembly(study, token);
        } catch (CatalogException e) {
            throw new ToolException(e);
        }
        TieringClinicalVariantCreator creator = new TieringClinicalVariantCreator(new ArrayList<>(clinicalAnalysis.getPanels()),
                clinicalAnalysis.getDisorder(), null, ClinicalProperty.Penetrance.valueOf(tieringConfiguration.getPenetrance()),
                assembly);

        // Primary findings
        List<ClinicalVariant> primaryFindings = new ArrayList<>();
        try {
            primaryFindings.addAll(creator.create(variantList, variantMoIMap));
        } catch (InterpretationAnalysisException e) {
            throw new ToolException(e.getMessage(), e);
        }

        // Add compound heterozygous variants
        try {
            List<ClinicalVariant> chVariants = ClinicalUtils.getCompoundHeterozygousClinicalVariants(chVariantMap, creator);
            logger.info("MoI: {}, num. variants = {}", COMPOUND_HETEROZYGOUS, chVariants.size());
            if (CollectionUtils.isNotEmpty(chVariants)) {
                primaryFindings.addAll(chVariants);
            }
        } catch (InterpretationAnalysisException e) {
            throw new ToolException("Error retrieving " + COMPOUND_HETEROZYGOUS + " variants", e);
        }
        primaryFindings = creator.mergeClinicalVariants(primaryFindings);

        // Write primary findings
        ClinicalUtils.writeClinicalVariants(primaryFindings, Paths.get(getOutDir() + "/primary-findings.json"));
    }

    private void executeQueries(Pedigree pedigree, Map<String, String> sampleMap,
                                Map<ClinicalProperty.ModeOfInheritance, List<Variant>> resultMap,
                                Map<String, List<Variant>> chVariantMap,
                                List<Variant> regionVariants) throws ToolException {
        ExecutorService threadPool = Executors.newFixedThreadPool(9);

        List<Future<Boolean>> futureList = new ArrayList<>(9);
        futureList.add(threadPool.submit(getNamedThread(AUTOSOMAL_DOMINANT.name(),
                () -> query(pedigree, sampleMap, AUTOSOMAL_DOMINANT, resultMap))));
        futureList.add(threadPool.submit(getNamedThread(X_LINKED_DOMINANT.name(),
                () -> query(pedigree, sampleMap, X_LINKED_DOMINANT, resultMap))));
        futureList.add(threadPool.submit(getNamedThread(Y_LINKED.name(),
                () -> query(pedigree, sampleMap, Y_LINKED, resultMap))));
        futureList.add(threadPool.submit(getNamedThread(AUTOSOMAL_RECESSIVE.name(),
                () -> query(pedigree, sampleMap, AUTOSOMAL_RECESSIVE, resultMap))));
        futureList.add(threadPool.submit(getNamedThread(X_LINKED_RECESSIVE.name(),
                () -> query(pedigree, sampleMap, X_LINKED_RECESSIVE, resultMap))));
        futureList.add(threadPool.submit(getNamedThread(MITOCHONDRIAL.name(),
                () -> query(pedigree, sampleMap, MITOCHONDRIAL, resultMap))));
        futureList.add(threadPool.submit(getNamedThread(COMPOUND_HETEROZYGOUS.name(), () -> compoundHeterozygous(chVariantMap))));
        futureList.add(threadPool.submit(getNamedThread(DE_NOVO.name(), () -> deNovo(resultMap))));
        futureList.add(threadPool.submit(getNamedThread(REGION, () -> region(sampleMap.values(), regionVariants))));
        threadPool.shutdown();

        try {
            if (!threadPool.awaitTermination(2, TimeUnit.MINUTES)) {
                futureList.forEach(f -> f.cancel(true));
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new ToolException("Error launching threads when executing the Tiering interpretation analysis", e);
        }
    }

    private <T> Callable<T> getNamedThread(String name, Callable<T> c) {
        String parentThreadName = Thread.currentThread().getName();
        return () -> {
            Thread.currentThread().setName(parentThreadName + "-" + name);
            return c.call();
        };
    }

    private Boolean compoundHeterozygous(Map<String, List<Variant>> resultMap) throws ToolException {
        Query query = new Query();
        addQueryFilters(COMPOUND_HETEROZYGOUS.name(), query);
        try {
            Map<String, List<Variant>> chVariants = clinicalInterpretationManager.getCompoundHeterozigousVariants(clinicalAnalysis.getId(),
                    study, query, QueryOptions.empty(), token);
            resultMap.putAll(chVariants);
        } catch (Exception e) {
            return false;
        }

        return true;
    }

    private Boolean deNovo(Map<ClinicalProperty.ModeOfInheritance, List<Variant>> resultMap) throws ToolException {
        Query query = new Query();
        addQueryFilters(DE_NOVO.name(), query);
        try {
            List<Variant> deNovoVariants = clinicalInterpretationManager.getDeNovoVariants(clinicalAnalysis.getId(), study, query,
                    QueryOptions.empty(), token);
            resultMap.put(DE_NOVO, deNovoVariants);
        } catch (Exception e) {
            return false;
        }

        return true;
    }

    private Boolean region(Collection<String> samples, List<Variant> result) throws ToolException {
        // Get assembly
        String assembly;
        try {
            assembly = clinicalInterpretationManager.getAssembly(study, token);
        } catch (CatalogException e) {
            throw new ToolException(e);
        }

        // Prepare disease panels from the clinical analysis
        if (CollectionUtils.isEmpty(clinicalAnalysis.getPanels())) {
            logger.warn("No disease panels found in the clinical analysis, so skipping region queries.");
            return true;
        }
        List<Region> regions = clinicalAnalysis.getPanels().stream()
                .filter(panel -> panel.getRegions() != null)
                .flatMap(panel -> panel.getRegions().stream())
                .flatMap(regionPanel -> regionPanel.getCoordinates().stream())
                .filter(coordinate -> coordinate.getAssembly().equalsIgnoreCase(assembly))
                .map(coordinate -> Region.parseRegion(coordinate.getLocation()))
                .collect(Collectors.toList());

        if (regions.isEmpty()) {
            logger.info("No regions found in the disease panels for assembly {}, so skipping region queries.", assembly);
            return true;
        }

        Query query = new Query();
        addQueryFilters(REGION, query);
        query.append(VariantQueryParam.REGION.key(), regions)
                .append(VariantQueryParam.INCLUDE_GENOTYPE.key(), true)
                .append(VariantQueryParam.STUDY.key(), study)
                .append(VariantQueryParam.FILTER.key(), VCFConstants.PASSES_FILTERS_v4)
                .append(VariantQueryParam.UNKNOWN_GENOTYPE.key(), "./.")
                .append(VariantQueryParam.SAMPLE.key(), samples);

        try {
            result.addAll(clinicalInterpretationManager.getVariantStorageManager().get(query, QueryOptions.empty(), token)
                    .getResults());
        } catch (CatalogException | IOException | StorageEngineException e) {
            logger.warn("Error querying region variants (returning false): {}", e.getMessage(), e);
            return false;
        }

        return true;
    }

    private Boolean query(Pedigree pedigree, Map<String, String> sampleMap, ClinicalProperty.ModeOfInheritance moi,
                          Map<ClinicalProperty.ModeOfInheritance, List<Variant>> resultMap) throws ToolException {
        Query query = new Query();
        Map<String, List<String>> genotypes;
        ClinicalProperty.Penetrance penetrance = ClinicalProperty.Penetrance.valueOf(tieringConfiguration.getPenetrance());

        switch (moi) {
            case AUTOSOMAL_DOMINANT:
                addQueryFilters(AUTOSOMAL_DOMINANT.name(), query);
                genotypes = ModeOfInheritance.dominant(pedigree, clinicalAnalysis.getDisorder(), penetrance);
                break;
            case Y_LINKED:
                addQueryFilters(Y_LINKED.name(), query);
                genotypes = ModeOfInheritance.yLinked(pedigree, clinicalAnalysis.getDisorder(), penetrance);
                break;
            case X_LINKED_DOMINANT:
                addQueryFilters(X_LINKED_DOMINANT.name(), query);
                genotypes = ModeOfInheritance.xLinked(pedigree, clinicalAnalysis.getDisorder(), true, penetrance);
                break;
            case AUTOSOMAL_RECESSIVE:
                addQueryFilters(AUTOSOMAL_RECESSIVE.name(), query);
                genotypes = ModeOfInheritance.recessive(pedigree, clinicalAnalysis.getDisorder(), penetrance);
                break;
            case X_LINKED_RECESSIVE:
                addQueryFilters(X_LINKED_RECESSIVE.name(), query);
                genotypes = ModeOfInheritance.xLinked(pedigree, clinicalAnalysis.getDisorder(), false, penetrance);
                break;
            case MITOCHONDRIAL:
                addQueryFilters(MITOCHONDRIAL.name(), query);
                genotypes = ModeOfInheritance.mitochondrial(pedigree, clinicalAnalysis.getDisorder(), penetrance);
                filterOutHealthyGenotypes(genotypes);
                break;
            default:
                return false;
        }

        // Check if there are genotypes to query
        if (ModeOfInheritance.isEmptyMapOfGenotypes(genotypes)) {
            logger.warn("No genotypes to query for MoI {}, so this query will be skipped", moi.name());
            return false;
        }
        addGenotypeFilter(genotypes, sampleMap, query);

        // Add common filters
        query.append(VariantQueryParam.INCLUDE_GENOTYPE.key(), true)
                .append(VariantQueryParam.STUDY.key(), study);

        // Execute query and save the returned variants in the result map
        try {
            resultMap.put(moi, clinicalInterpretationManager.getVariantStorageManager().get(query, QueryOptions.empty(), token)
                    .getResults());
        } catch (CatalogException | StorageEngineException | IOException e) {
            return false;
        }
        return true;
    }

    private void filterOutHealthyGenotypes(Map<String, List<String>> genotypes) {
        List<String> filterOutKeys = new ArrayList<>();
        for (Map.Entry<String, List<String>> entry : genotypes.entrySet()) {
            List<String> gts = genotypes.get(entry.getKey());
            boolean filterOut = true;
            for (String gt : gts) {
                if (gt.contains("1")) {
                    filterOut = false;
                }
            }
            if (filterOut) {
                filterOutKeys.add(entry.getKey());
            }
        }
        for (String filterOutKey : filterOutKeys) {
            genotypes.remove(filterOutKey);
        }
    }

    private void addGenotypeFilter(Map<String, List<String>> genotypes, Map<String, String> sampleMap, Query query) {
        String genotypeString = StringUtils.join(genotypes.entrySet().stream()
                .filter(entry -> sampleMap.containsKey(entry.getKey()))
                .filter(entry -> CollectionUtils.isNotEmpty(entry.getValue()))
                .map(entry -> sampleMap.get(entry.getKey()) + ":" + StringUtils.join(entry.getValue(), VariantQueryUtils.OR))
                .collect(Collectors.toList()), ";");
        if (StringUtils.isNotEmpty(genotypeString)) {
            query.put(VariantQueryParam.GENOTYPE.key(), genotypeString);
        }
    }

    private void addQueryFilters(String key, Query query) throws ToolException {
        Object queryConfig = tieringConfiguration.getQueries().get(key);
        if (queryConfig instanceof Map) {
            query.appendAll((Map<String, Object>) queryConfig);
        } else {
            throw new ToolException("Unexpected " + key + " query configuration: " + queryConfig);
        }
    }
    //-------------------------------------------------------------------------
    // Getters and setters
    //-------------------------------------------------------------------------

    public String getStudy() {
        return study;
    }

    public TieringInterpretationAnalysisExecutor setStudy(String study) {
        this.study = study;
        return this;
    }

    public ClinicalAnalysis getClinicalAnalysis() {
        return clinicalAnalysis;
    }

    public TieringInterpretationAnalysisExecutor setClinicalAnalysis(ClinicalAnalysis clinicalAnalysis) {
        this.clinicalAnalysis = clinicalAnalysis;
        return this;
    }

    public TieringConfiguration getTieringConfiguration() {
        return tieringConfiguration;
    }

    public TieringInterpretationAnalysisExecutor setTieringConfiguration(TieringConfiguration tieringConfiguration) {
        this.tieringConfiguration = tieringConfiguration;
        return this;
    }
}
