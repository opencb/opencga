package org.opencb.opencga.analysis.clinical.interpretation;

import htsjdk.variant.vcf.VCFConstants;
import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.models.clinical.interpretation.ClinicalProperty;
import org.opencb.biodata.models.clinical.interpretation.DiseasePanel;
import org.opencb.biodata.models.clinical.interpretation.ReportedVariant;
import org.opencb.biodata.models.clinical.interpretation.exceptions.InterpretationAnalysisException;
import org.opencb.biodata.models.clinical.pedigree.Pedigree;
import org.opencb.biodata.models.commons.Disorder;
import org.opencb.biodata.models.core.Region;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.tools.clinical.TieringReportedVariantCreator;
import org.opencb.biodata.tools.pedigree.ModeOfInheritance;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.utils.ListUtils;
import org.opencb.opencga.analysis.clinical.ClinicalInterpretationManager;
import org.opencb.opencga.analysis.clinical.ClinicalUtils;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.FamilyManager;
import org.opencb.opencga.core.tools.OpenCgaToolExecutor;
import org.opencb.opencga.core.annotations.ToolExecutor;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.clinical.ClinicalAnalysis;
import org.opencb.opencga.core.models.individual.Individual;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryUtils;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

import static org.opencb.biodata.models.clinical.interpretation.ClinicalProperty.ModeOfInheritance.*;

@ToolExecutor(id = "opencga-local",
        tool = TieringInterpretationAnalysisExecutor.ID,
        source = ToolExecutor.Source.STORAGE,
        framework = ToolExecutor.Framework.LOCAL)
public class TieringInterpretationAnalysisExecutor extends OpenCgaToolExecutor implements ClinicalInterpretationAnalysisExecutor {

    public final static String ID = "tiering-interpretation";

    private String studyId;
    private String clinicalAnalysisId;
    private List<DiseasePanel> diseasePanels;
    private ClinicalProperty.Penetrance penetrance;
    private TieringInterpretationConfiguration config;

    private String sessionId;
    private ClinicalInterpretationManager clinicalInterpretationManager;

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

//    public void setup(String clinicalAnalysisId, String studyId, List<DiseasePanel> diseasePanels, ClinicalProperty.Penetrance penetrance, Path outDir, ObjectMap executorParams,
//                      TieringInterpretationConfiguration config) throws AnalysisException {
//        super.setup(executorParams, outDir);
//        this.clinicalAnalysisId = clinicalAnalysisId;
//        this.studyId = studyId;
//        this.diseasePanels = diseasePanels;
//        this.penetrance = penetrance;
//        this.config = config;
//
//        // Sanity check
//        sessionId = executorParams.getString(SESSION_ID, "");
//        if (StringUtils.isEmpty(sessionId)) {
//            throw new AnalysisException("Missing executor parameter: " + SESSION_ID);
//        }
//        clinicalInterpretationManager = (ClinicalInterpretationManager) executorParams.getOrDefault(CLINICAL_INTERPRETATION_MANAGER, null);
//        if (clinicalInterpretationManager == null) {
//            throw new AnalysisException("Missing executor parameter: " + CLINICAL_INTERPRETATION_MANAGER);
//        }
//    }

    @Override
    public void run() throws ToolException {
        sessionId = getToken();
        clinicalInterpretationManager = getClinicalInterpretationManager();

        // Get assembly
        String assembly;
        try {
            assembly = clinicalInterpretationManager.getAssembly(studyId, sessionId);
        } catch (CatalogException e) {
            throw new ToolException("Error retrieving assembly", e);
        }

        // Get and check clinical analysis and proband
        ClinicalAnalysis clinicalAnalysis;
        try {
            clinicalAnalysis = clinicalInterpretationManager.getClinicalAnalysis(studyId, clinicalAnalysisId, sessionId);
        } catch (CatalogException e) {
            throw new ToolException("Error getting clinical analysis", e);
        }
        Individual proband = ClinicalUtils.getProband(clinicalAnalysis);

        // Get pedigree
        Pedigree pedigree = FamilyManager.getPedigreeFromFamily(clinicalAnalysis.getFamily(), proband.getId());

        // Discard members from the pedigree that do not have any samples. If we don't do this, we will always assume
        ClinicalUtils.removeMembersWithoutSamples(pedigree, clinicalAnalysis.getFamily());

        // Get the map of individual - sample id and update proband information (to be able to navigate to the parents and their
        // samples easily)
        Map<String, String> sampleMap = ClinicalUtils.getSampleMap(clinicalAnalysis, proband);

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

        try {
            threadPool.awaitTermination(2, TimeUnit.MINUTES);
            if (!threadPool.isTerminated()) {
                for (Future<Boolean> future : futureList) {
                    future.cancel(true);
                }
            }
        } catch (InterruptedException e) {
            throw new ToolException("Error launching threads when execuging the Tiering interpretation analysis", e);
        }

        List<Variant> variantList = new ArrayList<>();
        Map<String, List<ClinicalProperty.ModeOfInheritance>> variantMoIMap = new HashMap<>();

        for (Map.Entry<ClinicalProperty.ModeOfInheritance, List<Variant>> entry : resultMap.entrySet()) {
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
        TieringReportedVariantCreator creator;
        List<ReportedVariant> primaryFindings;
        try {
            creator = new TieringReportedVariantCreator(diseasePanels,
                    clinicalInterpretationManager.getRoleInCancerManager().getRoleInCancer(),
                    clinicalInterpretationManager.getActionableVariantManager().getActionableVariants(assembly),
                    clinicalAnalysis.getDisorder(), null, penetrance, assembly);
            primaryFindings = creator.create(variantList, variantMoIMap);
        } catch (InterpretationAnalysisException | IOException e) {
            throw new ToolException(e.getMessage(), e);
        }

        // Add compound heterozyous variants
        try {
            primaryFindings.addAll(ClinicalUtils.getCompoundHeterozygousReportedVariants(chVariantMap, creator));
        } catch (InterpretationAnalysisException e) {
            throw new ToolException("Error retrieving compound heterozygous variants", e);
        }
        primaryFindings = creator.mergeReportedVariants(primaryFindings);

        // Write primary findings
        ClinicalUtils.writeReportedVariants(primaryFindings, Paths.get(outDir + "/primary-findings.json"));

        // Secondary findings, if clinical consent is TRUE
        List<ReportedVariant> secondaryFindings = null;
        try {
            secondaryFindings = clinicalInterpretationManager.getSecondaryFindings(clinicalAnalysis,
                    new ArrayList<>(sampleMap.keySet()), studyId, creator, sessionId);
        } catch (CatalogException | IOException | StorageEngineException e) {
            throw new ToolException("Error retrieving secondary findings variants", e);
        }

        // Write primary findings
        ClinicalUtils.writeReportedVariants(secondaryFindings, Paths.get(outDir + "/secondary-findings.json"));
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
        try {
            Map<String, List<Variant>> chVariants = clinicalInterpretationManager.getCompoundHeterozigousVariants(clinicalAnalysisId,
                    studyId, query, QueryOptions.empty(), sessionId);
            resultMap.putAll(chVariants);
        } catch (Exception e) {
            return false;
        }

        return true;
    }

    private Boolean deNovo(Map<ClinicalProperty.ModeOfInheritance, List<Variant>> resultMap) {
        Query query = new Query(dominantQuery);
        try {
            List<Variant> deNovoVariants = clinicalInterpretationManager.getDeNovoVariants(clinicalAnalysisId, studyId, query,
                    QueryOptions.empty(), sessionId);
            resultMap.put(DE_NOVO, deNovoVariants);
        } catch (Exception e) {
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
            return true;
        }

        Query query = new Query()
                .append(VariantQueryParam.REGION.key(), regions)
                .append(VariantQueryParam.INCLUDE_GENOTYPE.key(), true)
                .append(VariantQueryParam.STUDY.key(), studyId)
                .append(VariantQueryParam.FILTER.key(), VCFConstants.PASSES_FILTERS_v4)
                .append(VariantQueryParam.UNKNOWN_GENOTYPE.key(), "./.")
                .append(VariantQueryParam.SAMPLE.key(), samples);

        try {
            result.addAll(clinicalInterpretationManager.getVariantStorageManager().get(query, QueryOptions.empty(), sessionId)
                    .getResults());
        } catch (Exception e) {
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
                return false;
        }
        query.append(VariantQueryParam.INCLUDE_GENOTYPE.key(), true)
                .append(VariantQueryParam.STUDY.key(), studyId)
                .append(VariantQueryParam.FILTER.key(), VCFConstants.PASSES_FILTERS_v4)
                .append(VariantQueryParam.UNKNOWN_GENOTYPE.key(), "./.");

        if (ModeOfInheritance.isEmptyMapOfGenotypes(genotypes)) {
            return false;
        }
        addGenotypeFilter(genotypes, sampleMap, query);

        try {
            resultMap.put(moi, clinicalInterpretationManager.getVariantStorageManager().get(query, QueryOptions.empty(), sessionId)
                    .getResults());
        } catch (CatalogException | StorageEngineException | IOException e) {
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

    private void addGenotypeFilter(Map<String, List<String>> genotypes, Map<String, String> sampleMap, Query query) {
        String genotypeString = StringUtils.join(genotypes.entrySet().stream()
                .filter(entry -> sampleMap.containsKey(entry.getKey()))
                .filter(entry -> ListUtils.isNotEmpty(entry.getValue()))
                .map(entry -> sampleMap.get(entry.getKey()) + ":" + StringUtils.join(entry.getValue(), VariantQueryUtils.OR))
                .collect(Collectors.toList()), ";");
        if (StringUtils.isNotEmpty(genotypeString)) {
            query.put(VariantQueryParam.GENOTYPE.key(), genotypeString);
        }
    }

    public String getStudyId() {
        return studyId;
    }

    public TieringInterpretationAnalysisExecutor setStudyId(String studyId) {
        this.studyId = studyId;
        return this;
    }

    public String getClinicalAnalysisId() {
        return clinicalAnalysisId;
    }

    public TieringInterpretationAnalysisExecutor setClinicalAnalysisId(String clinicalAnalysisId) {
        this.clinicalAnalysisId = clinicalAnalysisId;
        return this;
    }

    public List<DiseasePanel> getDiseasePanels() {
        return diseasePanels;
    }

    public TieringInterpretationAnalysisExecutor setDiseasePanels(List<DiseasePanel> diseasePanels) {
        this.diseasePanels = diseasePanels;
        return this;
    }

    public ClinicalProperty.Penetrance getPenetrance() {
        return penetrance;
    }

    public TieringInterpretationAnalysisExecutor setPenetrance(ClinicalProperty.Penetrance penetrance) {
        this.penetrance = penetrance;
        return this;
    }

    public TieringInterpretationConfiguration getConfig() {
        return config;
    }

    public TieringInterpretationAnalysisExecutor setConfig(TieringInterpretationConfiguration config) {
        this.config = config;
        return this;
    }
}
