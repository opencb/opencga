package org.opencb.opencga.analysis.clinical;

import htsjdk.variant.vcf.VCFConstants;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.opencb.biodata.models.clinical.interpretation.ClinicalProperty;
import org.opencb.biodata.models.clinical.pedigree.Pedigree;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.tools.pedigree.ModeOfInheritance;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.AnalysisResult;
import org.opencb.opencga.catalog.managers.FamilyManager;
import org.opencb.opencga.core.common.JacksonUtils;
import org.opencb.opencga.core.models.ClinicalAnalysis;
import org.opencb.opencga.core.models.Individual;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryUtils;
import org.opencb.opencga.storage.core.variant.adaptors.iterators.VariantDBIterator;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class DeNovoAnalysis extends FamilyAnalysis<List<Variant>> {

    private Query query;

    private static Query defaultQuery;

    static {
        defaultQuery = new Query()
                .append(VariantQueryParam.ANNOT_POPULATION_ALTERNATE_FREQUENCY.key(), "1kG_phase3:AFR<0.002;1kG_phase3:AMR<0.002;"
                        + "1kG_phase3:EAS<0.002;1kG_phase3:EUR<0.002;1kG_phase3:SAS<0.002;GNOMAD_EXOMES:AFR<0.001;GNOMAD_EXOMES:AMR<0.001;"
                        + "GNOMAD_EXOMES:EAS<0.001;GNOMAD_EXOMES:FIN<0.001;GNOMAD_EXOMES:NFE<0.001;GNOMAD_EXOMES:ASJ<0.001;"
                        + "GNOMAD_EXOMES:OTH<0.002")
                .append(VariantQueryParam.STATS_MAF.key(), "ALL<0.001")
                .append(VariantQueryParam.ANNOT_BIOTYPE.key(), ModeOfInheritance.proteinCoding)
                .append(VariantQueryParam.ANNOT_CONSEQUENCE_TYPE.key(), ModeOfInheritance.extendedLof);
    }

    public DeNovoAnalysis(String clinicalAnalysisId, List<String> diseasePanelIds, Query query,
                                        Map<String, ClinicalProperty.RoleInCancer> roleInCancer,
                                        Map<String, List<String>> actionableVariants, ObjectMap config, String studyStr, String opencgaHome,
                                        String token) {
        super(clinicalAnalysisId, diseasePanelIds, roleInCancer, actionableVariants, config, studyStr, opencgaHome, token);
        this.query = new Query(defaultQuery);
        this.query.append(VariantQueryParam.INCLUDE_GENOTYPE.key(), true)
                .append(VariantQueryParam.STUDY.key(), studyStr)
                .append(VariantQueryParam.FILTER.key(), VCFConstants.PASSES_FILTERS_v4)
                .append(VariantQueryParam.UNKNOWN_GENOTYPE.key(), "./.");

        if (MapUtils.isNotEmpty(query)) {
            this.query.putAll(query);
        }

    }

    @Override
    public AnalysisResult<List<Variant>> execute() throws Exception {
        StopWatch watcher = StopWatch.createStarted();

        // Get and check clinical analysis and proband
        ClinicalAnalysis clinicalAnalysis = getClinicalAnalysis();
        Individual proband = getProband(clinicalAnalysis);

        // Get pedigree
        Pedigree pedigree = FamilyManager.getPedigreeFromFamily(clinicalAnalysis.getFamily(), proband.getId());

        // Get the map of individual - sample id and update proband information (to be able to navigate to the parents and their
        // samples easily)
        Map<String, String> sampleMap = getSampleMap(clinicalAnalysis, proband);

        Map<String, List<String>> genotypeMap = ModeOfInheritance.deNovo(pedigree);

        List<String> samples = new ArrayList<>();
        List<String> genotypeList = new ArrayList<>();

        for (Map.Entry<String, List<String>> entry : genotypeMap.entrySet()) {
            if (sampleMap.containsKey(entry.getKey())) {
//                samples.add(sampleMap.get(entry.getKey()));
                genotypeList.add(sampleMap.get(entry.getKey()) + ":" + StringUtils.join(entry.getValue(), VariantQueryUtils.OR));
            }
        }
        if (genotypeList.isEmpty()) {
            logger.error("No genotypes found");
            return null;
        }
        query.put(VariantQueryParam.GENOTYPE.key(), StringUtils.join(genotypeList, ";"));

        samples.add(sampleMap.get(proband.getId()));
        samples.add(sampleMap.get(proband.getMother().getId()));
        samples.add(sampleMap.get(proband.getFather().getId()));
        query.put(VariantQueryParam.INCLUDE_SAMPLE.key(), samples);

        logger.debug("De novo query: {}", JacksonUtils.getDefaultObjectMapper().writer().writeValueAsString(query));
        VariantDBIterator iterator = variantStorageManager.iterator(query, QueryOptions.empty(), token);

        logger.debug("De novo Clinical Analysis: {}", JacksonUtils.getDefaultObjectMapper().writer().writeValueAsString(clinicalAnalysis));
        logger.debug("De novo Pedigree: {}", JacksonUtils.getDefaultObjectMapper().writer().writeValueAsString(pedigree));
        logger.debug("De novo Pedigree proband: {}", JacksonUtils.getDefaultObjectMapper().writer()
                .writeValueAsString(pedigree.getProband()));
        logger.debug("De novo Genotype: {}", JacksonUtils.getDefaultObjectMapper().writer().writeValueAsString(genotypeMap));
        logger.debug("De novo Proband: {}", JacksonUtils.getDefaultObjectMapper().writer().writeValueAsString(proband));
        logger.debug("De novo Sample map: {}", JacksonUtils.getDefaultObjectMapper().writer().writeValueAsString(sampleMap));
        logger.debug("De novo samples: {}", JacksonUtils.getDefaultObjectMapper().writer().writeValueAsString(samples));

        List<Variant> variants = ModeOfInheritance.deNovo(iterator, 0, 1, 2);

        logger.debug("De novo time: {}", watcher.getTime());
        return new AnalysisResult<>(variants, Math.toIntExact(watcher.getTime()), null);
    }

}
