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
import org.opencb.commons.utils.ListUtils;
import org.opencb.opencga.analysis.AnalysisResult;
import org.opencb.opencga.catalog.managers.FamilyManager;
import org.opencb.opencga.core.common.JacksonUtils;
import org.opencb.opencga.core.models.ClinicalAnalysis;
import org.opencb.opencga.core.models.Individual;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryUtils;
import org.opencb.opencga.storage.core.variant.adaptors.iterators.VariantDBIterator;
import org.opencb.oskar.analysis.exceptions.AnalysisException;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class CompoundHeterozygousAnalysis extends OpenCgaClinicalAnalysis {

    private Query query;

    private static Query defaultQuery;

    static {
        defaultQuery = new Query()
                .append(VariantQueryParam.ANNOT_BIOTYPE.key(), ModeOfInheritance.proteinCoding)
                .append(VariantQueryParam.ANNOT_CONSEQUENCE_TYPE.key(), ModeOfInheritance.extendedLof);
    }

    public CompoundHeterozygousAnalysis(String clinicalAnalysisId, String studyId, Query query, ObjectMap options, String opencgaHome,
                                        String sessionId) {
        super(clinicalAnalysisId, studyId, options, opencgaHome, sessionId);
        this.query = new Query(defaultQuery);
        this.query.append(VariantQueryParam.INCLUDE_GENOTYPE.key(), true)
                .append(VariantQueryParam.STUDY.key(), studyId)
                .append(VariantQueryParam.FILTER.key(), VCFConstants.PASSES_FILTERS_v4)
                .append(VariantQueryParam.UNKNOWN_GENOTYPE.key(), "./.");

        if (MapUtils.isNotEmpty(query)) {
            this.query.putAll(query);
        }
    }

    @Override
    protected void exec() throws AnalysisException {
    }

    public AnalysisResult<Map<String, List<Variant>>> compute() throws Exception {
        StopWatch watcher = StopWatch.createStarted();

        // Get and check clinical analysis and proband
        ClinicalAnalysis clinicalAnalysis = getClinicalAnalysis();
        Individual proband = getProband(clinicalAnalysis);

        // Get pedigree
        Pedigree pedigree = FamilyManager.getPedigreeFromFamily(clinicalAnalysis.getFamily(), proband.getId());

        // Discard members from the pedigree that do not have any samples. If we don't do this, we will always assume
        ClinicalUtils.removeMembersWithoutSamples(pedigree, clinicalAnalysis.getFamily());

        // Get the map of individual - sample id and update proband information (to be able to navigate to the parents and their
        // samples easily)
        Map<String, String> sampleMap = getSampleMap(clinicalAnalysis, proband);

        Map<String, List<String>> genotypeMap = ModeOfInheritance.compoundHeterozygous(pedigree);
        logger.debug("CH Clinical Analysis: {}", JacksonUtils.getDefaultObjectMapper().writer().writeValueAsString(clinicalAnalysis));
        logger.debug("CH Pedigree: {}", JacksonUtils.getDefaultObjectMapper().writer().writeValueAsString(pedigree));
        logger.debug("CH Pedigree proband: {}", JacksonUtils.getDefaultObjectMapper().writer().writeValueAsString(pedigree.getProband()));
        logger.debug("CH Genotype: {}", JacksonUtils.getDefaultObjectMapper().writer().writeValueAsString(genotypeMap));
        logger.debug("CH Proband: {}", JacksonUtils.getDefaultObjectMapper().writer().writeValueAsString(proband));
        logger.debug("CH Sample map: {}", JacksonUtils.getDefaultObjectMapper().writer().writeValueAsString(sampleMap));

        List<String> samples = new ArrayList<>();
        List<String> genotypeList = new ArrayList<>();

        for (Map.Entry<String, List<String>> entry : genotypeMap.entrySet()) {
            if (sampleMap.containsKey(entry.getKey())) {
                samples.add(sampleMap.get(entry.getKey()));
                genotypeList.add(sampleMap.get(entry.getKey()) + ":" + StringUtils.join(entry.getValue(), VariantQueryUtils.OR));
            }
        }

        int probandSampleIdx = samples.indexOf(proband.getSamples().get(0).getId());
        int fatherSampleIdx = -1;
        int motherSampleIdx = -1;
        if (proband.getFather() != null && ListUtils.isNotEmpty(proband.getFather().getSamples())) {
            fatherSampleIdx = samples.indexOf(proband.getFather().getSamples().get(0).getId());
        }
        if (proband.getMother() != null && ListUtils.isNotEmpty(proband.getMother().getSamples())) {
            motherSampleIdx = samples.indexOf(proband.getMother().getSamples().get(0).getId());
        }

        if (genotypeList.isEmpty()) {
            logger.error("No genotypes found");
            return null;
        }
        query.put(VariantQueryParam.GENOTYPE.key(), StringUtils.join(genotypeList, ";"));

        cleanQuery(query);

        logger.debug("CH Samples: {}", StringUtils.join(samples, ","));
        logger.debug("CH Proband idx: {}, mother idx: {}, father idx: {}", probandSampleIdx, motherSampleIdx, fatherSampleIdx);
        logger.debug("CH Query: {}", JacksonUtils.getDefaultObjectMapper().writer().writeValueAsString(query));
        VariantDBIterator iterator = variantStorageManager.iterator(query, QueryOptions.empty(), sessionId);
        Map<String, List<Variant>> variantMap =
                ModeOfInheritance.compoundHeterozygous(iterator, probandSampleIdx, motherSampleIdx, fatherSampleIdx);

        logger.debug("CH time: {}", watcher.getTime());
        return new AnalysisResult<>(variantMap, Math.toIntExact(watcher.getTime()), null);
    }

}
