package org.opencb.opencga.storage.mongodb.variant.stats;

import org.apache.commons.lang.StringUtils;
import org.bson.Document;
import org.opencb.biodata.models.feature.Genotype;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.stats.VariantStats;
import org.opencb.biodata.tools.variant.stats.VariantStatsCalculator;
import org.opencb.commons.run.Task;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.metadata.models.CohortMetadata;
import org.opencb.opencga.storage.core.metadata.models.StudyMetadata;
import org.opencb.opencga.storage.core.variant.adaptors.GenotypeClass;
import org.opencb.opencga.storage.core.variant.stats.VariantStatsWrapper;
import org.opencb.opencga.storage.mongodb.variant.converters.AbstractDocumentConverter;
import org.opencb.opencga.storage.mongodb.variant.converters.DocumentToStudyVariantEntryConverter;
import org.opencb.opencga.storage.mongodb.variant.converters.DocumentToVariantConverter;

import java.util.*;
import java.util.stream.Collectors;

import static org.opencb.opencga.storage.mongodb.variant.MongoDBVariantStorageOptions.DEFAULT_GENOTYPE;

/**
 * Created on 18/04/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class MongoDBVariantStatsCalculator extends AbstractDocumentConverter implements Task<Document, VariantStatsWrapper> {

    private StudyMetadata studyMetadata;

    private final Map<Integer, CohortMetadata> cohorts;
    private final Map<Integer, Set<Integer>> filesInCohortMap;
    private String unknownGenotype;
    private String defaultGenotype;
    private final DocumentToVariantConverter variantConverter;
    private final boolean multiAllelic;

    public MongoDBVariantStatsCalculator(StudyMetadata studyMetadata, Collection<CohortMetadata> cohorts, String unknownGenotype,
                                         boolean multiAllelic) {
        this.studyMetadata = studyMetadata;
        this.cohorts = cohorts.stream().collect(Collectors.toMap(CohortMetadata::getId, c -> c));
        this.unknownGenotype = unknownGenotype;
        this.filesInCohortMap = new HashMap<>();
        variantConverter = new DocumentToVariantConverter();
        this.multiAllelic = multiAllelic;
        init(studyMetadata);
    }

    public MongoDBVariantStatsCalculator(VariantStorageMetadataManager metadataManager, StudyMetadata studyMetadata,
                                         List<Integer> cohorts, String unknownGenotype, boolean multiAllelic) {
        this.studyMetadata = studyMetadata;
        this.cohorts = new HashMap<>();
        this.filesInCohortMap = new HashMap<>();
        this.variantConverter = new DocumentToVariantConverter();
        for (Object cohort : cohorts) {
            CohortMetadata cohortMetadata = metadataManager.getCohortMetadata(studyMetadata.getId(), cohort);
            this.cohorts.put(cohortMetadata.getId(), cohortMetadata);
        }
        this.multiAllelic = multiAllelic;
        this.unknownGenotype = unknownGenotype;
        init(studyMetadata);
    }

    public void init(StudyMetadata studyMetadata) {
        for (CohortMetadata cohortMetadata : this.cohorts.values()) {
            this.filesInCohortMap.put(cohortMetadata.getId(), new HashSet<>(cohortMetadata.getFiles()));
        }
        List<String> defaultGenotypes = studyMetadata.getAttributes().getAsStringList(DEFAULT_GENOTYPE.key());

        defaultGenotype = defaultGenotypes.isEmpty() ? null : defaultGenotypes.get(0);
        if (GenotypeClass.UNKNOWN_GENOTYPE.equals(defaultGenotype)) {
            defaultGenotype = this.unknownGenotype;
        }
    }

    @Override
    public List<VariantStatsWrapper> apply(List<Document> batch) throws Exception {
        List<VariantStatsWrapper> result = new ArrayList<>(batch.size());

        for (Document document : batch) {
            result.add(apply(document));
        }
        return result;
    }

    public VariantStatsWrapper apply(Document document) {
        Variant variant = variantConverter.convertToDataModelType(document);
        VariantStatsWrapper statsWrapper = null;

        List<Document> studies = getList(document, DocumentToVariantConverter.STUDIES_FIELD);
        for (Document study : studies) {
            Integer sid = study.getInteger(DocumentToStudyVariantEntryConverter.STUDYID_FIELD);
            if (studyMetadata.getId() == sid) {
                statsWrapper = calculateStats(variant, study);
                break;
            }
        }
        return statsWrapper;
    }

    public VariantStatsWrapper calculateStats(Variant variant, Document study) {
        VariantStatsWrapper statsWrapper = new VariantStatsWrapper(variant, new HashMap<>(cohorts.size()));

        List<Document> files = study.getList(DocumentToStudyVariantEntryConverter.FILES_FIELD, Document.class);
        Document gt = study.get(DocumentToStudyVariantEntryConverter.GENOTYPES_FIELD, Document.class);

        // Make a Set from the lists of genotypes for fast indexOf
        Map<String, Set<Integer>> gtsMap = new HashMap<>(gt.size());
        for (Map.Entry<String, Object> entry : gt.entrySet()) {
            gtsMap.put(entry.getKey(), new HashSet<>((Collection) entry.getValue()));
        }

        for (CohortMetadata cohort : cohorts.values()) {
            Set<Integer> filesInCohort = this.filesInCohortMap.get(cohort.getId());
            Map<String, Integer> gtStrCount = new HashMap<>();

            int unknownGenotypes = cohort.getSamples().size();
            for (Integer sampleId : cohort.getSamples()) {
                for (Map.Entry<String, Set<Integer>> entry : gtsMap.entrySet()) {
                    String gtStr = entry.getKey();
                    // If any ?/? is present in the DB, must be read as "unknownGenotype", usually "./."
                    if (GenotypeClass.UNKNOWN_GENOTYPE.equals(gtStr)) {
                        gtStr = unknownGenotype;
                    }
                    if ((entry.getValue()).contains(sampleId)) {
                        addGt(gtStrCount, gtStr, 1);
                        unknownGenotypes--;
                        break;
                    }
                }
            }

            // All the samples not present in any gt list must count as "defaultGenotype", usually "0/0"
            addGt(gtStrCount, defaultGenotype, unknownGenotypes);

            Map<Genotype, Integer> gtCountMap = new HashMap<>(gtStrCount.size());
            gtStrCount.forEach((str, count) -> gtCountMap.compute(new Genotype(str),
                    (key, value) -> value == null ? count : value + count));

            VariantStats stats = VariantStatsCalculator.calculate(variant, gtCountMap, multiAllelic);

            int numFilterFiles = 0;
            int numQualFiles = 0;
            double qualitySum = 0;
            for (Document file : files) {
                Integer fileId = file.getInteger(DocumentToStudyVariantEntryConverter.FILEID_FIELD);
                if (filesInCohort.contains(fileId)) {
                    Document attributes = file.get(DocumentToStudyVariantEntryConverter.ATTRIBUTES_FIELD, Document.class);
                    String filter = attributes.getString(StudyEntry.FILTER);
                    // Ensure missing filters are counted
                    if (StringUtils.isEmpty(filter)) {
                        filter = ".";
                    }
                    VariantStatsCalculator.addFileFilter(filter, stats.getFilterCount());
                    numFilterFiles++;

                    Object qual = attributes.get(StudyEntry.QUAL);
                    if (qual instanceof Double) {
                        qualitySum += ((Double) qual);
                        numQualFiles++;
                    }
                }
            }

            VariantStatsCalculator.calculateFilterFreq(stats, numFilterFiles);
            stats.setQualityAvg(((float) (qualitySum / numQualFiles)));
            statsWrapper.getCohortStats().put(cohort.getName(), stats);
        }

        return statsWrapper;
    }


    private void addGt(Map<String, Integer> gtStrCount, String gt, int num) {
        gtStrCount.merge(gt, num, Integer::sum);
    }
}
