package org.opencb.opencga.storage.core.metadata;

import com.google.common.collect.BiMap;
import org.opencb.biodata.models.metadata.*;
import org.opencb.biodata.models.variant.metadata.VariantDatasetMetadata;
import org.opencb.biodata.models.variant.metadata.VariantFileHeader;
import org.opencb.biodata.models.variant.metadata.VariantFileMetadata;
import org.opencb.biodata.models.variant.metadata.VariantMetadata;
import org.opencb.biodata.tools.variant.VariantMetadataManager;
import org.opencb.opencga.core.common.GitRepositoryState;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.storage.core.variant.annotation.VariantAnnotationManager;

import java.util.*;

/**
 * Converts from VariantMetadata to Collection&lt;StudyConfiguration&gt; and vice versa
 *
 * Created on 02/08/17.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class VariantMetadataConverter {

    public VariantMetadataConverter() {
    }

    public VariantMetadata toVariantMetadata(Collection<StudyConfiguration> studyConfigurations) {
        return toVariantMetadata(studyConfigurations, null, null);
    }

    public VariantMetadata toVariantMetadata(Collection<StudyConfiguration> studyConfigurations,
                                             Map<Integer, List<Integer>> returnedSamples,
                                             Map<Integer, List<Integer>> returnedFiles) {
        List<VariantDatasetMetadata> datasets = new ArrayList<>();
        String specie = "hsapiens";
        String assembly = null;
        for (StudyConfiguration studyConfiguration : studyConfigurations) {
            VariantDatasetMetadata variantDatasetMetadata = toVariantDatasetMetadata(studyConfiguration,
                    returnedSamples == null ? null : returnedSamples.get(studyConfiguration.getStudyId()),
                    returnedFiles == null ? null : returnedFiles.get(studyConfiguration.getStudyId()));
            datasets.add(variantDatasetMetadata);
            if (studyConfiguration.getAttributes().containsKey(VariantAnnotationManager.SPECIES)) {
                specie = studyConfiguration.getAttributes().getString(VariantAnnotationManager.SPECIES);
            }
            if (studyConfiguration.getAttributes().containsKey(VariantAnnotationManager.ASSEMBLY)) {
                assembly = studyConfiguration.getAttributes().getString(VariantAnnotationManager.ASSEMBLY);
            }
        }

        Species species = Species.newBuilder()
                .setId(specie)
                .setAssembly(assembly)
                .build();
        return VariantMetadata.newBuilder()
//                .setDate(Date.from(Instant.now()).toString())
                .setDate(TimeUtils.getTime())
                .setDatasets(datasets)
                .setVersion(GitRepositoryState.get().getDescribeShort())
                .setSpecies(species)
                .build();

    }

    public VariantDatasetMetadata toVariantDatasetMetadata(StudyConfiguration studyConfiguration) {
        return toVariantDatasetMetadata(studyConfiguration, null, null);
    }

    public VariantDatasetMetadata toVariantDatasetMetadata(StudyConfiguration studyConfiguration,
                                                           List<Integer> returnedSamples,
                                                           List<Integer> returnedFiles) {

        List<VariantFileMetadata> fileMetadata = new ArrayList<>(studyConfiguration.getIndexedFiles().size());

        for (Integer fileId : studyConfiguration.getIndexedFiles()) {
            if (returnedFiles != null && !returnedFiles.contains(fileId)) {
                continue;
            }

            LinkedHashSet<Integer> sampleIds = studyConfiguration.getSamplesInFiles().get(fileId);

            List<String> sampleNames = toSampleNames(studyConfiguration, sampleIds);
            fileMetadata.add(VariantFileMetadata.newBuilder()
                    .setId(fileId.toString())
                    .setAlias(studyConfiguration.getFileIds().inverse().get(fileId))
                    .setSampleIds(sampleNames)
                    .build());
        }

        ArrayList<Individual> individuals = new ArrayList<>();

        Collection<String> sampleNames;
        if (returnedSamples == null) {
            sampleNames = StudyConfiguration.getSortedIndexedSamplesPosition(studyConfiguration).keySet();
        } else {
            sampleNames = toSampleNames(studyConfiguration, returnedSamples);
        }
        for (String sampleName : sampleNames) {
            individuals.add(createIndividual(sampleName));
        }

        ArrayList<Cohort> cohorts = new ArrayList<>();
        for (Integer cohortId : studyConfiguration.getCalculatedStats()) {
            cohorts.add(getCohort(studyConfiguration, cohortId));
        }
        for (Integer cohortId : studyConfiguration.getInvalidStats()) {
            cohorts.add(getCohort(studyConfiguration, cohortId));
        }

        Map<String, String> attributes = new HashMap<>(studyConfiguration.getAttributes().size());
        for (String key : studyConfiguration.getAttributes().keySet()) {
            attributes.put(key, studyConfiguration.getAttributes().getString(key));
        }

        // Header from the StudyConfiguration stores variable attributes (where the value changes for every file) as unknown values.
        // We don't want to export those values at the aggregated header.
        // Copy the header and remove unknown attributes
        VariantFileHeader studyHeader = studyConfiguration.getVariantHeader();
        HashMap<String, String> headerAttributes = new HashMap<>();
        studyHeader.getAttributes().forEach((key, value) -> {
            if (!StudyConfiguration.UNKNOWN_HEADER_ATTRIBUTE.equals(value)) {
                headerAttributes.put(key, value);
            }
        });
        VariantFileHeader aggregatedHeader = new VariantFileHeader(studyHeader.getVersion(), studyHeader.getLines(), headerAttributes);

        return VariantDatasetMetadata.newBuilder()
                .setId(studyConfiguration.getStudyName())
                .setDescription(null)
                .setStats(null)
                .setFiles(fileMetadata)
                .setIndividuals(individuals)
                .setCohorts(cohorts)
                .setSampleSetType(SampleSetType.COLLECTION)
                .setAggregation(studyConfiguration.getAggregation())
                .setAggregatedHeader(aggregatedHeader)
                .setAttributes(attributes)
                .build();
    }

    protected Individual createIndividual(String sampleName) {
        return Individual.newBuilder()
                .setId(sampleName)
                .setSamples(Collections.singletonList(Sample.newBuilder()
                        .setId(sampleName)
                        .build()))
                .build();
    }

    protected Cohort getCohort(StudyConfiguration studyConfiguration, Integer cohortId) {
        return Cohort.newBuilder()
                .setId(studyConfiguration.getCohortIds().inverse().get(cohortId))
                .setSampleIds(toSampleNames(studyConfiguration, studyConfiguration.getCohorts().get(cohortId)))
                .setSampleSetType(SampleSetType.COLLECTION)
                .build();
    }

    protected List<String> toSampleNames(StudyConfiguration studyConfiguration, Collection<Integer> sampleIds) {
        List<String> sampleNames = new ArrayList<>(sampleIds.size());
        BiMap<Integer, String> sampleIdToSampleName = studyConfiguration.getSampleIds().inverse();
        sampleIds.forEach(sampleId -> sampleNames.add(sampleIdToSampleName.get(sampleId)));
        return sampleNames;
    }

    public List<StudyConfiguration> toStudyConfigurations(VariantMetadata variantMetadata) {
        List<StudyConfiguration> studyConfigurations = new ArrayList<>(variantMetadata.getDatasets().size());
        int id = 1;
        VariantMetadataManager metadataManager = new VariantMetadataManager().setVariantMetadata(variantMetadata);
        for (VariantDatasetMetadata datasetMetadata : variantMetadata.getDatasets()) {
            StudyConfiguration sc = new StudyConfiguration(id++, datasetMetadata.getId());
            studyConfigurations.add(sc);
            List<Sample> samples = metadataManager.getSamples(datasetMetadata.getId());
            for (Sample sample : samples) {
                sc.getSampleIds().put(sample.getId(), id++);
            }
            for (VariantFileMetadata fileMetadata : datasetMetadata.getFiles()) {
                int fileId = id++;
                sc.getIndexedFiles().add(fileId);
                sc.getFileIds().put(fileMetadata.getAlias(), fileId);
                List<Integer> sampleIds = toSampleIds(sc, fileMetadata.getSampleIds());
                sc.getSamplesInFiles().put(fileId, new LinkedHashSet<>(sampleIds));
            }

            for (Cohort cohort : datasetMetadata.getCohorts()) {
                int cohortId = id++;
                sc.getCohortIds().put(cohort.getId(), cohortId);
                sc.getCalculatedStats().add(cohortId);
                sc.getCohorts().put(cohortId, new HashSet<>(toSampleIds(sc, cohort.getSampleIds())));
            }

            sc.setVariantHeader(datasetMetadata.getAggregatedHeader());
            sc.setAggregation(datasetMetadata.getAggregation());
            datasetMetadata.getAttributes().forEach(sc.getAttributes()::put);
        }
        return studyConfigurations;
    }

    protected List<Integer> toSampleIds(StudyConfiguration sc, Collection<String> sampleNames) {
        List<Integer> sampleIds = new ArrayList<>(sampleNames.size());
        sampleNames.forEach(sampleName -> {
            Integer sampleId = sc.getSampleIds().get(sampleName);
            // Skip non exported samples
            if (sampleId != null) {
                sampleIds.add(sampleId);
            }
        });
        return sampleIds;
    }

}
