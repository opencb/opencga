package org.opencb.opencga.storage.core.metadata;

import com.google.common.collect.BiMap;
import org.opencb.biodata.models.metadata.*;
import org.opencb.biodata.models.variant.metadata.*;
import org.opencb.biodata.tools.variant.metadata.VariantMetadataManager;
import org.opencb.opencga.core.common.GitRepositoryState;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.storage.core.metadata.models.ProjectMetadata;

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

    public VariantMetadata toVariantMetadata(Collection<StudyConfiguration> studyConfigurations,
                                             ProjectMetadata projectMetadata, Map<Integer, List<Integer>> returnedSamples,
                                             Map<Integer, List<Integer>> returnedFiles) {
        List<VariantStudyMetadata> studies = new ArrayList<>();
        String specie = projectMetadata.getSpecies();
        String assembly = projectMetadata.getAssembly();
        for (StudyConfiguration studyConfiguration : studyConfigurations) {
            VariantStudyMetadata studyMetadata = toVariantStudyMetadata(studyConfiguration,
                    returnedSamples == null ? null : returnedSamples.get(studyConfiguration.getStudyId()),
                    returnedFiles == null ? null : returnedFiles.get(studyConfiguration.getStudyId()));
            studies.add(studyMetadata);
        }

        Species species = Species.newBuilder()
                .setId(specie)
                .setAssembly(assembly)
                .build();
        return VariantMetadata.newBuilder()
//                .setDate(Date.from(Instant.now()).toString())
                .setCreationDate(TimeUtils.getTime())
                .setStudies(studies)
                .setVersion(GitRepositoryState.get().getDescribeShort())
                .setSpecies(species)
                .build();

    }

    public VariantStudyMetadata toVariantStudyMetadata(StudyConfiguration studyConfiguration) {
        return toVariantStudyMetadata(studyConfiguration, null, null);
    }

    public VariantStudyMetadata toVariantStudyMetadata(StudyConfiguration studyConfiguration,
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
                    .setPath(studyConfiguration.getFileIds().inverse().get(fileId))
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
        List<VariantFileHeaderSimpleLine> headerSimpleLines = new ArrayList<>(studyHeader.getSimpleLines().size());
        studyHeader.getSimpleLines().forEach(line -> {
            if (!StudyConfiguration.UNKNOWN_HEADER_ATTRIBUTE.equals(line.getValue())) {
                headerSimpleLines.add(line);
            }
        });
        VariantFileHeader aggregatedHeader = new VariantFileHeader(studyHeader.getVersion(), studyHeader.getComplexLines(),
                headerSimpleLines);

        return VariantStudyMetadata.newBuilder()
                .setId(studyConfiguration.getStudyName())
                .setDescription(null)
                .setStats(null)
                .setFiles(fileMetadata)
                .setIndividuals(individuals)
                .setCohorts(cohorts)
                .setSampleSetType(SampleSetType.UNKNOWN)
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
                .setSampleSetType(SampleSetType.UNKNOWN)
                .build();
    }

    protected List<String> toSampleNames(StudyConfiguration studyConfiguration, Collection<Integer> sampleIds) {
        List<String> sampleNames = new ArrayList<>(sampleIds.size());
        BiMap<Integer, String> sampleIdToSampleName = studyConfiguration.getSampleIds().inverse();
        sampleIds.forEach(sampleId -> sampleNames.add(sampleIdToSampleName.get(sampleId)));
        return sampleNames;
    }

    public List<StudyConfiguration> toStudyConfigurations(VariantMetadata variantMetadata) {
        List<StudyConfiguration> studyConfigurations = new ArrayList<>(variantMetadata.getStudies().size());
        int id = 1;
        VariantMetadataManager metadataManager = new VariantMetadataManager().setVariantMetadata(variantMetadata);
        for (VariantStudyMetadata studyMetadata : variantMetadata.getStudies()) {
            StudyConfiguration sc = new StudyConfiguration(id++, studyMetadata.getId());
            studyConfigurations.add(sc);
            List<Sample> samples = metadataManager.getSamples(studyMetadata.getId());
            for (Sample sample : samples) {
                sc.getSampleIds().put(sample.getId(), id++);
            }
            for (VariantFileMetadata fileMetadata : studyMetadata.getFiles()) {
                int fileId = id++;
                sc.getIndexedFiles().add(fileId);
                sc.getFileIds().put(fileMetadata.getPath(), fileId);
                List<Integer> sampleIds = toSampleIds(sc, fileMetadata.getSampleIds());
                sc.getSamplesInFiles().put(fileId, new LinkedHashSet<>(sampleIds));
            }

            for (Cohort cohort : studyMetadata.getCohorts()) {
                int cohortId = id++;
                sc.getCohortIds().put(cohort.getId(), cohortId);
                sc.getCalculatedStats().add(cohortId);
                sc.getCohorts().put(cohortId, new HashSet<>(toSampleIds(sc, cohort.getSampleIds())));
            }

            sc.setVariantHeader(studyMetadata.getAggregatedHeader());
            sc.setAggregation(studyMetadata.getAggregation());
            studyMetadata.getAttributes().forEach(sc.getAttributes()::put);
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
