package org.opencb.opencga.storage.core.metadata;

import org.opencb.biodata.models.metadata.*;
import org.opencb.biodata.models.variant.metadata.*;
import org.opencb.biodata.tools.variant.metadata.VariantMetadataManager;
import org.opencb.opencga.core.common.GitRepositoryState;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.storage.core.metadata.models.CohortMetadata;
import org.opencb.opencga.storage.core.metadata.models.FileMetadata;
import org.opencb.opencga.storage.core.metadata.models.ProjectMetadata;
import org.opencb.opencga.storage.core.metadata.models.StudyMetadata;
import org.opencb.opencga.storage.core.variant.query.projection.VariantQueryProjection;

import java.util.*;

/**
 * Converts from VariantMetadata to Collection&lt;StudyMetadata&gt; and vice versa
 *
 * Created on 02/08/17.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class VariantMetadataConverter {

    private final VariantStorageMetadataManager metadataManager;

    public VariantMetadataConverter(VariantStorageMetadataManager metadataManager) {
        this.metadataManager = metadataManager;
    }

    public VariantMetadata toVariantMetadata(VariantQueryProjection variantQueryProjection) {
        ProjectMetadata projectMetadata = metadataManager.getProjectMetadata();
        List<VariantStudyMetadata> studies = new ArrayList<>();
        String specie = projectMetadata.getSpecies();
        String assembly = projectMetadata.getAssembly();
        for (StudyMetadata studyMetadata : variantQueryProjection.getStudyMetadatas().values()) {
            VariantStudyMetadata variantStudyMetadata = toVariantStudyMetadata(studyMetadata,
                    variantQueryProjection.getSamples().get(studyMetadata.getId()),
                    variantQueryProjection.getFiles().get(studyMetadata.getId()));
            studies.add(variantStudyMetadata);
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

    public VariantStudyMetadata toVariantStudyMetadata(StudyMetadata studyMetadata) {
        return toVariantStudyMetadata(studyMetadata, null, null);
    }

    public VariantStudyMetadata toVariantStudyMetadata(StudyMetadata studyMetadata,
                                                       List<Integer> returnedSamples,
                                                       List<Integer> returnedFiles) {

        int studyId = studyMetadata.getId();
        LinkedHashSet<Integer> indexedFiles = metadataManager.getIndexedFiles(studyId);
        if (returnedFiles == null) {
            returnedFiles = new ArrayList<>(indexedFiles);
        }
        List<VariantFileMetadata> fileMetadata = new ArrayList<>(returnedFiles.size());

        for (Integer fileId : returnedFiles) {
            if (!indexedFiles.contains(fileId)) {
                continue;
            }
            FileMetadata file = metadataManager.getFileMetadata(studyId, fileId);
            LinkedHashSet<Integer> sampleIds = file.getSamples();

            List<String> sampleNames = toSampleNames(studyMetadata, sampleIds);
            fileMetadata.add(VariantFileMetadata.newBuilder()
                    .setId(file.getName())
                    .setPath(file.getPath())
                    .setSampleIds(sampleNames)
                    .build());
        }

        ArrayList<Individual> individuals = new ArrayList<>();

        Collection<String> sampleNames;
        if (returnedSamples == null) {
            sampleNames = metadataManager.getSamplesPosition(studyMetadata, null).keySet();
        } else {
            sampleNames = toSampleNames(studyMetadata, returnedSamples);
        }
        for (String sampleName : sampleNames) {
            individuals.add(createIndividual(sampleName));
        }

        ArrayList<Cohort> cohorts = new ArrayList<>();
        metadataManager.cohortIterator(studyMetadata.getId()).forEachRemaining(cohortMetadata -> {
            cohorts.add(getCohort(studyMetadata, cohortMetadata));
        });

        Map<String, String> attributes = new HashMap<>(studyMetadata.getAttributes().size());
        for (String key : studyMetadata.getAttributes().keySet()) {
            attributes.put(key, studyMetadata.getAttributes().getString(key));
        }

        // Header from the StudyMetadata stores variable attributes (where the value changes for every file) as unknown values.
        // We don't want to export those values at the aggregated header.
        // Copy the header and remove unknown attributes
        VariantFileHeader studyHeader = studyMetadata.getVariantHeader();
        List<VariantFileHeaderSimpleLine> simpleLines = studyHeader.getSimpleLines() == null
                ? Collections.emptyList()
                : studyHeader.getSimpleLines();
        List<VariantFileHeaderSimpleLine> headerSimpleLines = new ArrayList<>(simpleLines.size());
        simpleLines.forEach(line -> {
            if (!StudyMetadata.UNKNOWN_HEADER_ATTRIBUTE.equals(line.getValue())) {
                headerSimpleLines.add(line);
            }
        });
        VariantFileHeader aggregatedHeader = new VariantFileHeader(studyHeader.getVersion(), studyHeader.getComplexLines(),
                headerSimpleLines);

        return VariantStudyMetadata.newBuilder()
                .setId(studyMetadata.getName())
                .setDescription(null)
                .setStats(null)
                .setFiles(fileMetadata)
                .setIndividuals(individuals)
                .setCohorts(cohorts)
                .setSampleSetType(SampleSetType.UNKNOWN)
                .setAggregation(studyMetadata.getAggregation())
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

    protected Cohort getCohort(StudyMetadata studyMetadata, CohortMetadata cohortMetadata) {
        return Cohort.newBuilder()
                .setId(cohortMetadata.getName())
                .setSampleIds(toSampleNames(studyMetadata, cohortMetadata.getSamples()))
                .setSampleSetType(SampleSetType.UNKNOWN)
                .build();
    }

    protected List<String> toSampleNames(StudyMetadata studyMetadata, Collection<Integer> sampleIds) {
        List<String> sampleNames = new ArrayList<>(sampleIds.size());
        sampleIds.forEach(sampleId -> sampleNames.add(metadataManager.getSampleName(studyMetadata.getId(), sampleId)));
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

    protected List<Integer> toSampleIds(StudyMetadata studyMetadata, Collection<String> sampleNames) {
        List<Integer> sampleIds = new ArrayList<>(sampleNames.size());
        sampleNames.forEach(sampleName -> {
            Integer sampleId = metadataManager.getSampleId(studyMetadata.getId(), sampleName);
            // Skip non exported samples
            if (sampleId != null) {
                sampleIds.add(sampleId);
            }
        });
        return sampleIds;
    }

}
