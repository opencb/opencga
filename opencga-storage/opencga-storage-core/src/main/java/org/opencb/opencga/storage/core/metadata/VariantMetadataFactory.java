package org.opencb.opencga.storage.core.metadata;

import com.google.common.collect.BiMap;
import org.opencb.biodata.models.metadata.*;
import org.opencb.biodata.models.variant.metadata.*;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.storage.core.variant.annotation.VariantAnnotationManager;

import java.util.*;

/**
 * Created on 02/08/17.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class VariantMetadataFactory {

    public VariantMetadataFactory() {
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
                .setVersion("1")
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
                    .setId(studyConfiguration.getFileIds().inverse().get(fileId))
                    .setAlias(fileId.toString())
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

        List<VariantFileHeaderLine> lines = new ArrayList<>();
        if (studyConfiguration.getVariantMetadata() != null) {
            toVariantFileHeaderLineSimple(lines, "ALT", studyConfiguration.getVariantMetadata().getAlternates());
            toVariantFileHeaderLineSimple(lines, "FILTER", studyConfiguration.getVariantMetadata().getFilter());
            toVariantFileHeaderLine(lines, "FORMAT", studyConfiguration.getVariantMetadata().getFormat().values());
            toVariantFileHeaderLine(lines, "INFO", studyConfiguration.getVariantMetadata().getInfo().values());
            studyConfiguration.getVariantMetadata().getContig().forEach((contig, length) -> {
                lines.add(VariantFileHeaderLine
                        .newBuilder()
                        .setKey("contig")
                        .setId(contig)
                        .setAttributes(Collections.singletonMap("length", String.valueOf(length)))
                        .build());
            });
        }

        return VariantDatasetMetadata.newBuilder()
                .setId(studyConfiguration.getStudyName())
                .setDescription(null)
                .setStats(null)
                .setFiles(fileMetadata)
                .setIndividuals(individuals)
                .setCohorts(cohorts)
                .setSampleSetType(SampleSetType.COLLECTION)
                .setAggregation(studyConfiguration.getAggregation())
                .setAggregatedHeader(VariantFileHeader.newBuilder()
                        .setVersion("")
                        .setLines(lines).build())
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

    protected void toVariantFileHeaderLineSimple(List<VariantFileHeaderLine> lines, String key, Map<String, String> values) {
        values.forEach((id, description) -> lines.add(VariantFileHeaderLine.newBuilder()
                .setKey(key)
                .setId(id)
                .setDescription(description)
                .build()));
    }

    protected void toVariantFileHeaderLine(List<VariantFileHeaderLine> lines, String key,
                                           Collection<VariantStudyMetadata.VariantMetadataRecord> records) {
        for (VariantStudyMetadata.VariantMetadataRecord record : records) {
            String number;
            switch (record.getNumberType()) {
                case INTEGER:
                    number = record.getNumber().toString();
                    break;
                case A:
                case R:
                case G:
                    number = record.getNumberType().name();
                    break;
                case UNBOUNDED:
                    number = null;
                    break;
                default:
                    throw new IllegalArgumentException("Unknown value " + record.getNumberType());
            }
            lines.add(VariantFileHeaderLine.newBuilder()
                    .setKey(key)
                    .setId(record.getId())
                    .setDescription(record.getDescription())
                    .setNumber(number)
                    .setType(record.getType().name())
                    .build());
        }
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

//    public Variants.VariantSetMetadata toVariantSetMetadata(StudyConfiguration studyConfiguration) {
//
//        return null;
//    }

}
