package org.opencb.opencga.storage.core.metadata;

import org.opencb.biodata.models.metadata.*;
import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.biodata.models.variant.commons.Aggregation;
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
        List<VariantDatasetMetadata> datasets = new ArrayList<>();
        String specie = null;
        String assembly = null;
        for (StudyConfiguration studyConfiguration : studyConfigurations) {
            VariantDatasetMetadata variantDatasetMetadata = toVariantDatasetMetadata(studyConfiguration);
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

        List<VariantFileMetadata> fileMetadata = new ArrayList<>(studyConfiguration.getIndexedFiles().size());
        Set<Integer> indexedSampleIds = new HashSet<>();
        ArrayList<Individual> individuals = new ArrayList<>();

        for (Integer fileId : studyConfiguration.getIndexedFiles()) {
            LinkedHashSet<Integer> sampleIds = studyConfiguration.getSamplesInFiles().get(fileId);

            List<String> sampleNames = toSampleNames(studyConfiguration, sampleIds);
            for (Integer sampleId : sampleIds) {
                String sampleName = studyConfiguration.getSampleIds().inverse().get(sampleId);
                if (indexedSampleIds.add(sampleId)) {
                    individuals.add(
                            Individual.newBuilder()
                                    .setId(sampleName)
                                    .setSamples(Collections.singletonList(Sample.newBuilder()
                                            .setId(sampleName)
                                            .build()))
                                    .build());
                }
            }
            fileMetadata.add(VariantFileMetadata.newBuilder()
                    .setId(studyConfiguration.getFileIds().inverse().get(fileId))
                    .setAlias(fileId.toString())
                    .setSampleIds(sampleNames)
                    .build());
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
            toVariantFileHeaderLineSimple(lines, "ALT", studyConfiguration.getVariantMetadata().getAlternates().values());
            toVariantFileHeaderLineSimple(lines, "FILTER", studyConfiguration.getVariantMetadata().getFilter().values());
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
//            toVariantFileHeaderLineSimple(lines, "config", studyConfiguration.getVariantMetadata().getContig().values());
        }

        return VariantDatasetMetadata.newBuilder()
                .setId(studyConfiguration.getStudyName())
                .setDescription(null)
                .setStats(null)
                .setFiles(fileMetadata)
                .setIndividuals(individuals)
                .setCohorts(cohorts)
                .setSampleSetType(SampleSetType.COLLECTION)
                .setAggregation(toAggregation(studyConfiguration.getAggregation()))
                .setAggregatedHeader(VariantFileHeader.newBuilder()
                        .setVersion("")
                        .setLines(lines).build())
                .build();
    }

    protected void toVariantFileHeaderLineSimple(List<VariantFileHeaderLine> lines, String key, Collection<String> values) {
        for (String value : values) {
            lines.add(VariantFileHeaderLine.newBuilder().setKey(key).setId(value).build());
        }
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
        sampleIds.forEach(sampleId -> sampleNames.add(studyConfiguration.getSampleIds().inverse().get(sampleId)));
        return sampleNames;
    }

    private Aggregation toAggregation(VariantSource.Aggregation aggregation) {
        if (aggregation == null) {
            return null;
        }

        switch (aggregation) {
            case NONE:
                return Aggregation.NONE;
            case BASIC:
                return Aggregation.BASIC;
            case EVS:
                return Aggregation.EVS;
            case EXAC:
                return Aggregation.EXAC;
            default:
                return Aggregation.valueOf(aggregation.toString());
        }
    }

//    public Variants.VariantSetMetadata toVariantSetMetadata(StudyConfiguration studyConfiguration) {
//
//        return null;
//    }

}
