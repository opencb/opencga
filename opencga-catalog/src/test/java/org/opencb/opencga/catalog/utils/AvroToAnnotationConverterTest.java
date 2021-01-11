package org.opencb.opencga.catalog.utils;

import org.junit.Ignore;
import org.junit.Test;
import org.opencb.biodata.models.variant.annotation.ConsequenceTypeMappings;
import org.opencb.biodata.models.variant.metadata.SampleVariantStats;
import org.opencb.biodata.models.variant.metadata.VariantFileMetadata;
import org.opencb.biodata.models.variant.metadata.VariantSetStats;
import org.opencb.cellbase.core.variant.annotation.VariantAnnotationUtils;
import org.opencb.opencga.core.common.JacksonUtils;
import org.opencb.opencga.core.models.study.Variable;
import org.opencb.opencga.core.models.study.VariableSet;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

public class AvroToAnnotationConverterTest {

    @Test
    public void generateCohortVariantSetStats() throws IOException {
        List<Variable> variables = AvroToAnnotationConverter.convertToVariableSet(VariantSetStats.getClassSchema());

        Variable biotypeCount = variables.stream().filter(v -> v.getId().equals("biotypeCount")).findFirst().get();
        addBiotypeKeys(biotypeCount);

        Variable consequenceTypeCount = variables.stream().filter(v -> v.getId().equals("consequenceTypeCount")).findFirst().get();
        addConsequenceTypeKeys(consequenceTypeCount);

        VariableSet variableSet = new VariableSet()
                .setId("opencga_cohort_variant_stats")
                .setName("opencga_cohort_variant_stats")
                .setDescription("OpenCGA cohort variant stats")
                .setEntities(Collections.singletonList(VariableSet.AnnotableDataModels.COHORT))
                .setUnique(true)
                .setConfidential(false)
                .setAttributes(Collections.singletonMap("avroClass", VariantSetStats.class.toString()))
                .setVariables(new LinkedHashSet<>(variables));


        serialize(variableSet, "cohort-variant-stats-variableset.json");
    }

    @Test
    public void generateFileVariantSetStats() throws IOException {
        List<Variable> variables = AvroToAnnotationConverter.convertToVariableSet(VariantSetStats.getClassSchema());

        Variable biotypeCount = variables.stream().filter(v -> v.getId().equals("biotypeCount")).findFirst().get();
        addBiotypeKeys(biotypeCount);

        Variable consequenceTypeCount = variables.stream().filter(v -> v.getId().equals("consequenceTypeCount")).findFirst().get();
        addConsequenceTypeKeys(consequenceTypeCount);

        VariableSet variableSet = new VariableSet()
                .setId(FileMetadataReader.FILE_VARIANT_STATS_VARIABLE_SET)
                .setName(FileMetadataReader.FILE_VARIANT_STATS_VARIABLE_SET)
                .setDescription("OpenCGA file variant stats")
                .setEntities(Collections.singletonList(VariableSet.AnnotableDataModels.FILE))
                .setUnique(true)
                .setConfidential(false)
                .setAttributes(Collections.singletonMap("avroClass", VariantSetStats.class.toString()))
                .setVariables(new LinkedHashSet<>(variables));


        serialize(variableSet, "file-variant-stats-variableset.json");
    }

    @Test
    public void generateSampleVariantStats() throws IOException {
        List<Variable> variables = AvroToAnnotationConverter.convertToVariableSet(SampleVariantStats.getClassSchema());

        Variable biotypeCount = variables.stream().filter(v -> v.getId().equals("biotypeCount")).findFirst().get();
        addBiotypeKeys(biotypeCount);

        Variable consequenceTypeCount = variables.stream().filter(v -> v.getId().equals("consequenceTypeCount")).findFirst().get();
        addConsequenceTypeKeys(consequenceTypeCount);

        VariableSet variableSet = new VariableSet()
                .setId("opencga_sample_variant_stats")
                .setName("opencga_sample_variant_stats")
                .setDescription("OpenCGA sample variant stats")
                .setEntities(Collections.singletonList(VariableSet.AnnotableDataModels.SAMPLE))
                .setUnique(false)
                .setInternal(true)
                .setConfidential(false)
                .setAttributes(Collections.singletonMap("avroClass", SampleVariantStats.class.toString()))
                .setVariables(new LinkedHashSet<>(variables));

        serialize(variableSet, "sample-variant-stats-variableset.json");
    }

    @Test
    @Ignore
    public void generateVariantFileMetadata() throws IOException {
        List<Variable> variables = AvroToAnnotationConverter.convertToVariableSet(VariantFileMetadata.getClassSchema());

        VariableSet variableSet = new VariableSet()
                .setId("opencga_variant_file_metadata")
                .setName("opencga_variant_file_metadata")
                .setDescription("OpenCGA variant file metadata")
                .setEntities(Collections.singletonList(VariableSet.AnnotableDataModels.FILE))
                .setUnique(true)
                .setConfidential(false)
                .setAttributes(Collections.singletonMap("avroClass", VariantFileMetadata.class.toString()))
                .setVariables(new LinkedHashSet<>(variables));

        serialize(variableSet, "variant-file-metadata-variableset.json");
    }

    private void serialize(VariableSet variableSet, String fileName) throws IOException {
        File file = getFile(fileName);
        System.out.println("Serialize variableSet '" + variableSet.getId() + "' in: " + file.getAbsolutePath());
        JacksonUtils.getDefaultObjectMapper().writerWithDefaultPrettyPrinter().writeValue(file, variableSet);
    }

    private File getFile(String s) throws IOException {
        Files.createDirectories(Paths.get("target/test-data/variablesets"));
        return Paths.get("target/test-data/variablesets", s).toFile();
    }

    private void addConsequenceTypeKeys(Variable consequenceTypeCount) {
        consequenceTypeCount.setAllowedKeys(new ArrayList<>(ConsequenceTypeMappings.termToAccession.keySet()));
    }

    private void addBiotypeKeys(Variable biotypeCount) {
        biotypeCount.setAllowedKeys(Arrays.asList(
                VariantAnnotationUtils.THREEPRIME_OVERLAPPING_NCRNA,
                "3prime_overlapping_ncRNA",
                VariantAnnotationUtils.AMBIGUOUS_ORF,
                VariantAnnotationUtils.ANTISENSE,
                "antisense_RNA",
                "bidirectional_promoter_lncRNA",
                VariantAnnotationUtils.IG_C_GENE,
                VariantAnnotationUtils.IG_C_PSEUDOGENE,
                VariantAnnotationUtils.IG_D_GENE,
                VariantAnnotationUtils.IG_J_GENE,
                VariantAnnotationUtils.IG_J_PSEUDOGENE,
                "IG_pseudogene",
                VariantAnnotationUtils.IG_V_GENE,
                VariantAnnotationUtils.IG_V_PSEUDOGENE,

                VariantAnnotationUtils.INTERGENIC_VARIANT,

                VariantAnnotationUtils.KNOWN_NCRNA,
                VariantAnnotationUtils.LINCRNA,
                VariantAnnotationUtils.LRG_GENE,

                "macro_lncRNA",
                VariantAnnotationUtils.MIRNA,
                VariantAnnotationUtils.MIRNA_PSEUDOGENE,
                VariantAnnotationUtils.MISC_RNA,
                VariantAnnotationUtils.MISC_RNA_PSEUDOGENE,
                VariantAnnotationUtils.MT_RRNA,
                VariantAnnotationUtils.MT_TRNA,
                VariantAnnotationUtils.NMD_TRANSCRIPT_VARIANT,
                VariantAnnotationUtils.NON_CODING,
                VariantAnnotationUtils.NONSENSE_MEDIATED_DECAY,
                VariantAnnotationUtils.NON_STOP_DECAY,

                VariantAnnotationUtils.POLYMORPHIC_PSEUDOGENE,
                VariantAnnotationUtils.PROCESSED_PSEUDOGENE,
                VariantAnnotationUtils.PROCESSED_TRANSCRIPT,
                VariantAnnotationUtils.PROTEIN_CODING,
                VariantAnnotationUtils.PSEUDOGENE,

                VariantAnnotationUtils.RETAINED_INTRON,
                VariantAnnotationUtils.RETROTRANSPOSED,
                "ribozyme",
                VariantAnnotationUtils.RRNA,
                VariantAnnotationUtils.RRNA_PSEUDOGENE,

                "scaRNA",
                "scRNA",
                VariantAnnotationUtils.SCRNA_PSEUDOGENE,
                VariantAnnotationUtils.SENSE_INTRONIC,
                VariantAnnotationUtils.SENSE_OVERLAPPING,
                VariantAnnotationUtils.SNORNA,
                VariantAnnotationUtils.SNORNA_PSEUDOGENE,
                VariantAnnotationUtils.SNRNA,
                VariantAnnotationUtils.SNRNA_PSEUDOGENE,
                "sRNA",

                "TEC",
                VariantAnnotationUtils.TRANSCRIBED_PROCESSED_PSEUDOGENE,
                VariantAnnotationUtils.TRANSCRIBED_UNITARY_PSEUDOGENE,
                VariantAnnotationUtils.TRANSCRIBED_UNPROCESSED_PSEUDGENE,
                VariantAnnotationUtils.TRANSLATED_PROCESSED_PSEUDOGENE,
                VariantAnnotationUtils.TRANSLATED_UNPROCESSED_PSEUDOGENE,
                VariantAnnotationUtils.TR_C_GENE,
                VariantAnnotationUtils.TR_D_GENE,
                VariantAnnotationUtils.TR_J_GENE,
                VariantAnnotationUtils.TR_J_PSEUDOGENE,
                VariantAnnotationUtils.TRNA_PSEUDOGENE,
                VariantAnnotationUtils.TR_V_GENE,
                VariantAnnotationUtils.TR_V_PSEUDOGENE,
                VariantAnnotationUtils.UNITARY_PSEUDOGENE,
                VariantAnnotationUtils.UNPROCESSED_PSEUDOGENE,
                "vaultRNA"
        ));
    }
}