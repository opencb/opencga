package org.opencb.opencga.catalog.utils;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.models.clinical.Disorder;
import org.opencb.commons.utils.DockerUtils;
import org.opencb.opencga.core.common.JacksonUtils;
import org.opencb.opencga.core.config.Configuration;
import org.opencb.opencga.core.config.ConfigurationUtils;
import org.opencb.opencga.core.config.Docker;
import org.opencb.opencga.core.exceptions.ToolExecutorException;
import org.opencb.opencga.core.models.family.Family;
import org.opencb.opencga.core.models.family.PedigreeGraph;
import org.opencb.opencga.core.models.individual.Individual;
import org.opencb.opencga.core.tools.family.PedigreeGraphAnalysisExecutor;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

public class PedigreeGraphUtils {

    public static final String PEDIGREE_FILENAME = "pedigree.ped";
    public static final String PEDIGREE_IMAGE_FILENAME = "pedigree.png";
    public static final String PEDIGREE_JSON_FILENAME = "ped_coords.json";
    public static final String PEDIGREE_TSV_FILENAME = "ped_coords.tsv";

    public static PedigreeGraph getPedigreeGraph(Family family, Path openCgaHome, Configuration configuration) throws IOException {
        PedigreeGraph pedigreeGraph = new PedigreeGraph();

        if (hasMinTwoGenerations(family)) {
            // Prepare R script and out paths
            Path rScriptPath = openCgaHome.resolve("analysis/" + PedigreeGraphAnalysisExecutor.ID);

            Path scratchDir = Paths.get(configuration.getAnalysis().getScratchDir());
            Path outDir = Paths.get(scratchDir + "/" + PedigreeGraphAnalysisExecutor.ID + "-" + family.getUuid() + "-" + System.nanoTime());
            outDir.toFile().mkdir();
            Runtime.getRuntime().exec("chmod 777 " + outDir.toAbsolutePath());

            // Execute R script and get b64 image
            createPedigreeGraph(family, rScriptPath, outDir, configuration);
            pedigreeGraph.setBase64(getB64Image(outDir));
            pedigreeGraph.setJson(getJsonPedigreeGraph(outDir));

            // Clean
            if (outDir.toFile().exists()) {
                FileUtils.deleteDirectory(outDir.toFile());
            }
        }

        return pedigreeGraph;
    }

    public static void createPedigreeGraph(Family family, Path rScriptPath, Path outDir, Configuration configuration) throws IOException {
        File pedFile;
        try {
            pedFile = createPedFile(family, outDir);
        } catch (FileNotFoundException e) {
            throw new IOException("Error creating the pedigree file", e);
        }

        // Build command line to execute
        List<AbstractMap.SimpleEntry<String, String>> inputBindings = new ArrayList<>();
        inputBindings.add(new AbstractMap.SimpleEntry<>(rScriptPath.toAbsolutePath().toString(), "/script"));

        AbstractMap.SimpleEntry<String, String> outputBinding = new AbstractMap.SimpleEntry<>(outDir.toAbsolutePath().toString(),
                "/data");

        StringBuilder scriptParams = new StringBuilder("R CMD Rscript --vanilla")
                .append(" /script/ped.R")
                .append(" /data/").append(pedFile.getName())
                .append(" /data/")
                .append(" --plot_format png");

        try {
            String dockerImage = ConfigurationUtils.getDockerImage(Docker.OPENCGA_EXT_TOOLS_IMAGE_KEY, configuration);
            String cmdline = DockerUtils.run(dockerImage, inputBindings, outputBinding, scriptParams.toString(), null);
        } catch (IOException | ToolExecutorException e) {
            throw new IOException("Error running the command line to create the family pedigree graph", e);
        }
    }

    public static String getB64Image(Path outDir) throws IOException {
        File imgFile = outDir.resolve(PEDIGREE_IMAGE_FILENAME).toFile();
        if (!imgFile.exists()) {
            throw new IOException("Pedigree graph image file (" + PEDIGREE_IMAGE_FILENAME + ") not found at " + outDir);
        }
        byte[] bytes = Files.readAllBytes(imgFile.toPath());
        return Base64.getEncoder().encodeToString(bytes);
    }

    public static Object getJsonPedigreeGraph(Path outDir) throws IOException {
        File jsonFile = outDir.resolve(PEDIGREE_JSON_FILENAME).toFile();
        if (!jsonFile.exists()) {
            throw new IOException("Pedigree graph JSON file (" + PEDIGREE_JSON_FILENAME + ") not found at " + outDir);
        }

        return JacksonUtils.getDefaultObjectMapper().readValue(jsonFile, Object.class);
    }

    public static String getTsvPedigreeGraph(Path outDir) throws IOException {
        File tsvFile = outDir.resolve(PEDIGREE_TSV_FILENAME).toFile();
        if (!tsvFile.exists()) {
            throw new IOException("Pedigree graph TSV file (" + PEDIGREE_TSV_FILENAME + ") not found at " + outDir);
        }

        byte[] bytes = Files.readAllBytes(tsvFile.toPath());
        return new String(bytes, StandardCharsets.UTF_8);
    }

    private static File createPedFile(Family family, Path outDir) throws FileNotFoundException {
        File file = outDir.resolve(PEDIGREE_FILENAME).toFile();
        try (PrintWriter pw = new PrintWriter(file)) {
            List<Disorder> disorders = family.getDisorders();
            StringBuilder sbDisorders = new StringBuilder();
            if (CollectionUtils.isNotEmpty(disorders) && disorders.size() > 1) {
                for (Disorder disorder : family.getDisorders()) {
                    sbDisorders.append("\"affected.").append(disorder.getId()).append("\"\t");
                }
            } else {
                sbDisorders.append("affected").append("\t");
            }
            pw.println("id\tdadid\tmomid\tsex\t" + sbDisorders + "status\trelation");

            for (Individual member : family.getMembers()) {
                StringBuilder sb = new StringBuilder(member.getId()).append("\t");

                // Father
                if (member.getFather() == null || (member.getFather() != null && StringUtils.isEmpty(member.getFather().getId()))) {
                    sb.append("NA").append("\t");
                } else {
                    sb.append(member.getFather().getId()).append("\t");
                }

                // Mother
                if (member.getMother() == null || (member.getMother() != null && StringUtils.isEmpty(member.getMother().getId()))) {
                    sb.append("NA").append("\t");
                } else {
                    sb.append(member.getMother().getId()).append("\t");
                }

                // Sex
                if (member.getSex() != null && member.getSex().getSex() != null) {
                    switch (member.getSex().getSex()) {
                        case MALE:
                            sb.append("1").append("\t");
                            break;
                        case FEMALE:
                            sb.append("2").append("\t");
                            break;
                        case UNDETERMINED:
                            sb.append("4").append("\t");
                            break;
                        case UNKNOWN:
                        default:
                            sb.append("3").append("\t");
                            break;
                    }
                } else {
                    // Unknown sex
                    sb.append("3").append("\t");
                }

                // Affected
                if (CollectionUtils.isEmpty(disorders)) {
                    sb.append("0").append("\t");
                } else {
                    for (Disorder disorder : disorders) {
                        if (CollectionUtils.isNotEmpty(member.getDisorders())) {
                            boolean match = member.getDisorders().stream().anyMatch(e -> e.getId().equals(disorder.getId()));
                            if (match) {
                                sb.append("1").append("\t");
                            } else {
                                sb.append("0").append("\t");
                            }
                        } else {
                            sb.append("0").append("\t");
                        }
                    }
                }

                // Status
                if (member.getLifeStatus() == null) {
                    sb.append("0").append("\t");
                } else {
                    switch (member.getLifeStatus()) {
                        case DECEASED:
                        case ABORTED:
                        case STILLBORN:
                        case MISCARRIAGE:
                            sb.append("1").append("\t");
                            break;
                        case ALIVE:
                        default:
                            sb.append("0").append("\t");
                            break;
                    }
                }

                // Relation
                sb.append("NA").append("\t");

                // Write line
                pw.println(sb);
            }
        }
        return file;
    }

    public static boolean hasTrios(Family family) {
        if (CollectionUtils.isNotEmpty(family.getMembers())) {
            Set<String> memberIds = new HashSet<>(family.getMembers().stream().map(m -> m.getId()).collect(Collectors.toList()));
            for (Individual member : family.getMembers()) {
                if (member.getFather() != null && memberIds.contains(member.getFather().getId())
                        && member.getMother() != null && memberIds.contains(member.getMother().getId())) {
                    return true;
                }
            }
        }
        return false;
    }

    public static boolean hasMinTwoGenerations(Family family) {
        if (CollectionUtils.isNotEmpty(family.getMembers())) {
            Set<String> memberIds = new HashSet<>(family.getMembers().stream().map(m -> m.getId()).collect(Collectors.toList()));
            for (Individual member : family.getMembers()) {
                if ((member.getFather() != null && memberIds.contains(member.getFather().getId()))
                        || (member.getMother() != null && memberIds.contains(member.getMother().getId()))) {
                    return true;
                }
            }
        }
        return false;
    }
}

