package org.opencb.opencga.catalog.utils;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.models.clinical.Disorder;
import org.opencb.commons.utils.DockerUtils;
import org.opencb.opencga.core.common.GitRepositoryState;
import org.opencb.opencga.core.models.family.Family;
import org.opencb.opencga.core.models.individual.Individual;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

public class PedigreeGraphUtils {

    public static final String PEDIGREE_FILENAME = "pedigree.ped";
    public static final String PEDIGREE_IMAGE_FILENAME = "pedigree.png";
    public static final String PEDIGREE_TSV_FILENAME = "ped_coords.tsv";

    public static final String R_DOCKER_IMAGE = "opencb/opencga-ext-tools:" + GitRepositoryState.get().getBuildVersion();

    public static String getPedigreeGraph(Family family, Path openCgaHome, Path scratchDir) throws IOException {
        // Prepare R script and out paths
        Path rScriptPath = openCgaHome.resolve("analysis/pedigree-graph");

        Path outDir = Paths.get(scratchDir +  "/pedigree-graph-" + family.getUuid() + "-" + System.nanoTime());
        outDir.toFile().mkdir();
        Runtime.getRuntime().exec("chmod 777 " + outDir.toAbsolutePath());

        // Execute R script and get b64 image
        createPedigreeGraph(family, rScriptPath, outDir);
        String b64PedigreeGraph = getB64PedigreeGraph(outDir);

        // Clean
        if (outDir.toFile().exists()) {
            FileUtils.deleteDirectory(outDir.toFile());
        }
        return b64PedigreeGraph;
    }

    public static void createPedigreeGraph(Family family, Path rScriptPath, Path outDir) throws IOException {
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
            String cmdline = DockerUtils.run(R_DOCKER_IMAGE, inputBindings, outputBinding, scriptParams.toString(), null);
        } catch (IOException e) {
            throw new IOException("Error running the command line to create the family pedigree graph", e);
        }
    }

    public static String getB64PedigreeGraph(Path outDir) throws IOException {
        File imgFile = outDir.resolve(PEDIGREE_IMAGE_FILENAME).toFile();
        if (!imgFile.exists()) {
            throw new IOException("Pedigree graph image file (" + PEDIGREE_IMAGE_FILENAME + ") not found at " + outDir);
        }

        FileInputStream fileInputStreamReader = new FileInputStream(imgFile);
        byte[] bytes = new byte[(int) imgFile.length()];
        fileInputStreamReader.read(bytes);
        return new String(Base64.getEncoder().encode(bytes), StandardCharsets.UTF_8);
    }

    public static String getTsvPedigreeGraph(Path outDir) throws IOException {
        File imgFile = outDir.resolve(PEDIGREE_TSV_FILENAME).toFile();
        if (!imgFile.exists()) {
            throw new IOException("Pedigree graph tsv file (" + PEDIGREE_TSV_FILENAME + ") not found at " + outDir);
        }

        FileInputStream fileInputStreamReader = new FileInputStream(imgFile);
        byte[] bytes = new byte[(int) imgFile.length()];
        fileInputStreamReader.read(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    private static File createPedFile(Family family, Path outDir) throws FileNotFoundException {
        File file = outDir.resolve(PEDIGREE_FILENAME).toFile();
        try (PrintWriter pw = new PrintWriter(file)) {
            List<Disorder> disorders = family.getDisorders();
            StringBuilder sbDisorders = new StringBuilder();
            if (disorders.size() == 1) {
                sbDisorders.append("affected").append("\t");
            } else {
                for (Disorder disorder : family.getDisorders()) {
                    sbDisorders.append("affected.").append(disorder.getId()).append("\t");
                }
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
                for (Disorder disorder : disorders) {
                    if (CollectionUtils.isNotEmpty(member.getDisorders())) {
                        boolean match = member.getDisorders().stream().anyMatch(e -> e.getId().equals(disorder.getId()));
                        if (match) {
                            sb.append("1").append("\t");
                        } else {
                            sb.append("2").append("\t");
                        }
                    } else {
                        sb.append("2").append("\t");
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
}

