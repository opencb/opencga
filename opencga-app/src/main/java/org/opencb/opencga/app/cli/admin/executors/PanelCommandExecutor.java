package org.opencb.opencga.app.cli.admin.executors;

import org.opencb.biodata.models.clinical.interpretation.DiseasePanel;
import org.opencb.biodata.tools.clinical.DiseasePanelParsers;
import org.opencb.commons.utils.FileUtils;
import org.opencb.opencga.app.cli.CommandExecutor;
import org.opencb.opencga.app.cli.admin.AdminCliOptionsParser;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.core.common.JacksonUtils;

import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class PanelCommandExecutor extends CommandExecutor {

    private AdminCliOptionsParser.PanelCommandOptions panelCommandOptions;

    public PanelCommandExecutor(AdminCliOptionsParser.PanelCommandOptions panelCommandOptions) {
        super(panelCommandOptions.commonOptions);
        this.panelCommandOptions = panelCommandOptions;
    }

    @Override
    public void execute() throws Exception {
        String subCommandString = panelCommandOptions.getParsedSubCommand();
        logger.debug("Executing catalog admin {} command line", subCommandString);
        switch (subCommandString) {
            case "panelapp":
                parsePanelApp();
                break;
            case "cancer-gene-census":
                parseGeneCensus();
                break;
            default:
                logger.error("Subcommand not valid");
                break;
        }

    }

    private void parsePanelApp() throws IOException {
        AdminCliOptionsParser.PanelAppCommandOptions options = panelCommandOptions.panelAppCommandOptions;

        Path directory = Paths.get(options.outdir);
        FileUtils.checkDirectory(directory, true);

        Path originalsDirectory = directory.resolve("originals");
        logger.info("Creating temporal directory '{}' to store downloaded panels from PanelApp", originalsDirectory);
        Files.createDirectory(originalsDirectory);

        int i = 1;
        int max = Integer.MAX_VALUE;

        FileWriter fileWriter;
        try {
            fileWriter = new FileWriter(directory.resolve("panels.json").toFile());
        } catch (IOException e) {
            logger.error("Error creating fileWriter: {}", e.getMessage(), e);
            throw e;
        }

        List<String> panelIds = new LinkedList<>();
        while (i < max) {
            URL url = new URL("https://panelapp.genomicsengland.co.uk/api/v1/panels/?format=json&page=" + i);

            Map<String, Object> panels;
            try (InputStream inputStream = url.openStream()) {
                panels = JacksonUtils.getDefaultObjectMapper().readValue(inputStream, Map.class);
            } catch (IOException e) {
                logger.error("{}", e.getMessage(), e);
                return;
            }

            if (i == 1) {
                // We set the maximum number of pages we will need to fetch
                max = Double.valueOf(Math.ceil(Integer.parseInt(String.valueOf(panels.get("count"))) / 100.0)).intValue() + 1;
            }

            for (Map<String, Object> panel : (List<Map>) panels.get("results")) {

                if (String.valueOf(panel.get("version")).startsWith("0")) {
                    logger.warn("Panel is not ready for interpretation: '{}', version: '{}'", panel.get("name"), panel.get("version"));
                } else {
                    logger.info("Processing {} {} ...", panel.get("name"), panel.get("id"));

                    url = new URL("https://panelapp.genomicsengland.co.uk/api/v1/panels/" + panel.get("id") + "?format=json");
                    try (InputStream in = url.openStream()) {
                        Path path = originalsDirectory.resolve(panel.get("id") + ".json");
                        Files.copy(in, path);

                        DiseasePanel diseasePanel = DiseasePanelParsers.parsePanelApp(path);
                        JacksonUtils.getDefaultObjectMapper().writeValue(directory.resolve(diseasePanel.getId() + ".json").toFile(),
                                diseasePanel);

                        // Add to multijson file
                        fileWriter.write(JacksonUtils.getDefaultObjectMapper().writeValueAsString(diseasePanel));
                        fileWriter.write("\n");

                        Files.delete(path);
                        panelIds.add(diseasePanel.getId());
                    }
                }
            }

            i++;
        }

        try {
            fileWriter.close();
        } catch (IOException e) {
            logger.error("Error closing FileWriter: {}", e.getMessage(), e);
        }

        // Delete temporal directory
        Files.delete(originalsDirectory);

        // Generate file containing whole list of panel ids
        generatePanelIdsFile(directory, panelIds);
    }

    private void parseGeneCensus() throws IOException {
        AdminCliOptionsParser.CancerGeneCensusCommandOptions options = panelCommandOptions.cancerGeneCensusCommandOptions;

        Path directory = Paths.get(options.outdir);
        FileUtils.checkDirectory(directory, true);

        Path inputFile = Paths.get(options.input);
        FileUtils.checkFile(inputFile);

        logger.info("Processing {} panel...", inputFile.getFileName().toString());
        DiseasePanel diseasePanel = DiseasePanelParsers.parseCensus(inputFile);
        JacksonUtils.getDefaultObjectMapper().writeValue(directory.resolve(diseasePanel.getId() + ".json").toFile(),
                diseasePanel);

        generatePanelIdsFile(directory, Collections.singletonList(diseasePanel.getId()));
    }

    private void generatePanelIdsFile(Path directory, List<String> panelIds) throws IOException {
        // Generate file containing the list of all the panels processed
        try (FileWriter fw = new FileWriter(directory.resolve("panels.txt").toFile())) {
            for (String panelId : panelIds) {
                fw.write(panelId + "\n");
            }
        }
    }
}
