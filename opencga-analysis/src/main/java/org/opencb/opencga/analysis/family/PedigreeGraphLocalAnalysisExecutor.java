/*
 * Copyright 2015-2020 OpenCB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.opencb.opencga.analysis.family;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.models.clinical.Disorder;
import org.opencb.commons.utils.DockerUtils;
import org.opencb.opencga.analysis.StorageToolExecutor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.core.common.GitRepositoryState;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.individual.Individual;
import org.opencb.opencga.core.tools.annotations.ToolExecutor;
import org.opencb.opencga.core.tools.family.PedigreeGraphAnalysisExecutor;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

@ToolExecutor(id="opencga-local", tool = PedigreeGraphAnalysis.ID,
        framework = ToolExecutor.Framework.LOCAL, source = ToolExecutor.Source.STORAGE)
public class PedigreeGraphLocalAnalysisExecutor extends PedigreeGraphAnalysisExecutor {

    public final static String R_DOCKER_IMAGE = "opencb/opencga-ext-tools:" + GitRepositoryState.get().getBuildVersion();

    private Path opencgaHome;

    private Logger logger = LoggerFactory.getLogger(this.getClass());

    @Override
    public void run() throws ToolException, CatalogException, IOException, StorageEngineException {
        opencgaHome = Paths.get(getExecutorParams().getString("opencgaHome"));
         // Run R script for fitting signature
        computeRScript();
    }

    private void computeRScript() throws IOException, ToolException, CatalogException {
        File pedFile = createPedFile();

        // Build command line to execute
        List<AbstractMap.SimpleEntry<String, String>> inputBindings = new ArrayList<>();
        inputBindings.add(new AbstractMap.SimpleEntry<>(opencgaHome.resolve("analysis/" + PedigreeGraphAnalysis.ID).toAbsolutePath()
                .toString(), "/script"));

        AbstractMap.SimpleEntry<String, String> outputBinding = new AbstractMap.SimpleEntry<>(getOutDir().toAbsolutePath().toString(),
                "/data");

        StringBuilder scriptParams = new StringBuilder("R CMD Rscript --vanilla")
                .append(" /script/ped.R")
                .append(" /data/").append(pedFile.getName())
                .append(" /data/")
                .append(" --plot_format png");

        String cmdline = DockerUtils.run(R_DOCKER_IMAGE, inputBindings, outputBinding, scriptParams.toString(), null);
        logger.info("Docker command line: " + cmdline);
    }

    private File createPedFile() throws FileNotFoundException {
        File file = getOutDir().resolve(PedigreeGraphAnalysis.PEDIGREE_FILENAME).toFile();
        try (PrintWriter pw = new PrintWriter(file)) {
            List<Disorder> disorders = getFamily().getDisorders();
            StringBuilder sbDisorders = new StringBuilder();
            if (disorders.size() == 1) {
                sbDisorders.append("affected").append("\t");
            } else {
                for (Disorder disorder : getFamily().getDisorders()) {
                    sbDisorders.append("affected.").append(disorder.getId()).append("\t");
                }
            }
            pw.println("id\tdadid\tmomid\tsex\t" + sbDisorders + "status\trelation");

            for (Individual member : getFamily().getMembers()) {
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
                        case UNKNOWN:
                            sb.append("3").append("\t");
                            break;
                        case UNDETERMINED:
                            sb.append("4").append("\t");
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
                    sb.append("NA").append("\t");
                } else {
                    switch (member.getLifeStatus()) {
                        case ALIVE:
                            sb.append("0").append("\t");
                            break;
                        case DECEASED:
                        case ABORTED:
                        case STILLBORN:
                        case MISCARRIAGE:
                            sb.append("1").append("\t");
                            break;
                        default:
                            sb.append("NA").append("\t");
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
