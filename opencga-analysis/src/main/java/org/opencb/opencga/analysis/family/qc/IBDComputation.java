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

package org.opencb.opencga.analysis.family.qc;

import org.apache.commons.math3.ode.nonstiff.RungeKuttaFieldIntegrator;
import org.opencb.biodata.models.clinical.qc.RelatednessReport;
import org.opencb.biodata.models.clinical.qc.RelatednessScore;
import org.opencb.commons.utils.DockerUtils;
import org.opencb.opencga.analysis.individual.qc.IndividualQcUtils;
import org.opencb.opencga.analysis.variant.manager.VariantStorageManager;
import org.opencb.opencga.analysis.wrappers.plink.PlinkWrapperAnalysisExecutor;
import org.opencb.opencga.core.exceptions.ToolException;

import java.io.*;
import java.nio.file.Path;
import java.util.*;

public class IBDComputation {

    private static final String BASENAME = "variants";

    public static RelatednessReport compute(String study, List<String> samples, String maf, Path outDir,
                                            VariantStorageManager storageManager, String token) throws ToolException {
        // Select markers
        IndividualQcUtils.selectMarkers(BASENAME, study, samples, maf, outDir, storageManager, token);

        // run IBD and return the result file (now autosome-file comprises X chromosome too)
        File outFile = runIBD(BASENAME, outDir);

        if (!outFile.exists()) {
            throw new ToolException("Something wrong happened executing relatedness analysis");
        }

        RelatednessReport relatedness = new RelatednessReport()
                .setMethod("PLINK/IBD")
                .setMaf(maf)
                .setScores(parseRelatednessScores(outFile));

        return relatedness;
    }

    private static File runIBD(String basename, Path outDir) throws ToolException {
        AbstractMap.SimpleEntry<String, String> outputBinding = new AbstractMap.SimpleEntry<>(outDir.toAbsolutePath().toString(),
                "/data/output");

        // Calculate allele frequencies to pass to PLINK: --read-freq <.freq filename>
        String readFreq = "";
        String readFreqFilename = basename + ".maf.freq";
        createReadFreqFile(outputBinding.getKey() + "/" + basename + ".tped", outputBinding.getKey() + "/" + readFreqFilename);
        if (new File(outputBinding.getKey() + "/" + readFreqFilename).exists()) {
            readFreq = " --read-freq /data/output/" + readFreqFilename;
        }

        // Run IBD using PLINK in docker
        String exclude = "";
        File pruneOutFile = new File(outputBinding.getKey() + "/" + basename + ".prune.out");
        if (pruneOutFile.exists() && pruneOutFile.length() > 0) {
            exclude = " --exclude /data/output/" + basename + ".prune.out";
        }
        String plinkParams = "plink1.9 --tfile /data/output/" + basename + " --genome rel-check " + readFreq + exclude + " --out /data/output/"
                + basename;
        try {
            PlinkWrapperAnalysisExecutor plinkExecutor = new PlinkWrapperAnalysisExecutor();
            DockerUtils.run(plinkExecutor.getDockerImageName(), null, outputBinding, plinkParams, null);
        } catch (IOException e) {
            throw new ToolException(e);
        }

        // Check output file
        File outFile = new File(outputBinding.getKey() + "/" + basename + ".genome");
        if (!outFile.exists()) {
            throw new ToolException("Something wrong executing relatedness analysis (i.e., IBD/IBS computation) in PLINK docker.");
        }

        return outFile;
    }

    private static void createReadFreqFile(String tpedFilename, String mafFilename) throws ToolException {
        try (FileWriter f = new FileWriter(mafFilename, true);
             PrintWriter p = new PrintWriter(new BufferedWriter(f))) {

            p.println("CHR\tSNP\tA1\tA2\tMAF\tNCHROBS");

            FileInputStream fis = new FileInputStream(tpedFilename);
            Scanner sc = new Scanner(fis);
            while(sc.hasNextLine()) {
                String[] splits = sc.nextLine().split("\t");
                String[] varSplits = splits[1].split(":");

                int a1 = 0;
                int a2 = 0;
                for (int i = 4; i < splits.length; i++) {
                    if (splits[i].equals(varSplits[2])) {
                        a1++;
                    } else if (splits[i].equals(varSplits[3])) {
                        a2++;
                    }
                }
                p.println(splits[0] + "\t" + splits[1] + "\t" + varSplits[2] + "\t" + varSplits[3] + "\t" + (1.0d * a2 / (a1 + a2)) + "\t"
                        + (a1 + a2));
            }
        } catch (IOException e) {
            throw new ToolException(e);
        }
    }

    public static List<RelatednessScore> parseRelatednessScores(File file) throws ToolException {
        List<RelatednessScore> scores = new ArrayList<>();

        String line;
        BufferedReader reader;
        try {
            reader = new BufferedReader(new FileReader(file));

            // Skip first line is the header
            // FID1         IID1 FID2         IID2 RT    EZ      Z0      Z1      Z2  PI_HAT PHE       DST     PPC   RATIO
            reader.readLine();

            while ((line = reader.readLine()) != null) {
                String[] splits = line.trim().split("\\s+");

                // Create relatedness score
                RelatednessScore score = new RelatednessScore();
                score.setSampleId1(splits[1]);
                score.setSampleId2(splits[3]);

                score.setReportedRelationship(getReportedRelationshipDescription(splits[4]));
                score.setInferredRelationship(inferredRelationship(splits[6], splits[7], splits[8], splits[9], splits[4]));

                Map<String, Object> values = new LinkedHashMap<>();
                values.put("ez", splits[5]);
                values.put("z0", splits[6]);
                values.put("z1", splits[7]);
                values.put("z2", splits[8]);
                values.put("PiHat", splits[9]);
                score.setValues(values);

                // Add relatedness score to the report
                scores.add(score);
            }
            reader.close();
        } catch (IOException e) {
            throw new ToolException(e);
        }

        return scores;
    }

    public static String inferredRelationship(String z0, String z1, String z2, String piHat, String reportedRelationship) {
        String result = "unknown";

        float z0Score;
        float z1Score;
        float z2Score;
        float piHatScore;

        // Sanity check
        try {
            z0Score = Float.parseFloat(z0);
        } catch (NumberFormatException e) {
            return result;
        }
        try {
            z1Score = Float.parseFloat(z1);
        } catch (NumberFormatException e) {
            return result;
        }
        try {
            z2Score = Float.parseFloat(z2);
        } catch (NumberFormatException e) {
            return result;
        }
        try {
            piHatScore = Float.parseFloat(piHat);
        } catch (NumberFormatException e) {
            return result;
        }

        //              z0	   z1    z2    PiHat
        // Identical	0	   0	 1     1
        if (z0Score <= 0.1f && z1Score <= 0.1f && z2Score >= 0.9f && piHatScore >= 0.9) {
            return "identical twins";
        }

        //              z0	   z1    z2    PiHat
        // Parent	    0	   1	 0     0.5
        if (z0Score <= 0.1f && z1Score >= 0.9f && z2Score <= 0.1f && piHatScore >= 0.4f && piHatScore <= 0.6f) {
            return "parent-offspring";
        }

        //              z0	   z1    z2    PiHat
        // Sibling	    0.25   0.5   0.25  0.5
        if (z0Score >= 0.15f && z0Score <= 0.35f
                && z1Score >= 0.4f && z1Score <= 0.6f
                && z2Score >= 0.15f && z2Score <= 0.35f
                && piHatScore >= 0.4f && piHatScore <= 0.6f) {
            return "full siblings";
        }

        //              z0	   z1    z2    PiHat
        // Grandparent	0.5    0.5   0     0.25
        // Half sibling	0.5    0.5   0     0.25
        if (z0Score >= 0.4f && z0Score <= 0.6f
                && z1Score >= 0.4f && z1Score <= 0.6f
                && z2Score <= 0.1f
                && piHatScore >= 0.15f && piHatScore <= 0.35f) {
            if (reportedRelationship.equals("HS")) {
                return "half siblings";
            } else {
                return "grandparent";
            }
        }

        //              z0	   z1    z2    PiHat
        // Cousins	    0.75   0.25	 0	   0.15
        if (z0Score >= 0.65f && z0Score <= 0.85f
                && z1Score >= 0.15f && z1Score <= 0.35f
                && z2Score <= 0.1f
                && piHatScore >= 0.05f && piHatScore <= 0.25f) {
            return "cousins";
        }

        return result;
    }

    public static String getReportedRelationshipDescription(String value) {
        switch (value) {
            case "FS":
                return "full siblings";
            case "HS":
                return "half siblings";
            case "PO":
                return "parent-offspring";
            case "OT":
                return "other";
            default:
                return value;
        }
    }
}
