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

package org.opencb.opencga.core.tools.variant;

import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.tools.OpenCgaToolExecutor;

import java.io.File;
import java.io.PrintWriter;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;

public abstract class MutationalSignatureAnalysisExecutor extends OpenCgaToolExecutor {

    private static String[] FIRST_LEVEL_KEYS = new String[]{"C>A", "C>G", "C>T", "T>A", "T>C", "T>G"};
    private static String[] SECOND_LEVEL_KEYS_C = new String[]{"ACA", "ACC", "ACG", "ACT", "CCA", "CCC", "CCG", "CCT", "GCA", "GCC", "GCG",
            "GCT", "TCA", "TCC", "TCG", "TCT"};
    private static String[] SECOND_LEVEL_KEYS_T = new String[]{"ATA", "ATC", "ATG", "ATT", "CTA", "CTC", "CTG", "CTT", "GTA", "GTC", "GTG",
            "GTT", "TTA", "TTC", "TTG", "TTT"};

    private String study;
    private String sampleName;
    private Path refGenomePath;
    private Path mutationalSignaturePath;

    public MutationalSignatureAnalysisExecutor() {
    }

    public String getStudy() {
        return study;
    }

    public MutationalSignatureAnalysisExecutor setStudy(String study) {
        this.study = study;
        return this;
    }

    public String getSampleName() {
        return sampleName;
    }

    public MutationalSignatureAnalysisExecutor setSampleName(String sampleName) {
        this.sampleName = sampleName;
        return this;
    }

    public Path getRefGenomePath() {
        return refGenomePath;
    }

    public MutationalSignatureAnalysisExecutor setRefGenomePath(Path refGenomePath) {
        this.refGenomePath = refGenomePath;
        return this;
    }

    public Path getMutationalSignaturePath() {
        return mutationalSignaturePath;
    }

    public MutationalSignatureAnalysisExecutor setMutationalSignaturePath(Path mutationalSignaturePath) {
        this.mutationalSignaturePath = mutationalSignaturePath;
        return this;
    }

    protected String getContextIndexFilename(String sampleName) {
        return "OPENCGA_" + getSampleName() + "_mutational_signature_context.csv";
    }


    protected Map<String, Map<String, Double>> initFreqMap() {
        Map<String, Map<String, Double>> map = new LinkedHashMap<>();
        for (String firstKey : FIRST_LEVEL_KEYS) {
            Map<String, Double> secondMap = new LinkedHashMap<>();
            String[] secondLevelKeys = firstKey.startsWith("C") ? SECOND_LEVEL_KEYS_C : SECOND_LEVEL_KEYS_T;
            for (String secondKey : secondLevelKeys) {
                secondMap.put(secondKey, 0.0d);
            }
            map.put(firstKey, secondMap);
        }

        return map;
    }

    protected void writeCountMap(Map<String, Map<String, Double>> map, File outputFile) throws ToolException {
        double sum = sumFreqMap(map);
        try {
            PrintWriter pw = new PrintWriter(outputFile);
            pw.println("Substitution Type\tTrinucleotide\tSomatic Mutation Type\tCount\tNormalized Count");
            for (String firstKey : FIRST_LEVEL_KEYS) {
                String[] secondLevelKeys = firstKey.startsWith("C") ? SECOND_LEVEL_KEYS_C : SECOND_LEVEL_KEYS_T;
                for (String secondKey : secondLevelKeys) {
                    pw.println(firstKey + "\t" + secondKey + "\t" + secondKey.substring(0,1) + "[" + firstKey + "]"
                                    + secondKey.substring(2) + "\t" + map.get(firstKey).get(secondKey) + "\t"
                            + (map.get(firstKey).get(secondKey) / sum));
                }
            }
            pw.close();
        } catch (Exception e) {
            throw new ToolException("Error writing output file: " + outputFile.getName(), e);
        }
    }

    private Double sumFreqMap(Map<String, Map<String, Double>> map) {
        double sum = 0;
        for (String firstKey : FIRST_LEVEL_KEYS) {
            String[] secondLevelKeys = firstKey.startsWith("C") ? SECOND_LEVEL_KEYS_C : SECOND_LEVEL_KEYS_T;
            for (String secondKey : secondLevelKeys) {
                sum += map.get(firstKey).get(secondKey);
            }
        }

        return sum;
    }
}
