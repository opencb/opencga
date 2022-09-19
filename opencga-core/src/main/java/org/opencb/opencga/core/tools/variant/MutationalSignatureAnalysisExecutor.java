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

import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.tools.OpenCgaToolExecutor;

import java.io.File;
import java.io.PrintWriter;
import java.util.LinkedHashMap;
import java.util.Map;

public abstract class MutationalSignatureAnalysisExecutor extends OpenCgaToolExecutor {

    private static String[] FIRST_LEVEL_KEYS = new String[]{"C>A", "C>G", "C>T", "T>A", "T>C", "T>G"};
    private static String[] SECOND_LEVEL_KEYS_C = new String[]{"ACA", "ACC", "ACG", "ACT", "CCA", "CCC", "CCG", "CCT", "GCA", "GCC", "GCG",
            "GCT", "TCA", "TCC", "TCG", "TCT"};
    private static String[] SECOND_LEVEL_KEYS_T = new String[]{"ATA", "ATC", "ATG", "ATT", "CTA", "CTC", "CTG", "CTT", "GTA", "GTC", "GTG",
            "GTT", "TTA", "TTC", "TTG", "TTT"};

    private String study;
    private String sample;
    private String assembly;
    private String queryId;
    private String queryDescription;
    private ObjectMap query;

    // For fitting signature
    private String catalogues;
    private String fitMethod;
    private int nBoot;
    private String sigVersion;
    private String organ;
    private float thresholdPerc;
    private float thresholdPval;
    private int maxRareSigs;

    public MutationalSignatureAnalysisExecutor() {
    }

    protected String getContextIndexFilename() {
        return "OPENCGA_" + sample + "_" + assembly + "_genome_context.csv";
    }

    protected String getMutationalSignatureFilename() {
        return "result_" + sigVersion + "_" + assembly + ".txt";
    }

    protected static Map<String, Map<String, Integer>> initCountMap() {
        Map<String, Map<String, Integer>> map = new LinkedHashMap<>();
        for (String firstKey : FIRST_LEVEL_KEYS) {
            Map<String, Integer> secondMap = new LinkedHashMap<>();
            String[] secondLevelKeys = firstKey.startsWith("C") ? SECOND_LEVEL_KEYS_C : SECOND_LEVEL_KEYS_T;
            for (String secondKey : secondLevelKeys) {
                secondMap.put(secondKey, 0);
            }
            map.put(firstKey, secondMap);
        }

        return map;
    }

    protected static void writeCountMap(String sample, Map<String, Map<String, Integer>> map, File outputFile) throws ToolException {
        //double sum = sumFreqMap(map);
        try (PrintWriter pw = new PrintWriter(outputFile)) {
            pw.println(sample);
            for (String firstKey : FIRST_LEVEL_KEYS) {
                String[] secondLevelKeys = firstKey.startsWith("C") ? SECOND_LEVEL_KEYS_C : SECOND_LEVEL_KEYS_T;
                for (String secondKey : secondLevelKeys) {
                    pw.println(secondKey.substring(0, 1) + "[" + firstKey + "]" + secondKey.substring(2) + "\t"
                            + map.get(firstKey).get(secondKey));
                }
            }
        } catch (Exception e) {
            throw new ToolException("Error writing output file: " + outputFile.getName(), e);
        }
    }

    protected static String complement(String in) {
        char[] inArray = in.toCharArray();
        char[] outArray = new char[inArray.length];
        for (int i = 0; i < inArray.length; i++) {
            switch (inArray[i]) {
                case 'A':
                    outArray[i] = 'T';
                    break;
                case 'T':
                    outArray[i] = 'A';
                    break;
                case 'G':
                    outArray[i] = 'C';
                    break;
                case 'C':
                    outArray[i] = 'G';
                    break;
                default:
                    outArray[i] = inArray[i];
                    break;
            }
        }
        return new String(outArray);
    }

    protected static String reverseComplement(String in) {
        char[] inArray = in.toCharArray();
        char[] outArray = new char[inArray.length];
        for (int i = 0; i < inArray.length; i++) {
            switch (inArray[i]) {
                case 'A':
                    outArray[inArray.length - i - 1] = 'T';
                    break;
                case 'T':
                    outArray[inArray.length - i - 1] = 'A';
                    break;
                case 'G':
                    outArray[inArray.length - i - 1] = 'C';
                    break;
                case 'C':
                    outArray[inArray.length - i - 1] = 'G';
                    break;
                default:
                    outArray[inArray.length - i - 1] = inArray[i];
                    break;
            }
        }
        return new String(outArray);
    }

    private static Double sumFreqMap(Map<String, Map<String, Double>> map) {
        double sum = 0;
        for (String firstKey : FIRST_LEVEL_KEYS) {
            String[] secondLevelKeys = firstKey.startsWith("C") ? SECOND_LEVEL_KEYS_C : SECOND_LEVEL_KEYS_T;
            for (String secondKey : secondLevelKeys) {
                sum += map.get(firstKey).get(secondKey);
            }
        }

        return sum;
    }

    public String getStudy() {
        return study;
    }

    public MutationalSignatureAnalysisExecutor setStudy(String study) {
        this.study = study;
        return this;
    }

    public String getSample() {
        return sample;
    }

    public MutationalSignatureAnalysisExecutor setSample(String sample) {
        this.sample = sample;
        return this;
    }

    public String getAssembly() {
        return assembly;
    }

    public MutationalSignatureAnalysisExecutor setAssembly(String assembly) {
        this.assembly = assembly;
        return this;
    }

    public String getQueryId() {
        return queryId;
    }

    public MutationalSignatureAnalysisExecutor setQueryId(String queryId) {
        this.queryId = queryId;
        return this;
    }

    public String getQueryDescription() {
        return queryDescription;
    }

    public MutationalSignatureAnalysisExecutor setQueryDescription(String queryDescription) {
        this.queryDescription = queryDescription;
        return this;
    }

    public ObjectMap getQuery() {
        return query;
    }

    public MutationalSignatureAnalysisExecutor setQuery(ObjectMap query) {
        this.query = query;
        return this;
    }

    public String getCatalogues() {
        return catalogues;
    }

    public MutationalSignatureAnalysisExecutor setCatalogues(String catalogues) {
        this.catalogues = catalogues;
        return this;
    }

    public String getFitMethod() {
        return fitMethod;
    }

    public MutationalSignatureAnalysisExecutor setFitMethod(String fitMethod) {
        this.fitMethod = fitMethod;
        return this;
    }

    public int getnBoot() {
        return nBoot;
    }

    public MutationalSignatureAnalysisExecutor setnBoot(int nBoot) {
        this.nBoot = nBoot;
        return this;
    }

    public String getSigVersion() {
        return sigVersion;
    }

    public MutationalSignatureAnalysisExecutor setSigVersion(String sigVersion) {
        this.sigVersion = sigVersion;
        return this;
    }

    public String getOrgan() {
        return organ;
    }

    public MutationalSignatureAnalysisExecutor setOrgan(String organ) {
        this.organ = organ;
        return this;
    }

    public float getThresholdPerc() {
        return thresholdPerc;
    }

    public MutationalSignatureAnalysisExecutor setThresholdPerc(float thresholdPerc) {
        this.thresholdPerc = thresholdPerc;
        return this;
    }

    public float getThresholdPval() {
        return thresholdPval;
    }

    public MutationalSignatureAnalysisExecutor setThresholdPval(float thresholdPval) {
        this.thresholdPval = thresholdPval;
        return this;
    }

    public int getMaxRareSigs() {
        return maxRareSigs;
    }

    public MutationalSignatureAnalysisExecutor setMaxRareSigs(int maxRareSigs) {
        this.maxRareSigs = maxRareSigs;
        return this;
    }
}
