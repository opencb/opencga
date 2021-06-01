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
    private String assembly;
    private String sample;
    private String queryId;
    private String queryDescription;
    private ObjectMap query;
    private boolean fitting;

//    private String sampleName;
//    private Path refGenomePath;
//    private Path mutationalSignaturePath;
//    private Path openCgaHome;

    public MutationalSignatureAnalysisExecutor() {
    }

//    public String getStudy() {
//        return study;
//    }
//
//    public MutationalSignatureAnalysisExecutor setStudy(String study) {
//        this.study = study;
//        return this;
//    }
//
//    public String getSampleName() {
//        return sampleName;
//    }
//
//    public MutationalSignatureAnalysisExecutor setSampleName(String sampleName) {
//        this.sampleName = sampleName;
//        return this;
//    }
//
//    public Path getRefGenomePath() {
//        return refGenomePath;
//    }
//
//    public MutationalSignatureAnalysisExecutor setRefGenomePath(Path refGenomePath) {
//        this.refGenomePath = refGenomePath;
//        return this;
//    }
//
//    public Path getMutationalSignaturePath() {
//        return mutationalSignaturePath;
//    }
//
//    public MutationalSignatureAnalysisExecutor setMutationalSignaturePath(Path mutationalSignaturePath) {
//        this.mutationalSignaturePath = mutationalSignaturePath;
//        return this;
//    }
//
//    public Path getOpenCgaHome() {
//        return openCgaHome;
//    }
//
//    public MutationalSignatureAnalysisExecutor setOpenCgaHome(Path openCgaHome) {
//        this.openCgaHome = openCgaHome;
//        return this;
//    }

    public static String getContextIndexFilename(String sampleName) {
        return "OPENCGA_" + sampleName + "_genome_context.csv";
    }

    protected static Map<String, Map<String, Double>> initFreqMap() {
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

    protected static void writeCountMap(Map<String, Map<String, Double>> map, File outputFile) throws ToolException {
        double sum = sumFreqMap(map);
        try {
            PrintWriter pw = new PrintWriter(outputFile);
            pw.println("Substitution Type\tTrinucleotide\tSomatic Mutation Type\tCount\tNormalized Count");
            for (String firstKey : FIRST_LEVEL_KEYS) {
                String[] secondLevelKeys = firstKey.startsWith("C") ? SECOND_LEVEL_KEYS_C : SECOND_LEVEL_KEYS_T;
                for (String secondKey : secondLevelKeys) {
                    pw.println(firstKey + "\t" + secondKey + "\t" + secondKey.substring(0, 1) + "[" + firstKey + "]"
                            + secondKey.substring(2) + "\t" + map.get(firstKey).get(secondKey) + "\t"
                            + (map.get(firstKey).get(secondKey) / sum));
                }
            }
            pw.close();
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

//    private MutationalSignature parse(Path dir) throws IOException {
//        MutationalSignature result = new MutationalSignature();
//
//        // Context counts
//        File contextFile = dir.resolve("context.txt").toFile();
//        if (contextFile.exists()) {
//            List<String> lines = FileUtils.readLines(contextFile, Charset.defaultCharset());
//            Signature.SignatureCount[] sigCounts = new Signature.SignatureCount[lines.size() - 1];
//            for (int i = 1; i < lines.size(); i++) {
//                String[] fields = lines.get(i).split("\t");
//                sigCounts[i-1] = new Signature.SignatureCount(fields[2], Math.round(Float.parseFloat((fields[3]))));
//            }
//            result.setSignature(new Signature("SNV", sigCounts));
//        }
//
//        // Signatures coefficients
//        File coeffsFile = dir.resolve("signature_coefficients.json").toFile();
//        if (coeffsFile.exists()) {
//            SignatureFitting fitting = new SignatureFitting()
//                    .setMethod("GEL")
//                    .setSignatureSource("Cosmic")
//                    .setSignatureVersion("2.0");
//
//            Map content = JacksonUtils.getDefaultObjectMapper().readValue(coeffsFile, Map.class);
//            Map coefficients = (Map) content.get("coefficients");
//            SignatureFitting.Score[] scores = new SignatureFitting.Score[coefficients.size()];
//            int i = 0;
//            for (Object key : coefficients.keySet()) {
//                Number coeff = (Number) coefficients.get(key);
//                scores[i++] = new SignatureFitting.Score((String) key, coeff.doubleValue());
//            }
//            fitting.setScores(scores);
//            fitting.setCoeff((Double) content.get("rss"));
//
//            // Signature summary image
//            File imgFile = dir.resolve("signature_summary.png").toFile();
//            if (imgFile.exists()) {
//                FileInputStream fileInputStreamReader = new FileInputStream(imgFile);
//                byte[] bytes = new byte[(int) imgFile.length()];
//                fileInputStreamReader.read(bytes);
//
//                fitting.setImage(new String(Base64.getEncoder().encode(bytes), "UTF-8"));
//            }
//
//            result.setFitting(fitting);
//        }
//
//        return result;
//    }

    public String getStudy() {
        return study;
    }

    public MutationalSignatureAnalysisExecutor setStudy(String study) {
        this.study = study;
        return this;
    }

    public String getAssembly() {
        return assembly;
    }

    public MutationalSignatureAnalysisExecutor setAssembly(String assembly) {
        this.assembly = assembly;
        return this;
    }

    public String getSample() {
        return sample;
    }

    public MutationalSignatureAnalysisExecutor setSample(String sample) {
        this.sample = sample;
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

    public boolean isFitting() {
        return fitting;
    }

    public MutationalSignatureAnalysisExecutor setFitting(boolean fitting) {
        this.fitting = fitting;
        return this;
    }
}
