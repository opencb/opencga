package org.opencb.opencga.core.tools.variant;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.tools.OpenCgaToolExecutor;

import java.io.File;
import java.io.PrintWriter;
import java.nio.file.Path;
import java.util.*;

public abstract class MutationalSignatureAnalysisExecutor extends OpenCgaToolExecutor {

    private static String[] FIRST_LEVEL_KEYS = new String[]{"C>A", "C>G", "C>T", "T>A", "T>C", "T>G"};
    private static String[] SECOND_LEVEL_KEYS_C = new String[]{"ACA", "ACC", "ACG", "ACT", "CCA", "CCC", "CCG", "CCT", "GCA", "GCC", "GCG",
            "GCT", "TCA", "TCC", "TCG", "TCT"};
    private static String[] SECOND_LEVEL_KEYS_T = new String[]{"ATA", "ATC", "ATG", "ATT", "CTA", "CTC", "CTG", "CTT", "GTA", "GTC", "GTG",
            "GTT", "TTA", "TTC", "TTG", "TTT"};

    private String study;
    private String sampleName;
    private Path outputFile;

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

    public Path getOutputFile() {
        return outputFile;
    }

    public MutationalSignatureAnalysisExecutor setOutputFile(Path outputFile) {
        this.outputFile = outputFile;
        return this;
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

    protected void normalizeFreqMap(Map<String, Map<String, Double>> map) {
        double sum = sumFreqMap(map);
        for (String firstKey : FIRST_LEVEL_KEYS) {
            String[] secondLevelKeys = firstKey.startsWith("C") ? SECOND_LEVEL_KEYS_C : SECOND_LEVEL_KEYS_T;
            for (String secondKey : secondLevelKeys) {
                map.get(firstKey).put(secondKey, map.get(firstKey).get(secondKey) / sum);
            }
        }
    }

    protected void writeResult(Map<String, Map<String, Double>> counterMap) throws ToolException {
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.configure(MapperFeature.REQUIRE_SETTERS_FOR_GETTERS, true);
        objectMapper.configure(DeserializationFeature.FAIL_ON_IGNORED_PROPERTIES, false);
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        objectMapper.configure(DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES, false);
        objectMapper.configure(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY, true);

        File outFilename = getOutputFile().toFile();
        try {
            PrintWriter pw = new PrintWriter(outFilename);
            pw.println(objectMapper.writer().writeValueAsString(counterMap));
            pw.close();
        } catch (Exception e) {
            throw new ToolException("Error writing output file: " + outFilename, e);
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
