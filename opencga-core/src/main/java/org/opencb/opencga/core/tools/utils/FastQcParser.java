package org.opencb.opencga.core.tools.utils;

import org.opencb.opencga.core.models.variant.fastqc.*;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;
import java.util.Map;

public class FastQcParser {

    public static FastQcReport parse(File file) throws IOException {
        FastQcReport fastQcReport = new FastQcReport();

        FileReader fr = new FileReader(file);

        BufferedReader br = new BufferedReader(fr);

        // Skip first line
        br.readLine();

        String line;

        while ((line = br.readLine()) != null) {
            if (line.startsWith(">>")) {
                String status = line.split("\t")[1].toUpperCase();
                if (line.startsWith(">>Basic Statistics")) {
                    fastQcReport.getSummary().setBasicStatistics(status);
                    parseBasicStatistics(fastQcReport.getBasicStats(), br);
                } else if (line.startsWith(">>Per base sequence quality")) {
                    fastQcReport.getSummary().setPerBaseSeqQuality(status);
                    parsePerBaseSeqQuality(fastQcReport.getPerBaseSeqQualities(), br);
                } else if (line.startsWith(">>Per tile sequence quality")) {
                    fastQcReport.getSummary().setPerTileSeqQuality(status);
                    parsePerTileSeqQuality(fastQcReport.getPerTileSeqQualities(), br);
                } else if (line.startsWith(">>Per sequence quality scores")) {
                    fastQcReport.getSummary().setPerSeqQualityScores(status);
                    parsePerSeqQualityScores(fastQcReport.getPerSeqQualityScores(), br);
                } else if (line.startsWith(">>Per base sequence content")) {
                    fastQcReport.getSummary().setPerBaseSeqContent(status);
                    parsePerBaseSeqContent(fastQcReport.getPerBaseSeqContent(), br);
                } else if (line.startsWith(">>Per sequence GC content")) {
                    fastQcReport.getSummary().setPerSeqGcContent(status);
                    parsePerSeqGcContent(fastQcReport.getPerSeqGcContent(), br);
                } else if (line.startsWith(">>Per base N content")) {
                    fastQcReport.getSummary().setPerBaseNContent(status);
                    parsePerBaseNContent(fastQcReport.getPerBaseNContent(), br);
                } else if (line.startsWith(">>Sequence Length Distribution")) {
                    fastQcReport.getSummary().setSeqLengthDistribution(status);
                    parseSeqLengthDistribution(fastQcReport.getSeqLengthDistribution(), br);
                } else if (line.startsWith(">>Sequence Duplication Levels")) {
                    fastQcReport.getSummary().setSeqDuplicationLevels(status);
                    parseSeqDuplicationLevels(fastQcReport.getSeqDuplicationLevels(), br);
                } else if (line.startsWith(">>Overrepresented sequences")) {
                    fastQcReport.getSummary().setOverrepresentedSeqs(status);
                    parseOverrepresentedSeqs(fastQcReport.getOverrepresentedSeqs(), br);
                } else if (line.startsWith(">>Adapter Content")) {
                    fastQcReport.getSummary().setAdapterContent(status);
                    parseAdapterContent(fastQcReport.getAdapterContent(), br);
                } else if (line.startsWith(">>Kmer Content")) {
                    fastQcReport.getSummary().setKmerContent(status);
                    parseKmerContent(fastQcReport.getKmerContent(), br);
                }
            }
        }
        fr.close();

        return fastQcReport;

    }

    private static void parseKmerContent(List<KmerContent> kmerContent, BufferedReader br) throws IOException {
        String line;
        while ((line = br.readLine()) != null) {
            if (line.startsWith("#")) {
                continue;
            }
            if (line.startsWith(">>END_MODULE")) {
                return;
            }

            String[] fields = line.split("\t");
            // #Sequence	Count	PValue	Obs/Exp Max	Max Obs/Exp Position
            kmerContent.add(new KmerContent(fields[0], Integer.parseInt(fields[1]), Double.parseDouble(fields[2]),
                    Double.parseDouble(fields[3]), fields[4]));
        }
    }

    private static void parseAdapterContent(List<AdapterContent> adapterContent, BufferedReader br) throws IOException {
        String line;
        while ((line = br.readLine()) != null) {
            if (line.startsWith("#")) {
                continue;
            }
            if (line.startsWith(">>END_MODULE")) {
                return;
            }

            String[] fields = line.split("\t");
            // #Position	Illumina Universal Adapter	Illumina Small RNA 3' Adapter	Illumina Small RNA 5' Adapter	Nextera Transposase Sequence	SOLID S
            //        mall RNA Adapter
            adapterContent.add(new AdapterContent(fields[0], Double.parseDouble(fields[1]), Double.parseDouble(fields[2]),
                    Double.parseDouble(fields[3]), Double.parseDouble(fields[4]), Double.parseDouble(fields[5])));
        }

    }

    private static void parseOverrepresentedSeqs(List<OverrepresentedSeq> overrepresentedSeqs, BufferedReader br) throws IOException {
        String line;
        while ((line = br.readLine()) != null) {
            if (line.startsWith("#")) {
                continue;
            }
            if (line.startsWith(">>END_MODULE")) {
                return;
            }

            String[] fields = line.split("\t");
            // #Sequence	Count	Percentage	Possible Source
            overrepresentedSeqs.add(new OverrepresentedSeq(fields[0], Integer.parseInt(fields[1]), Double.parseDouble(fields[2]),
                    fields[3]));
        }
    }

    private static void parseSeqDuplicationLevels(List<SeqDuplicationLevel> seqDuplicationLevels, BufferedReader br) throws IOException {
        String line;
        while ((line = br.readLine()) != null) {
            if (line.startsWith("#")) {
                continue;
            }
            if (line.startsWith(">>END_MODULE")) {
                return;
            }

            String[] fields = line.split("\t");
            // #Duplication Level	Percentage of deduplicated	Percentage of total
            seqDuplicationLevels.add(new SeqDuplicationLevel(fields[0], Double.parseDouble(fields[1]), Double.parseDouble(fields[2])));
        }
    }

    private static void parseSeqLengthDistribution(Map<Integer, Double> seqLengthDistribution, BufferedReader br) throws IOException {
        String line;
        while ((line = br.readLine()) != null) {
            if (line.startsWith("#")) {
                continue;
            }
            if (line.startsWith(">>END_MODULE")) {
                return;
            }

            String[] fields = line.split("\t");
            // #Length	Count
            seqLengthDistribution.put(Integer.parseInt(fields[0]), Double.parseDouble(fields[1]));
        }
    }

    private static void parsePerBaseNContent(Map<String, Double> perBaseNContent, BufferedReader br) throws IOException {
        String line;
        while ((line = br.readLine()) != null) {
            if (line.startsWith("#")) {
                continue;
            }
            if (line.startsWith(">>END_MODULE")) {
                return;
            }

            String[] fields = line.split("\t");
            // #Base	N-Count
            perBaseNContent.put(fields[0], Double.parseDouble(fields[1]));
        }
    }

    private static void parsePerSeqGcContent(double[] perSeqGcContent, BufferedReader br) throws IOException {
        int i = 0;
        String line;
        while ((line = br.readLine()) != null) {
            if (line.startsWith("#")) {
                continue;
            }
            if (line.startsWith(">>END_MODULE")) {
                return;
            }

            String[] fields = line.split("\t");
            // #GC Content	Count
            perSeqGcContent[i++] = Double.parseDouble(fields[1]);
        }
    }

    private static void parsePerBaseSeqContent(List<PerBaseSeqContent> perBaseSeqContent, BufferedReader br) throws IOException {
        String line;
        while ((line = br.readLine()) != null) {
            if (line.startsWith("#")) {
                continue;
            }
            if (line.startsWith(">>END_MODULE")) {
                return;
            }

            String[] fields = line.split("\t");
            // #Base	G	A	T	C
            perBaseSeqContent.add(new PerBaseSeqContent(fields[0], Double.parseDouble(fields[1]), Double.parseDouble(fields[2]),
                    Double.parseDouble(fields[3]), Double.parseDouble(fields[4])));
        }
    }

    private static void parsePerTileSeqQuality(List<PerTileSeqQuality> perTileSeqQualities, BufferedReader br) throws IOException {
        String line;
        while ((line = br.readLine()) != null) {
            if (line.startsWith("#")) {
                continue;
            }
            if (line.startsWith(">>END_MODULE")) {
                return;
            }

            String[] fields = line.split("\t");
            // #Tile	Base	Mean
            perTileSeqQualities.add(new PerTileSeqQuality(fields[0], fields[1], Double.parseDouble(fields[2])));
        }
    }

    private static void parsePerSeqQualityScores(Map<Integer, Double> perSeqQualityScores, BufferedReader br) throws IOException {
        String line;
        while ((line = br.readLine()) != null) {
            if (line.startsWith("#")) {
                continue;
            }
            if (line.startsWith(">>END_MODULE")) {
                return;
            }

            String[] fields = line.split("\t");
            // #Quality	Count
            perSeqQualityScores.put(Integer.parseInt(fields[0]), Double.parseDouble(fields[1]));
        }
    }

    private static void parsePerBaseSeqQuality(List<PerBaseSeqQuality> perBaseSequenceQualities, BufferedReader br) throws IOException {
        String line;
        while ((line = br.readLine()) != null) {
            if (line.startsWith("#")) {
                continue;
            }
            if (line.startsWith(">>END_MODULE")) {
                return;
            }

            String[] fields = line.split("\t");
            // #Base	Mean	Median	Lower Quartile	Upper Quartile	10th Percentile	90th Percentile
            perBaseSequenceQualities.add(new PerBaseSeqQuality(fields[0], Double.parseDouble(fields[1]), Double.parseDouble(fields[2]),
                    Double.parseDouble(fields[3]), Double.parseDouble(fields[4]), Double.parseDouble(fields[5]),
                    Double.parseDouble(fields[6])));
        }
    }

    private static void parseBasicStatistics(Map<String, String> basicStats, BufferedReader br) throws IOException {
        String line;
        while ((line = br.readLine()) != null) {
            if (line.startsWith("#")) {
                continue;
            }
            if (line.startsWith(">>END_MODULE")) {
                return;
            }

            String[] fields = line.split("\t");
            basicStats.put(fields[0], fields[1]);
        }
    }

}
