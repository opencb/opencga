package org.opencb.opencga.analysis.wrappers.executors;

import org.junit.Test;
import org.opencb.biodata.formats.alignment.samtools.SamtoolsFlagstats;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.Assert.*;

public class SamtoolsWrapperAnalysisExecutorTest {

    @Test
    public void test() throws IOException {
        Path path = Paths.get("/opt/opencga/sessions/jobs/JOBS/titi/20200626/sample-qc.20200626082736.WOtcTM/flagstat/scratch/fastqc.stdout.txt");
        SamtoolsFlagstats flagstats = parse(path);
        System.out.println(flagstats);
    }

    public SamtoolsFlagstats parse(Path path) throws IOException {

        SamtoolsFlagstats flagstats = new SamtoolsFlagstats();

        FileReader fr = new FileReader(path.toFile());
        BufferedReader br = new BufferedReader(fr);

        String line;

        while ((line = br.readLine()) != null) {
            String[] splits = line.split(" ");
            int passed = Integer.parseInt(splits[0]);
            int failed = Integer.parseInt(splits[2]);
            if (line.contains("QC-passed")) {
                flagstats.setTotalQcPassed(passed);
                flagstats.setTotalReads(passed + failed);
            } else if (line.contains("secondary")) {
                flagstats.setSecondaryAlignments(passed);
            } else if (line.contains("supplementary")) {
                flagstats.setSupplementary(passed);
            } else if (line.contains("duplicates")) {
                flagstats.setDuplicates(passed);
            } else if (line.contains("mapped")) {
                flagstats.setMapped(passed);
            } else if (line.contains("paired in sequencing")) {
                flagstats.setPairedInSequencing(passed);
            } else if (line.contains("read1")) {
                flagstats.setRead1(passed);
            } else if (line.contains("read2")) {
                flagstats.setRead2(passed);
            } else if (line.contains("properly paired")) {
                flagstats.setProperlyPaired(passed);
            } else if (line.contains("with itself and mate mapped")) {
                flagstats.setSelfAndMateMapped(passed);
            } else if (line.contains("singletons")) {
                flagstats.setSingletons(passed);
            } else if (line.contains("mapQ>=5")) {
                flagstats.setDiffChrMapQ5(passed);
            } else if (line.contains("with mate mapped")) {
                flagstats.setMateMappedToDiffChr(passed);
            }
        }

        return flagstats;
    }

//        108 + 0 in total (QC-passed reads + QC-failed reads)
//        0 + 0 secondary
//        0 + 0 supplementary
//        0 + 0 duplicates
//        108 + 0 mapped (100.00% : N/A)
//        108 + 0 paired in sequencing
//        53 + 0 read1
//        55 + 0 read2
//        104 + 0 properly paired (96.30% : N/A)
//        108 + 0 with itself and mate mapped
//        0 + 0 singletons (0.00% : N/A)
//        0 + 0 with mate mapped to a different chr
//        0 + 0 with mate mapped to a different chr (mapQ>=5)
}