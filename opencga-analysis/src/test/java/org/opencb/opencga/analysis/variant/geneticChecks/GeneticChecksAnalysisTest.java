package org.opencb.opencga.analysis.variant.geneticChecks;

import org.broad.igv.bbfile.BigWigIterator;
import org.broad.igv.bbfile.WigItem;
import org.junit.Test;
import org.opencb.biodata.models.alignment.RegionCoverage;
import org.opencb.biodata.models.core.Chromosome;
import org.opencb.biodata.models.core.Region;
import org.opencb.biodata.tools.feature.BigWigManager;
import org.opencb.opencga.storage.core.alignment.local.LocalAlignmentDBAdaptor;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;

public class GeneticChecksAnalysisTest {

    @Test
    public void coverage1() throws IOException {
//        time samtools depth -r 1:4999730-4999740 daughter.bam  | awk 'BEGIN{sum=0} {sum+=$3} END{print "avg:" sum/NR}'
//        avg:1.18182

        Path path = Paths.get("/home/jtarraga/data150/test/corpasome/daughter.bam.bs1.bw");
        BigWigManager bwManager = new BigWigManager(path);

        int windowSize = 1;

        Region region = new Region("1", 4999730, 4999740);
        String chrom = region.getChromosome();
        int start = region.getStart();
        int end = region.getEnd();

        int zoomLevel = -1;
        for (int level = 0; level < bwManager.getZoomWindowSizes().size(); level++) {
            if (windowSize < bwManager.getZoomWindowSizes().get(level)) {
                break;
            }
            zoomLevel++;
        }

        // Calculate the number of needed windows, ensure windowSize => 1
        windowSize = Math.max(1, windowSize);
        int numWindows = (end - start + 1) / windowSize;
        if ((end - start + 1) % windowSize != 0) {
            numWindows++;
            end = start + (numWindows * windowSize - 1);
        }
        float[] chunks = new float[numWindows];

        BigWigIterator bigWigIterator = bwManager.iterator(new Region(chrom, start, end));
        WigItem wItem;
        int length, chunkStart, chunkEnd;
        while (bigWigIterator.hasNext()) {
            wItem = bigWigIterator.next();
            System.out.println("wItem: start = " + wItem.getStartBase() + ", end = " + wItem.getEndBase() + ", value = " + wItem.getWigValue());

            chunkStart = (Math.max(start, wItem.getStartBase()) - start) / windowSize;
            chunkEnd = (Math.min(end, wItem.getEndBase()) - start) / windowSize;
            System.out.println("(chunkStart, chunkEnd) = (" + chunkStart + ", " + chunkEnd + ")");

            for (int chunk = chunkStart; chunk <= chunkEnd; chunk++) {
                length = Math.min(wItem.getEndBase() - start, chunk * windowSize + windowSize)
                        - Math.max(wItem.getStartBase() - start, chunk * windowSize);
                if (chunk < chunks.length) {
                    chunks[chunk] += (wItem.getWigValue() * length);
                }
            }
        }

        for (int i = 0; i < chunks.length; i++) {
            chunks[i] /= windowSize;
            System.out.println("chunk[" + i + "] = " + chunks[i]);
        }

        RegionCoverage coverage = new RegionCoverage(region, windowSize, chunks);
        System.out.println(coverage.toJSON());
    }


    @Test
    public void coverage() throws Exception {
        LocalAlignmentDBAdaptor dbAdaptor = new LocalAlignmentDBAdaptor();

//        Path path = Paths.get("/home/jtarraga/data150/test/corpasome/dad.bam.bw");
//        Path path = Paths.get("/home/jtarraga/data150/test/corpasome/son.bam.bw");
        Path path = Paths.get("/home/jtarraga/data150/test/corpasome/daughter.bam.bs1.bw");
//        Path path = Paths.get("/home/jtarraga/data150/test/corpasome/mom.bam.bw");

        double[] means = new double[]{0, 0, 0};

        List<AbstractMap.SimpleEntry<String, Integer>> pairs = new ArrayList<>();
        pairs.add(new AbstractMap.SimpleEntry<>("1", 249250621));
//        pairs.add(new AbstractMap.SimpleEntry<>("10", 135534747));
//        pairs.add(new AbstractMap.SimpleEntry<>("11", 135006516));
//        pairs.add(new AbstractMap.SimpleEntry<>("12", 133851895));
//        pairs.add(new AbstractMap.SimpleEntry<>("13", 115169878));
//        pairs.add(new AbstractMap.SimpleEntry<>("14", 107349540));
//        pairs.add(new AbstractMap.SimpleEntry<>("15", 102531392));
//        pairs.add(new AbstractMap.SimpleEntry<>("16", 90354753));
//        pairs.add(new AbstractMap.SimpleEntry<>("17", 81195210));
//        pairs.add(new AbstractMap.SimpleEntry<>("18", 78077248));
//        pairs.add(new AbstractMap.SimpleEntry<>("19", 59128983));
//        pairs.add(new AbstractMap.SimpleEntry<>("2", 243199373));
//        pairs.add(new AbstractMap.SimpleEntry<>("20", 63025520));
//        pairs.add(new AbstractMap.SimpleEntry<>("21", 48129895));
//        pairs.add(new AbstractMap.SimpleEntry<>("22", 51304566));
//        pairs.add(new AbstractMap.SimpleEntry<>("3", 198022430));
//        pairs.add(new AbstractMap.SimpleEntry<>("4", 191154276));
//        pairs.add(new AbstractMap.SimpleEntry<>("5", 180915260));
//        pairs.add(new AbstractMap.SimpleEntry<>("6", 171115067));
//        pairs.add(new AbstractMap.SimpleEntry<>("7", 159138663));
//        pairs.add(new AbstractMap.SimpleEntry<>("8", 146364022));
//        pairs.add(new AbstractMap.SimpleEntry<>("9", 141213431));
//        pairs.add(new AbstractMap.SimpleEntry<>("MT", 16569));
        pairs.add(new AbstractMap.SimpleEntry<>("X", 155270560));
        pairs.add(new AbstractMap.SimpleEntry<>("Y", 59373566));

        for (AbstractMap.SimpleEntry<String, Integer> pair : pairs) {
            Chromosome chrom = new Chromosome();
            chrom.setName(pair.getKey());
            chrom.setStart(1);
            chrom.setEnd(pair.getValue());
            chrom.setSize(pair.getValue());

            int windowSize = 1; //50; //chrom.getSize() / 10000;
//            Region region = new Region(chrom.getName(), chrom.getStart(), chrom.getEnd());
            Region region = new Region(chrom.getName(), 4999730, 4999740);
//            region.setStart(400000);
//            region.setEnd(401000);
            List<RegionCoverage> regionCoverages = dbAdaptor.coverageQuery(path, region, 0, Integer.MAX_VALUE, windowSize).getResults();

            double meanCoverage = 0;
            for (RegionCoverage regionCoverage : regionCoverages) {
                meanCoverage += regionCoverage.meanCoverage();
            }
            meanCoverage /= regionCoverages.size();

            String name = chrom.getName().toLowerCase();
            switch (name) {
                case "mt": {
                    // Nothing to do
                    break;
                }
                case "y": {
                    means[2] = meanCoverage;
                    break;
                }
                case "x": {
                    means[1] = meanCoverage;
                    break;
                }
                default: {
                    means[0] += meanCoverage;
                    System.out.println("Chromosome " + pair.getKey() + ", mean coverage = " + meanCoverage + ", acc. = " + means[0]);
                    break;
                }
            }
        }
        means[0] /= 22;

        System.out.println("means(autosomal, X, Y) = (" + means[0] + ", " + means[1] + ", " + means[2] + ")");
    }

}