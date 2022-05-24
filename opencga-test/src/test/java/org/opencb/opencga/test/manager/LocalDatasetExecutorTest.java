package org.opencb.opencga.test.manager;

import org.junit.Test;
import org.opencb.opencga.test.config.Variant;
import org.opencb.opencga.test.execution.LocalDatasetExecutor;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class LocalDatasetExecutorTest {


    @Test
    public void setVariantsInFile() throws Exception {
        Variant v1 = new Variant().setId(".").setAlternate("T").setChromosome("5")
                .setFilter(".").setFormat("GT:AD:DP:GQ:PL")
                .setInfo("AC=2;AF=1.00;AN=2;DP=2;ExcessHet=0.0000;FS=0.000;MLEAC=1;MLEAF=0.500;MQ=60.00;QD=18.66;SOR=0.693")
                .setPosition("548745").setReference("A").setQuality("37.32");
        List<String> samples = new ArrayList<>();
        samples.add("1/1:0,2:2:6:90,6,0");
        samples.add("1/1:0,2:2:6:90,6,0");
        samples.add("1/1:0,2:2:6:49,6,0");
        v1.setSamples(samples);


        Variant v2 = new Variant().setId(".").setAlternate("T").setChromosome("5")
                .setFilter("PASS").setFormat("GT:DP")
                .setInfo("RSPOS=564477;AF=0.009;NS=111").setPosition("519387").setReference("A").setQuality("37.32");
        List<String> samples2 = new ArrayList<>();
        samples2.add("1/1:21");
        samples2.add("0/1:32");
        samples2.add("0/0:11");
        v1.setSamples(samples);
        v2.setSamples(samples);
        List<Variant> variants = new ArrayList<>();
        variants.add(v1);
        variants.add(v2);
        File file = new File("/home/juanfe/trainning/corpasome-grch37/output/vcf/GATK_v4.2.15_Falb_COL1.vcf");
        LocalDatasetExecutor executor = new LocalDatasetExecutor(null);
        executor.setVariantsInFile(variants, file);
    }
}
