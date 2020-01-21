package org.opencb.opencga.analysis.variant.samples;

import org.junit.Assert;
import org.junit.Test;

public class VariantQueryOptimizerTest {

    @Test
    public void testAlreadyOptimized() {
        TreeQuery.Node node = new TreeQuery("biotype=protein_coding AND gene=BRCA2 AND ct=lof").getRoot();
        System.out.println("node = " + node);
        TreeQuery.Node optimize = VariantQueryOptimizer.optimize(node);
        System.out.println("optimize = " + optimize);
    }

    @Test
    public void testOptimize1() {
        TreeQuery.Node node = new TreeQuery("((biotype=protein_coding AND (gene=BRCA2)) AND (ct=lof AND (biotype=protein_coding AND stats > 30)))").getRoot();
        System.out.println("node = " + node);
        TreeQuery.Node optimize = VariantQueryOptimizer.optimize(node);
        System.out.println("optimize = " + optimize);
    }

    @Test
    public void testOptimize2() {
        TreeQuery.Node node = new TreeQuery("((biotype=protein_coding AND (gene=BRCA2)) AND (ct=lof AND (gene=BMPR2 AND biotype=protein_coding)))").getRoot();
        System.out.println("node = " + node);
        TreeQuery.Node optimize = VariantQueryOptimizer.optimize(node);
        System.out.println("optimize = " + optimize);
    }

    @Test
    public void testOptimize3() {
        TreeQuery.Node node = new TreeQuery("((biotype=protein_coding AND (gene=BRCA2)) AND (ct=lof AND (biotype=protein_coding)))").getRoot();
        System.out.println("node = " + node);
        TreeQuery.Node optimize = VariantQueryOptimizer.optimize(node);
        System.out.println("optimize = " + optimize);
    }

    @Test
    public void testOptimize4() {
        TreeQuery.Node node = new TreeQuery("((biotype=protein_coding AND (gene=BRCA2)) AND (ct=lof AND (region=chr22)))").getRoot();
        System.out.println("node = " + node);
        TreeQuery.Node optimize = VariantQueryOptimizer.optimize(node);
        System.out.println("optimize = " + optimize);
        Assert.assertEquals("(gene=BRCA2 AND biotype=protein_coding) AND (region=chr22 AND ct=lof)", optimize.toString());
    }
}