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

package org.opencb.opencga.analysis.variant.samples;

import org.junit.Assert;
import org.junit.Test;

public class VariantQueryOptimizerTest {

    @Test
    public void testAlreadyOptimized() {
        TreeQuery node = new TreeQuery("biotype=protein_coding AND gene=BRCA2 AND ct=lof");
        //System.out.println("node = " + node);
        TreeQuery optimize = VariantQueryOptimizer.optimize(node);
        //System.out.println("optimize = " + optimize);
        Assert.assertEquals("biotype=protein_coding AND gene=BRCA2 AND ct=lof", optimize.toString());
    }

    @Test
    public void testOptimize1() {
        TreeQuery node = new TreeQuery("((biotype=protein_coding AND (gene=BRCA2)) AND (ct=lof AND (biotype=protein_coding AND stats > 30)))");
        //System.out.println("node = " + node);
        TreeQuery optimize = VariantQueryOptimizer.optimize(node);
        //System.out.println("optimize = " + optimize);
        Assert.assertEquals("biotype=protein_coding AND gene=BRCA2 AND ct=lof AND stats>30", optimize.toString());
    }

    @Test
    public void testOptimize2() {
        TreeQuery node = new TreeQuery("((biotype=protein_coding AND (gene=BRCA2)) AND (ct=lof AND (gene=BMPR2 AND biotype=protein_coding)))");
        //System.out.println("node = " + node);
        TreeQuery optimize = VariantQueryOptimizer.optimize(node);
        //System.out.println("optimize = " + optimize);
        Assert.assertEquals("(biotype=protein_coding AND gene=BRCA2) AND (ct=lof AND gene=BMPR2 AND biotype=protein_coding)", optimize.toString());
    }

    @Test
    public void testOptimize3() {
        TreeQuery node = new TreeQuery("((biotype=protein_coding AND (gene=BRCA2)) AND (ct=lof AND (biotype=protein_coding)))");
        //System.out.println("node = " + node);
        TreeQuery optimize = VariantQueryOptimizer.optimize(node);
        //System.out.println("optimize = " + optimize);
        Assert.assertEquals("biotype=protein_coding AND gene=BRCA2 AND ct=lof", optimize.toString());
    }

    @Test
    public void testOptimize4() {
        TreeQuery node = new TreeQuery("((biotype=protein_coding AND (gene=BRCA2)) AND (ct=lof AND (region=chr22)))");
        //System.out.println("node = " + node);
        TreeQuery optimize = VariantQueryOptimizer.optimize(node);
        //System.out.println("optimize = " + optimize);
        Assert.assertEquals("(biotype=protein_coding AND gene=BRCA2) AND (ct=lof AND region=chr22)", optimize.toString());
    }

    @Test
    public void testOptimizeNegations() {
        TreeQuery node = new TreeQuery("NOT ( NOT ( key=3))");
        //System.out.println("node = " + node);
        TreeQuery optimize = VariantQueryOptimizer.optimize(node);
        //System.out.println("optimize = " + optimize);
        Assert.assertEquals("key=3", optimize.toString());
    }
    @Test
    public void testOptimizeNegations2() {
        TreeQuery node = new TreeQuery("NOT ((key=value) AND (key3>50)) OR NOT (key5>=2323)");
        //System.out.println("node = " + node);
        TreeQuery optimize = VariantQueryOptimizer.optimize(node);
        //System.out.println("optimize = " + optimize);
        Assert.assertEquals("(NOT (key=value AND key3>50)) OR (NOT (key5>=2323))", optimize.toString());
    }
}