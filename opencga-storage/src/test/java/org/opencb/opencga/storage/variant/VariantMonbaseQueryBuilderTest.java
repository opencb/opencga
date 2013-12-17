package org.opencb.opencga.storage.variant;

import org.junit.Test;
import org.opencb.commons.bioformats.feature.Genotype;
import org.opencb.commons.bioformats.variant.Variant;
import org.opencb.commons.bioformats.variant.utils.stats.VariantStats;
import org.opencb.opencga.storage.variant.VariantMonbaseQueryBuilder;

import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: jrodriguez
 * Date: 12/12/13
 * Time: 2:56 PM
 * To change this template use File | Settings | File Templates.
 */
public class VariantMonbaseQueryBuilderTest {

    @Test
    public void test1() {
        Long start, end;
        start = System.currentTimeMillis();
        VariantMonbaseQueryBuilder tal = new VariantMonbaseQueryBuilder("localhost", "pruebaVariant");
        List<Variant> cual = tal.getRegionMongo("1", "1", 32100000, 32180000, "miEstudio2");
        end = System.currentTimeMillis();
        System.out.println("Time: " + (end - start));
        for(Variant v: cual){
            System.out.println(v.getReference());
            System.out.println(v.getAlternate());
            System.out.println(v.getChromosome());
            System.out.println(v.getPosition());
            VariantStats st = v.getStats();
            List<Genotype> gn = st.getGenotypes();
            for(Genotype gnt : gn){
                System.out.println(gnt.getGenotype());
            }

        }

    }
}
