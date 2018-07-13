package org.opencb.opencga.storage.core.variant.analysis;

import com.google.common.collect.Iterators;
import org.junit.Before;
import org.junit.Test;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.commons.datastore.core.Query;
import org.opencb.opencga.storage.core.variant.adaptors.GenotypeClass;
import org.opencb.opencga.storage.core.variant.adaptors.iterators.VariantDBIterator;
import org.opencb.opencga.storage.core.variant.adaptors.VariantIterable;

import java.util.*;

import static org.junit.Assert.assertEquals;

/**
 * Created on 26/06/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class VariantSampleFilterTest {

    public static final String S1 = "S1";
    public static final String S2 = "S2";
    public static final String S3 = "S3";
    public static final List<String> SAMPLES = Arrays.asList(S1, S2, S3);
    public static final String STUDY = "1";
    private VariantSampleFilter filter;

    @Before
    public void setUp() throws Exception {
        List<Variant> variants = Arrays.asList(
                Variant.newBuilder("1:100:A:C")
                        .setStudyId(STUDY).setFormat("GT").addSample(S1, "0/0").addSample(S2, "0/1").addSample(S3, "1/1").build(),
                Variant.newBuilder("1:101:A:C")
                        .setStudyId(STUDY).setFormat("GT").addSample(S1, "0/1").addSample(S2, "0/1").addSample(S3, "1/1").build(),
                Variant.newBuilder("1:102:A:C")
                        .setStudyId(STUDY).setFormat("GT").addSample(S1, "0/1").addSample(S2, "0/0").addSample(S3, "0/1").build(),
                Variant.newBuilder("1:103:A:C")
                        .setStudyId(STUDY).setFormat("GT").addSample(S1, "0/0").addSample(S2, "0/0").addSample(S3, "0/0").build(),
                Variant.newBuilder("1:104:A:C")
                        .setStudyId(STUDY).setFormat("GT").addSample(S1, "1/1").addSample(S2, "0/1").addSample(S3, "1/1").build()
        );

        VariantIterable iterable = (query, options) -> {
            List<String> queryVariants = query.getAsStringList("id");
            Iterator<Variant> iterator = Iterators.filter(variants.iterator(),
                    variant -> queryVariants.isEmpty() || queryVariants.contains(variant.toString()));
            return VariantDBIterator.wrapper(iterator);
        };
        filter = new VariantSampleFilter(iterable);
    }

    @Test
    public void testGetSamplesInAllVariants() throws Exception {
        Collection<String> samplesInAllVariants = filter.getSamplesInAllVariants(Arrays.asList("1:100:A:C", "1:102:A:C"), STUDY, SAMPLES,
                Arrays.asList("0/1", "1/1"));
        assertEquals(Arrays.asList(S3), new ArrayList<>(samplesInAllVariants));
    }

    @Test
    public void testGetSamplesInAllVariantsGenotypeClass() throws Exception {
        Collection<String> samplesInAllVariants = filter.getSamplesInAllVariants(Arrays.asList("1:100:A:C", "1:102:A:C"), STUDY, SAMPLES,
                Arrays.asList(GenotypeClass.HET.toString(), GenotypeClass.HOM_ALT.toString()));
        assertEquals(Arrays.asList(S3), new ArrayList<>(samplesInAllVariants));
    }

    @Test
    public void testGetSamplesInAllVariantsGenotypeClassMix() throws Exception {
        Collection<String> samplesInAllVariants = filter.getSamplesInAllVariants(Arrays.asList("1:100:A:C", "1:102:A:C"), STUDY, SAMPLES,
                Arrays.asList(GenotypeClass.HET.toString(), "1/1"));
        assertEquals(Arrays.asList(S3), new ArrayList<>(samplesInAllVariants));
    }

    @Test
    public void testGetSamplesInAnyVariants() throws Exception {
        Map<String, Set<Variant>> samples = filter.getSamplesInAnyVariants(new Query(), Arrays.asList("1/1"));
        System.out.println("samplesInAllVariants = " + samples);

        assertEquals(new HashSet<>(Arrays.asList(S1, S3)), samples.keySet());
    }

    @Test
    public void testGetSamplesInAnyVariantsGenotypeClass() throws Exception {
        Map<String, Set<Variant>> samples = filter.getSamplesInAnyVariants(new Query(), Arrays.asList(GenotypeClass.HOM_ALT.toString()));
        System.out.println("samplesInAllVariants = " + samples);

        assertEquals(new HashSet<>(Arrays.asList(S1, S3)), samples.keySet());
    }
}