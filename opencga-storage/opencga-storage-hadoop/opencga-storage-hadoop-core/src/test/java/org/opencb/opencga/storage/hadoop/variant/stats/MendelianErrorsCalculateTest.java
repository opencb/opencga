package org.opencb.opencga.storage.hadoop.variant.stats;

import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExternalResource;
import org.opencb.biodata.models.feature.Genotype;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.tools.pedigree.MendelianError;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.core.results.VariantQueryResult;
import org.opencb.opencga.storage.core.variant.VariantStorageBaseTest;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryUtils;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageEngine;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageTest;
import org.opencb.opencga.storage.hadoop.variant.VariantHbaseTestUtils;

import java.net.URI;
import java.util.*;

import static org.hamcrest.CoreMatchers.hasItem;
import static org.junit.Assert.*;
import static org.opencb.opencga.storage.core.variant.adaptors.VariantMatchers.gt;

/**
 * Created on 12/03/19.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class MendelianErrorsCalculateTest extends VariantStorageBaseTest implements HadoopVariantStorageTest {

    @Rule
    public ExternalResource externalResource = new HadoopExternalResource();

    @After
    public void tearDown() throws Exception {
        VariantHbaseTestUtils.printVariants(getVariantStorageEngine().getDBAdaptor(), newOutputUri(getTestName().getMethodName()));
    }


    @Test
    public void testMendelianErrors() throws Exception {
        HadoopVariantStorageEngine variantStorageEngine = getVariantStorageEngine();
        URI outputUri = newOutputUri();

        String study = "study";
        ObjectMap params = new ObjectMap(VariantStorageEngine.Options.ANNOTATE.key(), false)
                .append(VariantStorageEngine.Options.CALCULATE_STATS.key(), false)
                .append(VariantStorageEngine.Options.STUDY.key(), study);
        runETL(variantStorageEngine, getPlatinumFile(12877), outputUri, params, true, true, true);
        runETL(variantStorageEngine, getPlatinumFile(12878), outputUri, params, true, true, true);
        runETL(variantStorageEngine, getPlatinumFile(12879), outputUri, params, true, true, true);

        String father = "NA12877";
        String mother = "NA12878";
        String child = "NA12879";  // Maybe this is not accurate, but works file for the example
        List<String> family = Arrays.asList(father, mother, child);

        variantStorageEngine.fillGaps(study, family, new ObjectMap());

        variantStorageEngine.calculateMendelianErrors(study, Collections.singletonList(family), new ObjectMap());

        Set<String> mendelianErrorVariants = new HashSet<>();
        for (Variant variant : variantStorageEngine) {
            Genotype fatherGenotype = new Genotype(variant.getStudies().get(0).getSampleData(father, "GT"));
            Genotype motherGenotype = new Genotype(variant.getStudies().get(0).getSampleData(mother, "GT"));
            Genotype childGenotype = new Genotype(variant.getStudies().get(0).getSampleData(child, "GT"));
            if (MendelianError.compute(fatherGenotype, motherGenotype, childGenotype, variant.getChromosome()) != 0) {
                mendelianErrorVariants.add(variant.toString());
            }
        }

        Query query = new Query()
                .append(VariantQueryUtils.SAMPLE_MENDELIAN_ERROR.key(), child)
                .append(VariantQueryParam.INCLUDE_GENOTYPE.key(), true)
                .append(VariantQueryParam.INCLUDE_SAMPLE.key(), VariantQueryUtils.ALL)
                .append(VariantQueryParam.INCLUDE_FILE.key(), VariantQueryUtils.NONE);
        VariantQueryResult<Variant> result = variantStorageEngine.get(query, new QueryOptions());
        for (Variant variant : result.getResult()) {
            System.out.println(variant.toString() + "\t" + variant.getStudies().get(0).getSamplesData());
            assertThat(mendelianErrorVariants, hasItem(variant.toString()));
        }
        assertEquals(mendelianErrorVariants.size(), result.getNumResults());


        query.append(VariantQueryParam.FILE.key(), "1K.end.platinum-genomes-vcf-" + child + "_S1.genome.vcf.gz");
        query.append(VariantQueryParam.QUAL.key(), ">30");
        query.remove(VariantQueryParam.INCLUDE_FILE.key());

        result = variantStorageEngine.get(query, new QueryOptions());
        for (Variant variant : result.getResult()) {
            System.out.println(variant.toString() + "\t" + variant.getStudies().get(0).getSamplesData());
            assertThat(mendelianErrorVariants, hasItem(variant.toString()));
            assertThat(Double.valueOf(variant.getStudies().get(0).getFiles().get(0).getAttributes().get(StudyEntry.QUAL)), gt(30));
        }
        assertNotEquals(0, result.getNumResults());
    }

}
