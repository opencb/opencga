package org.opencb.opencga.storage.core.utils;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.opencb.biodata.models.core.Region;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.ConsequenceType;
import org.opencb.biodata.models.variant.avro.VariantAnnotation;
import org.opencb.cellbase.client.config.ClientConfiguration;
import org.opencb.cellbase.client.config.RestConfig;
import org.opencb.cellbase.client.rest.CellBaseClient;
import org.opencb.cellbase.core.result.CellBaseDataResponse;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryException;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.core.variant.query.VariantQueryUtils;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.*;

/**
 * Created on 15/03/19.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
@RunWith(Parameterized.class)
public class CellBaseUtilsTest {

    public static final String UNKNOWN_GENE = "UNKNOWN_GENE";

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    private CellBaseUtils cellBaseUtils;
    private CellBaseClient cellBaseClient;

    @Parameters(name = "{0}")
    public static List<Object[]> data() {
        return Arrays.asList(
                new Object[]{"http://ws.opencb.org/cellbase-4.7.3/", "v4", "grch37"},
                new Object[]{"http://ws.opencb.org/cellbase-4.8.2/", "v4", "grch37"},
//                new Object[]{"http://ws.opencb.org/cellbase-4.8.3/", "v4", "grch37"},
//                new Object[]{"http://ws.opencb.org/cellbase-4.9.0/", "v4", "grch37"},
//                new Object[]{"http://ws.opencb.org/cellbase/", "v4", "grch37"},
                new Object[]{"https://ws.zettagenomics.com/cellbase/", "v5", "grch38"});
    }

    @Parameter(0)
    public String url;

    @Parameter(1)
    public String version;

    @Parameter(2)
    public String assembly;

    @Before
    public void setUp() throws Exception {
        cellBaseClient = new CellBaseClient("hsapiens", assembly,
                new ClientConfiguration().setVersion(version)
                        .setRest(new RestConfig(Collections.singletonList(url), 10000)));
        cellBaseUtils = new CellBaseUtils(cellBaseClient, assembly);
    }

    @Test
    public void testGetGene() {
        assertNotNull(cellBaseUtils.getGeneRegion(Arrays.asList("BRCA2"), false).get(0));
        Region region = cellBaseUtils.getGeneRegion(Arrays.asList("MT-TQ"), false).get(0);
        assertNotNull(region);
        assertEquals(1, region.getStart());
        assertEquals(region, new Region(region.toString()));
    }

    @Test
    public void testGetDuplicatedGene() {
        assertNotNull(cellBaseUtils.getGeneRegion(Arrays.asList("MFRP"), false).get(0));
    }

    @Test
    public void testGetMissing() {
        List<Region> list = cellBaseUtils.getGeneRegion(Arrays.asList(UNKNOWN_GENE), true);
        assertEquals(0, list.size());


        VariantQueryException e = VariantQueryException.geneNotFound(UNKNOWN_GENE);
        thrown.expectMessage(e.getMessage());
        thrown.expect(e.getClass());
        cellBaseUtils.getGeneRegion(Arrays.asList(UNKNOWN_GENE), false);
    }

    @Test
    public void convertGeneToRegion() {
        Query query = new Query(VariantQueryParam.GENE.key(), "BRCA2," + UNKNOWN_GENE + ",MFRP").append(VariantQueryUtils.SKIP_MISSING_GENES, true);
        VariantQueryUtils.convertGenesToRegionsQuery(query, cellBaseUtils);

        assertEquals(2, query.getAsStringList(VariantQueryUtils.ANNOT_GENE_REGIONS.key()).size());
    }

    @Test
    public void convertGeneToRegionFail() {
        Query query = new Query(VariantQueryParam.GENE.key(), "BRCA2," + UNKNOWN_GENE + ",MFRP");

        VariantQueryException e = VariantQueryException.geneNotFound(UNKNOWN_GENE);
        thrown.expectMessage(e.getMessage());
        thrown.expect(e.getClass());
        VariantQueryUtils.convertGenesToRegionsQuery(query, cellBaseUtils);
    }

    @Test
    public void testGetVariant() throws Exception {
        assertEquals(new Variant("19:44934489:G:A"), cellBaseUtils.getVariant("rs2571174"));
        assertEquals(Arrays.asList(new Variant("19:44934489:G:A"), new Variant("1:7797503:C:G")),
                cellBaseUtils.getVariants(Arrays.asList("rs2571174", "rs41278952")));
        assertEquals(Arrays.asList(new Variant("1:7797503:C:G"), new Variant("19:44934489:G:A")),
                cellBaseUtils.getVariants(Arrays.asList("rs41278952", "rs2571174")));

//        assertEquals(Arrays.asList(new Variant("1:7797503:C:G"), new Variant("19:44934489:G:A")),
//                cellBaseUtils.getVariants(Arrays.asList("COSM5828004", "rs2571174")));

        thrown.expectMessage("Unknown variant 'rs_NON_EX'");
        // Test some missing variant
        assertEquals(Arrays.asList(new Variant("1:7797503:C:G"), new Variant("19:44934489:G:A")),
                cellBaseUtils.getVariants(Arrays.asList("rs41278952", "rs_NON_EX", "rs2571174")));
    }

    @Test(timeout = 60000)
    public void testGetMeta() throws IOException {
        CellBaseDataResponse<ObjectMap> versions = cellBaseClient.getMetaClient().versions();
        assertNotNull(versions);
        assertNotNull(versions.allResults());
        assertNotEquals(0, versions.allResults().size());
    }

    @Test
    public void testGetTranscriptFlags() throws IOException {
        CellBaseDataResponse<VariantAnnotation> v = cellBaseClient.getVariantClient()
                .getAnnotationByVariantIds(Collections.singletonList("1:26644214:T:C"), new QueryOptions());
        VariantAnnotation variantAnnotation = v.firstResult();
        System.out.println("variantAnnotation = " + variantAnnotation);
        boolean withTranscriptFlags = false;
        for (ConsequenceType consequenceType : variantAnnotation.getConsequenceTypes()) {
            if (consequenceType.getTranscriptFlags() != null) {
                withTranscriptFlags = true;
            }
        }
        assertTrue(withTranscriptFlags);
    }

}