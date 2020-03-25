package org.opencb.opencga.storage.core.utils;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.opencb.biodata.models.core.Region;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.cellbase.client.config.ClientConfiguration;
import org.opencb.cellbase.client.config.RestConfig;
import org.opencb.cellbase.client.rest.CellBaseClient;
import org.opencb.commons.datastore.core.Query;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryException;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.core.variant.query.VariantQueryUtils;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Created on 15/03/19.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class CellBaseUtilsTest {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    private CellBaseUtils cellBaseUtils;
    private CellBaseClient cellBaseClient;

    @Before
    public void setUp() throws Exception {
        String assembly = "grch37";
        cellBaseClient = new CellBaseClient("hsapiens", assembly, new ClientConfiguration().setVersion("v4").setRest(
                new RestConfig(Collections.singletonList("http://bioinfo.hpc.cam.ac.uk/cellbase"), 10)));
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
        List<Region> list = cellBaseUtils.getGeneRegion(Arrays.asList("B3GLCT"), true);
        assertEquals(0, list.size());


        VariantQueryException e = VariantQueryException.geneNotFound("B3GLCT");
        thrown.expectMessage(e.getMessage());
        thrown.expect(e.getClass());
        cellBaseUtils.getGeneRegion(Arrays.asList("B3GLCT"), false);
    }

    @Test
    public void convertGeneToRegion() {
        Query query = new Query(VariantQueryParam.GENE.key(), "BRCA2,B3GLCT,MFRP").append(VariantQueryUtils.SKIP_MISSING_GENES, true);
        VariantQueryUtils.convertGenesToRegionsQuery(query, cellBaseUtils);

        assertEquals(2, query.getAsStringList(VariantQueryUtils.ANNOT_GENE_REGIONS.key()).size());
    }

    @Test
    public void convertGeneToRegionFail() {
        Query query = new Query(VariantQueryParam.GENE.key(), "BRCA2,B3GLCT,MFRP");

        VariantQueryException e = VariantQueryException.geneNotFound("B3GLCT");
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

}