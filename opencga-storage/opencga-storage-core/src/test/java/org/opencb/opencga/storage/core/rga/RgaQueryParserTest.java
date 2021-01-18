package org.opencb.opencga.storage.core.rga;

import org.apache.solr.client.solrj.SolrQuery;
import org.junit.Before;
import org.junit.Test;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.core.models.analysis.knockout.KnockoutVariant;
import org.opencb.opencga.storage.core.exceptions.RgaException;

import static org.junit.Assert.*;

public class RgaQueryParserTest {

    private RgaQueryParser parser;

    @Before
    public void setUp() throws Exception {
        parser = new RgaQueryParser();
    }

    @Test
    public void parseComplexFilters() throws RgaException {
        Query query = new Query(RgaQueryParams.POPULATION_FREQUENCY.key(), RgaUtils.THOUSAND_GENOMES_STUDY + "<0.001");
        SolrQuery parse = parser.parseQuery(query);
        assertEquals(RgaDataModel.POPULATION_FREQUENCIES + ":( P1-1 || P1-2 || P1-3 )", parse.get("fq"));

        query = new Query(RgaQueryParams.POPULATION_FREQUENCY.key(), RgaUtils.THOUSAND_GENOMES_STUDY + "<0.001;"
                + RgaUtils.GNOMAD_GENOMES_STUDY + ">0.05");
        parse = parser.parseQuery(query);
        assertEquals(RgaDataModel.POPULATION_FREQUENCIES + ":( ( P1-1 || P1-2 || P1-3 ) && P2-8 )", parse.get("fq"));

        query = new Query(RgaQueryParams.POPULATION_FREQUENCY.key(), RgaUtils.THOUSAND_GENOMES_STUDY + "<0.001;"
                + RgaUtils.GNOMAD_GENOMES_STUDY + ">=0.05");
        parse = parser.parseQuery(query);
        assertEquals(RgaDataModel.POPULATION_FREQUENCIES + ":( ( P1-1 || P1-2 || P1-3 ) && ( P2-8 || P2-7 ) )", parse.get("fq"));

        query = new Query(RgaQueryParams.CONSEQUENCE_TYPE.key(), "SO:0001891,SO:0001822");
        parse = parser.parseQuery(query);
        assertEquals(RgaDataModel.CONSEQUENCE_TYPES + ":( SO\\:0001891 || SO\\:0001822 )", parse.get("fq"));

        query = new Query(RgaQueryParams.FILTER.key(), "PASS");
        parse = parser.parseQuery(query);
        assertEquals(RgaDataModel.FILTERS + ":PASS", parse.get("fq"));

        query = new Query(RgaQueryParams.KNOCKOUT.key(), KnockoutVariant.KnockoutType.HOM_ALT + "," + KnockoutVariant.KnockoutType.HET_ALT);
        parse = parser.parseQuery(query);
        assertEquals(RgaDataModel.KNOCKOUT_TYPES + ":( HOM_ALT || HET_ALT )", parse.get("fq"));

        query = new Query()
                .append(RgaQueryParams.POPULATION_FREQUENCY.key(), RgaUtils.THOUSAND_GENOMES_STUDY + "<0.001;" + RgaUtils.GNOMAD_GENOMES_STUDY + ">=0.05")
                .append(RgaQueryParams.CONSEQUENCE_TYPE.key(), "SO:0001891,SO:0001822");
        parse = parser.parseQuery(query);
        assertEquals(RgaDataModel.COMPOUND_FILTERS + ":( ( CH__P__1891__P1-1 || CH__P__1822__P1-1 || CH__NP__1891__P1-1 || CH__NP__1822__P1-1 || DO__P__1891__P1-1 || DO__P__1822__P1-1 || DO__NP__1891__P1-1 || DO__NP__1822__P1-1 || HEA__P__1891__P1-1 || HEA__P__1822__P1-1 || HEA__NP__1891__P1-1 || HEA__NP__1822__P1-1 || HOA__P__1891__P1-1 || HOA__P__1822__P1-1 || HOA__NP__1891__P1-1 || HOA__NP__1822__P1-1 || CH__P__1891__P1-2 || CH__P__1822__P1-2 || CH__NP__1891__P1-2 || CH__NP__1822__P1-2 || DO__P__1891__P1-2 || DO__P__1822__P1-2 || DO__NP__1891__P1-2 || DO__NP__1822__P1-2 || HEA__P__1891__P1-2 || HEA__P__1822__P1-2 || HEA__NP__1891__P1-2 || HEA__NP__1822__P1-2 || HOA__P__1891__P1-2 || HOA__P__1822__P1-2 || HOA__NP__1891__P1-2 || HOA__NP__1822__P1-2 || CH__P__1891__P1-3 || CH__P__1822__P1-3 || CH__NP__1891__P1-3 || CH__NP__1822__P1-3 || DO__P__1891__P1-3 || DO__P__1822__P1-3 || DO__NP__1891__P1-3 || DO__NP__1822__P1-3 || HEA__P__1891__P1-3 || HEA__P__1822__P1-3 || HEA__NP__1891__P1-3 || HEA__NP__1822__P1-3 || HOA__P__1891__P1-3 || HOA__P__1822__P1-3 || HOA__NP__1891__P1-3 || HOA__NP__1822__P1-3 ) && ( CH__P__1891__P2-8 || CH__P__1822__P2-8 || CH__NP__1891__P2-8 || CH__NP__1822__P2-8 || DO__P__1891__P2-8 || DO__P__1822__P2-8 || DO__NP__1891__P2-8 || DO__NP__1822__P2-8 || HEA__P__1891__P2-8 || HEA__P__1822__P2-8 || HEA__NP__1891__P2-8 || HEA__NP__1822__P2-8 || HOA__P__1891__P2-8 || HOA__P__1822__P2-8 || HOA__NP__1891__P2-8 || HOA__NP__1822__P2-8 || CH__P__1891__P2-7 || CH__P__1822__P2-7 || CH__NP__1891__P2-7 || CH__NP__1822__P2-7 || DO__P__1891__P2-7 || DO__P__1822__P2-7 || DO__NP__1891__P2-7 || DO__NP__1822__P2-7 || HEA__P__1891__P2-7 || HEA__P__1822__P2-7 || HEA__NP__1891__P2-7 || HEA__NP__1822__P2-7 || HOA__P__1891__P2-7 || HOA__P__1822__P2-7 || HOA__NP__1891__P2-7 || HOA__NP__1822__P2-7 ) )", parse.get("fq"));

        query = new Query()
                .append(RgaQueryParams.POPULATION_FREQUENCY.key(), RgaUtils.THOUSAND_GENOMES_STUDY + "<0.001;" + RgaUtils.GNOMAD_GENOMES_STUDY + ">=0.05")
                .append(RgaQueryParams.CONSEQUENCE_TYPE.key(), "SO:0001891,SO:0001822")
                .append(RgaQueryParams.KNOCKOUT.key(), KnockoutVariant.KnockoutType.HOM_ALT + "," + KnockoutVariant.KnockoutType.HET_ALT);
        parse = parser.parseQuery(query);
        assertEquals(RgaDataModel.COMPOUND_FILTERS + ":( ( HOA__P__1891__P1-1 || HOA__P__1822__P1-1 || HOA__NP__1891__P1-1 || HOA__NP__1822__P1-1 || HEA__P__1891__P1-1 || HEA__P__1822__P1-1 || HEA__NP__1891__P1-1 || HEA__NP__1822__P1-1 || HOA__P__1891__P1-2 || HOA__P__1822__P1-2 || HOA__NP__1891__P1-2 || HOA__NP__1822__P1-2 || HEA__P__1891__P1-2 || HEA__P__1822__P1-2 || HEA__NP__1891__P1-2 || HEA__NP__1822__P1-2 || HOA__P__1891__P1-3 || HOA__P__1822__P1-3 || HOA__NP__1891__P1-3 || HOA__NP__1822__P1-3 || HEA__P__1891__P1-3 || HEA__P__1822__P1-3 || HEA__NP__1891__P1-3 || HEA__NP__1822__P1-3 ) && ( HOA__P__1891__P2-8 || HOA__P__1822__P2-8 || HOA__NP__1891__P2-8 || HOA__NP__1822__P2-8 || HEA__P__1891__P2-8 || HEA__P__1822__P2-8 || HEA__NP__1891__P2-8 || HEA__NP__1822__P2-8 || HOA__P__1891__P2-7 || HOA__P__1822__P2-7 || HOA__NP__1891__P2-7 || HOA__NP__1822__P2-7 || HEA__P__1891__P2-7 || HEA__P__1822__P2-7 || HEA__NP__1891__P2-7 || HEA__NP__1822__P2-7 ) )", parse.get("fq"));

        query = new Query()
                .append(RgaQueryParams.POPULATION_FREQUENCY.key(), RgaUtils.THOUSAND_GENOMES_STUDY + "<0.001;" + RgaUtils.GNOMAD_GENOMES_STUDY + ">=0.05")
                .append(RgaQueryParams.CONSEQUENCE_TYPE.key(), "SO:0001891,SO:0001822")
                .append(RgaQueryParams.KNOCKOUT.key(), KnockoutVariant.KnockoutType.HOM_ALT + "," + KnockoutVariant.KnockoutType.HET_ALT)
                .append(RgaQueryParams.FILTER.key(), "PASS");
        parse = parser.parseQuery(query);
        assertEquals(RgaDataModel.COMPOUND_FILTERS + ":( ( HOA__P__1891__P1-1 || HOA__P__1822__P1-1 || HEA__P__1891__P1-1 || HEA__P__1822__P1-1 || HOA__P__1891__P1-2 || HOA__P__1822__P1-2 || HEA__P__1891__P1-2 || HEA__P__1822__P1-2 || HOA__P__1891__P1-3 || HOA__P__1822__P1-3 || HEA__P__1891__P1-3 || HEA__P__1822__P1-3 ) && ( HOA__P__1891__P2-8 || HOA__P__1822__P2-8 || HEA__P__1891__P2-8 || HEA__P__1822__P2-8 || HOA__P__1891__P2-7 || HOA__P__1822__P2-7 || HEA__P__1891__P2-7 || HEA__P__1822__P2-7 ) )", parse.get("fq"));

        query = new Query()
                .append(RgaQueryParams.CONSEQUENCE_TYPE.key(), "SO:0001891,SO:0001822")
                .append(RgaQueryParams.FILTER.key(), "PASS");
        parse = parser.parseQuery(query);
        assertEquals(RgaDataModel.COMPOUND_FILTERS + ":( CH__P__1891 || CH__P__1822 || DO__P__1891 || DO__P__1822 || HEA__P__1891 || HEA__P__1822 || HOA__P__1891 || HOA__P__1822 )", parse.get("fq"));

        query = new Query()
                .append(RgaQueryParams.CONSEQUENCE_TYPE.key(), "SO:0001891,SO:0001822")
                .append(RgaQueryParams.KNOCKOUT.key(), KnockoutVariant.KnockoutType.HOM_ALT + "," + KnockoutVariant.KnockoutType.HET_ALT)
                .append(RgaQueryParams.FILTER.key(), "PASS");
        parse = parser.parseQuery(query);
        assertEquals(RgaDataModel.COMPOUND_FILTERS + ":( HOA__P__1891 || HOA__P__1822 || HEA__P__1891 || HEA__P__1822 )", parse.get("fq"));

        query = new Query()
                .append(RgaQueryParams.POPULATION_FREQUENCY.key(), RgaUtils.THOUSAND_GENOMES_STUDY + "<0.001;" + RgaUtils.GNOMAD_GENOMES_STUDY + ">=0.5")
                .append(RgaQueryParams.FILTER.key(), "PASS");
        parse = parser.parseQuery(query);
        assertEquals(RgaDataModel.COMPOUND_FILTERS + ":( ( CH__P__P1-1 || DO__P__P1-1 || HEA__P__P1-1 || HOA__P__P1-1 || CH__P__P1-2 || DO__P__P1-2 || HEA__P__P1-2 || HOA__P__P1-2 || CH__P__P1-3 || DO__P__P1-3 || HEA__P__P1-3 || HOA__P__P1-3 ) && ( CH__P__P2-8 || DO__P__P2-8 || HEA__P__P2-8 || HOA__P__P2-8 ) )", parse.get("fq"));

        query = new Query()
                .append(RgaQueryParams.POPULATION_FREQUENCY.key(), RgaUtils.THOUSAND_GENOMES_STUDY + "<0.001;" + RgaUtils.GNOMAD_GENOMES_STUDY + ">=0.5")
                .append(RgaQueryParams.KNOCKOUT.key(), KnockoutVariant.KnockoutType.HOM_ALT + "," + KnockoutVariant.KnockoutType.HET_ALT)
                .append(RgaQueryParams.FILTER.key(), "PASS");
        parse = parser.parseQuery(query);
        assertEquals(RgaDataModel.COMPOUND_FILTERS + ":( ( HOA__P__P1-1 || HEA__P__P1-1 || HOA__P__P1-2 || HEA__P__P1-2 || HOA__P__P1-3 || HEA__P__P1-3 ) && ( HOA__P__P2-8 || HEA__P__P2-8 ) )", parse.get("fq"));

        query = new Query()
                .append(RgaQueryParams.POPULATION_FREQUENCY.key(), RgaUtils.THOUSAND_GENOMES_STUDY + "<0.001")
                .append(RgaQueryParams.KNOCKOUT.key(), KnockoutVariant.KnockoutType.COMP_HET);
        parse = parser.parseQuery(query);
        assertEquals(RgaDataModel.COMPOUND_FILTERS + ":( CH__P__P__P1-1 || CH__NP__NP__P1-1 || CH__NP__P__P1-1 || CH__P__P__P1-2 || CH__NP__NP__P1-2 || CH__NP__P__P1-2 || CH__P__P__P1-3 || CH__NP__NP__P1-3 || CH__NP__P__P1-3 )", parse.get("fq"));

        query = new Query()
                .append(RgaQueryParams.POPULATION_FREQUENCY.key(), RgaUtils.THOUSAND_GENOMES_STUDY + "<0.001")
                .append(RgaQueryParams.FILTER.key(), "PASS")
                .append(RgaQueryParams.KNOCKOUT.key(), KnockoutVariant.KnockoutType.COMP_HET);
        parse = parser.parseQuery(query);
        assertEquals(RgaDataModel.COMPOUND_FILTERS + ":( CH__P__P__P1-1 || CH__P__P__P1-2 || CH__P__P__P1-3 )", parse.get("fq"));

        query = new Query()
                .append(RgaQueryParams.POPULATION_FREQUENCY.key(), RgaUtils.THOUSAND_GENOMES_STUDY + "<=0.001")
                .append(RgaQueryParams.FILTER.key(), "PASS")
                .append(RgaQueryParams.KNOCKOUT.key(), KnockoutVariant.KnockoutType.COMP_HET);
        parse = parser.parseQuery(query);
        assertEquals(RgaDataModel.COMPOUND_FILTERS + ":( CH__P__P__P1-1 || CH__P__P__P1-2 || CH__P__P__P1-3 || CH__P__P__P1-4 )", parse.get("fq"));
    }

    @Test
    public void consequenceTypeParserTest() throws RgaException {
        Query query = new Query(RgaQueryParams.CONSEQUENCE_TYPE.key(), "SO:0001891,SO:0001822");
        SolrQuery parse = parser.parseQuery(query);
        assertEquals(RgaDataModel.CONSEQUENCE_TYPES + ":( SO\\:0001891 || SO\\:0001822 )", parse.get("fq"));

        query = new Query(RgaQueryParams.CONSEQUENCE_TYPE.key(), "regulatory_region_amplification,inframe_deletion");
        parse = parser.parseQuery(query);
        assertEquals(RgaDataModel.CONSEQUENCE_TYPES + ":( SO\\:0001891 || SO\\:0001822 )", parse.get("fq"));
    }
}