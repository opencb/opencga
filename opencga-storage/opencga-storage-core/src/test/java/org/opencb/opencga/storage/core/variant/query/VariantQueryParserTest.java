package org.opencb.opencga.storage.core.variant.query;

import org.junit.Before;
import org.junit.Test;
import org.opencb.cellbase.client.config.ClientConfiguration;
import org.opencb.cellbase.client.rest.CellBaseClient;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.metadata.models.StudyMetadata;
import org.opencb.opencga.storage.core.utils.CellBaseUtils;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.core.variant.VariantStorageOptions;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.core.variant.dummy.DummyVariantStorageMetadataDBAdaptorFactory;
import org.opencb.opencga.storage.core.variant.dummy.DummyVariantStorageTest;

import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.opencb.opencga.storage.core.variant.query.VariantQueryUtils.parseGenotypeFilter;

public class VariantQueryParserTest implements DummyVariantStorageTest {

    private VariantStorageMetadataManager metadataManager;
    private VariantQueryParser variantQueryParser;
    private StudyMetadata study1;
    private final List<String> loadedGenotypes = Arrays.asList(
            "0/1", "1/1",
            "1", "2",
            "0|1", "1|0", "1|1",
            "0/2", "0|2", "2|0",
            "1/2", "1|2", "2|1",
            "0/3", "0|3", "3|0",
            "1/3", "1|3", "3|1",
            "2/3", "2|3", "3|2",
            "0/4", "0/5", "0/6", "0/16",
            "1/4", "1/5", "1/6", "1/16"
    );

    @Before
    public void setUp() throws Exception {
        metadataManager = new VariantStorageMetadataManager(new DummyVariantStorageMetadataDBAdaptorFactory(true));
        ClientConfiguration clientConfiguration = getVariantStorageEngine().getConfiguration().getCellbase().toClientConfiguration();
        CellBaseUtils cellBaseUtils = new CellBaseUtils(new CellBaseClient("hsapiens", "grch38", clientConfiguration), "grch38");
        variantQueryParser = new VariantQueryParser(cellBaseUtils, metadataManager);


        study1 = metadataManager.createStudy("study1");
        metadataManager.updateStudyMetadata(study1.getId(), sm -> {
            sm.getAttributes().put(VariantStorageOptions.LOADED_GENOTYPES.key(), loadedGenotypes);
            return sm;
        });
        int file1 = metadataManager.registerFile(study1.getId(), "file1.vcf", Arrays.asList("sample1", "sample2", "sample3"));
        metadataManager.addIndexedFiles(study1.getId(), Collections.singletonList(file1));
        int file2 = metadataManager.registerFile(study1.getId(), "file2.vcf", Arrays.asList("sample1"));
        metadataManager.updateSampleMetadata(study1.getId(), metadataManager.getSampleId(study1.getId(), "sample1"), sm -> {
            sm.setSplitData(VariantStorageEngine.SplitData.MULTI);
            return sm;
        });
        metadataManager.addIndexedFiles(study1.getId(), Collections.singletonList(file2));
    }

    @Test
    public void testParseQuery() {
        ParsedVariantQuery parsedVariantQuery;

        parsedVariantQuery = variantQueryParser.parseQuery(new Query(VariantQueryParam.SAMPLE.key(), "sample1"), new QueryOptions());
        System.out.println("parsedVariantQuery = " + parsedVariantQuery);
        System.out.println("parsedVariantQuery.getInputQuery() = " + parsedVariantQuery.getInputQuery().toJson());
        System.out.println("parsedVariantQuery.getQuery() = " + parsedVariantQuery.getQuery().toJson());
        System.out.println("parsedVariantQuery.getStudyQuery().getGenotypes().toString() = " + parsedVariantQuery.getStudyQuery().getGenotypes().toString());
        System.out.println("parsedVariantQuery.getStudyQuery().getGenotypes().toQuery() = " + parsedVariantQuery.getStudyQuery().getGenotypes().toQuery());
        System.out.println("parsedVariantQuery.getStudyQuery().getGenotypes().describe() = " + parsedVariantQuery.getStudyQuery().getGenotypes().describe());
    }

    @Test
    public void preProcessGenotypesFilter() {
        assertEquals("S1:1/1,1|1", preProcessGenotypesFilter("S1:1/1", loadedGenotypes));
        assertEquals("S1:1/1,1|1,1/2,1|2,2|1,1/3,1|3,3|1,1/4,1/5,1/6,1/16", preProcessGenotypesFilter("S1:1/1,1/2", loadedGenotypes));
        assertEquals("S1:1/3,1|3,3|1,1/2,1|2,2|1,1/4,1/5,1/6,1/16", preProcessGenotypesFilter("S1:1/3", loadedGenotypes));
        assertEquals("S1:0/2,0|2,2|0,0/3,0|3,3|0,0/4,0/5,0/6,0/16", preProcessGenotypesFilter("S1:0/2", loadedGenotypes));
        assertEquals("S1:0/3,0|3,3|0,0/2,0|2,2|0,0/4,0/5,0/6,0/16", preProcessGenotypesFilter("S1:0/3", loadedGenotypes));
        assertEquals("S1:0|3,0|2", preProcessGenotypesFilter("S1:0|3", loadedGenotypes));
        assertEquals("S1:./2", preProcessGenotypesFilter("S1:./2", loadedGenotypes)); // check scape '.' in regex
    }

    protected String preProcessGenotypesFilter(String genotypeFilter, List<String> loadedGenotypes) {
        Map<Object, List<String>> map = new LinkedHashMap<>();
        VariantQueryUtils.QueryOperation op = parseGenotypeFilter(genotypeFilter, map);
        return VariantQueryParser.preProcessGenotypesFilter(map, op, loadedGenotypes);
    }
}