package org.opencb.opencga.storage.core.variant.query.projection;

import org.junit.Test;
import org.opencb.commons.datastore.core.Query;
import org.opencb.opencga.storage.core.variant.adaptors.VariantField;

import java.util.Arrays;
import java.util.Set;

import static org.junit.Assert.*;
import static org.opencb.opencga.core.api.ParamConstants.ALL;
import static org.opencb.opencga.core.api.ParamConstants.NONE;
import static org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam.*;
import static org.opencb.opencga.storage.core.variant.query.projection.VariantQueryProjectionParser.*;

public class VariantQueryProjectionParserTest {

    @Test
    public void queryBySampleGenotype() throws Exception {
        Query query = new Query(STUDY.key(), "s1").append(SAMPLE.key(), "sample1:0/1,0|1,1|0;sample2:0/1,0|1,1|0;sample3:1/1,1|1");
        assertEquals(Arrays.asList("sample1", "sample2", "sample3"), getIncludeSamplesList(query));
    }

    @Test
    public void getIncludeStatus() {
        checkIncludeStatus(new Query(), IncludeStatus.NONE);
        checkIncludeStatus(new Query(INCLUDE_FILE.key(), NONE), IncludeStatus.NONE);
        checkIncludeStatus(new Query(INCLUDE_FILE.key(), ALL), IncludeStatus.ALL);
        checkIncludeStatus(new Query(INCLUDE_FILE.key(), "myFile.vcf"), IncludeStatus.SOME);

        checkIncludeStatus(new Query(FILE.key(), "myFile.vcf"), IncludeStatus.SOME);
        checkIncludeStatus(new Query(FILE_DATA.key(), "myFile.vcfL:FILTER=PASS"), IncludeStatus.SOME);
        checkIncludeStatus(new Query(FILE.key(), "myFile.vcf").append(INCLUDE_FILE.key(), ALL), IncludeStatus.ALL);
        checkIncludeStatus(new Query(FILE.key(), "myFile.vcf").append(INCLUDE_FILE.key(), NONE), IncludeStatus.NONE);

        checkIncludeStatus(new Query(INCLUDE_SAMPLE.key(), NONE), IncludeStatus.NONE);
        checkIncludeStatus(new Query(INCLUDE_SAMPLE.key(), ALL), IncludeStatus.ALL);
        checkIncludeStatus(new Query(INCLUDE_SAMPLE.key(), "mySample"), IncludeStatus.SOME);

        checkIncludeStatus(new Query(SAMPLE.key(), "mySample"), IncludeStatus.SOME);
        checkIncludeStatus(new Query(SAMPLE_DATA.key(), "mySample:DP>5"), IncludeStatus.SOME);

        checkIncludeStatus(new Query(SAMPLE.key(), "mySample").append(INCLUDE_FILE.key(), NONE), IncludeStatus.SOME, IncludeStatus.NONE);
        checkIncludeStatus(new Query(SAMPLE.key(), "mySample").append(INCLUDE_FILE.key(), NONE), IncludeStatus.SOME, IncludeStatus.NONE, VariantField.parseInclude("samples"));
        checkIncludeStatus(new Query(SAMPLE.key(), "mySample").append(INCLUDE_FILE.key(), NONE), IncludeStatus.NONE, IncludeStatus.NONE, VariantField.parseInclude("annotation"));
        checkIncludeStatus(new Query(SAMPLE.key(), "mySample"), IncludeStatus.NONE, IncludeStatus.NONE, VariantField.parseInclude("annotation"));
    }

    private void checkIncludeStatus(Query query, IncludeStatus includeStatus) {
        checkIncludeStatus(query, includeStatus, includeStatus);
    }

    private void checkIncludeStatus(Query query, IncludeStatus sampleStatus, IncludeStatus fileStatus) {
        checkIncludeStatus(query, sampleStatus, fileStatus, VariantField.all());
    }

    private void checkIncludeStatus(Query query, IncludeStatus sampleStatus, IncludeStatus fileStatus, Set<VariantField> all) {
        assertEquals(sampleStatus, getIncludeSampleStatus(query, all));
        assertEquals(fileStatus, getIncludeFileStatus(query, all));
    }


}