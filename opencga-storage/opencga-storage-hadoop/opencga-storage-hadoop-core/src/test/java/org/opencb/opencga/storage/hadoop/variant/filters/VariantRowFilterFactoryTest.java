package org.opencb.opencga.storage.hadoop.variant.filters;

import org.apache.phoenix.schema.types.PhoenixArray;
import org.junit.Before;
import org.junit.Test;
import org.opencb.opencga.storage.hadoop.variant.converters.VariantRow;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.opencb.opencga.storage.hadoop.variant.converters.study.HBaseToStudyEntryConverter.*;

public class VariantRowFilterFactoryTest {

    private VariantRowFilterFactory factory;
    private List<VariantRow.FileColumn> fileColumns;
    private List<VariantRow.SampleColumn> sampleColumns;

    @Before
    public void setUp() throws Exception {
        factory = new VariantRowFilterFactory(Arrays.asList("DP", "F", "T"), Arrays.asList("GT", "DP", "F", "T"));
        fileColumns = Arrays.asList(
                fileColumn(0, "PASS",        "15546",  "5",    "12",  "A"),
                fileColumn(1, "PASS",        ".",      "10",   "15",  "B"),
                fileColumn(2, "LowGQ;LowDP", "14.5",   "15",   "22",  "C"),
                fileColumn(3, "LowDP",       "89",     "20",   "24",  "D")
        );
        sampleColumns = Arrays.asList(
                sampleColumn(0, "0|0",  "5",    "12",  "A"),
                sampleColumn(1, "0|1",  "10",   "15",  "B"),
                sampleColumn(2, "1|1",  "15",   "22",  "C"),
                sampleColumn(3, "1|2",  "20",   "24",  "D")
        );
    }

    @Test
    public void testFileDataFilter() {
        assertEquals(Arrays.asList(0), filterFiles(factory.buildFileDataFilter("DP<10;F>10")));
        assertEquals(Arrays.asList(0, 1), filterFiles(factory.buildFileDataFilter("DP<=10;F>10")));
        assertEquals(Arrays.asList(0), filterFiles(factory.buildFileDataFilter("DP<=10;F<13")));
        assertEquals(Arrays.asList(2), filterFiles(factory.buildFileDataFilter("FILTER=LowGQ")));
        assertEquals(Arrays.asList(2, 3), filterFiles(factory.buildFileDataFilter("FILTER=LowDP")));
        assertEquals(Arrays.asList(0, 2), filterFiles(factory.buildFileDataFilter("FILTER=LowGQ,QUAL>1000")));
        assertEquals(Arrays.asList(0, 1), filterFiles(factory.buildFileDataFilter("FILTER=PASS")));
        assertEquals(Arrays.asList(2, 3), filterFiles(factory.buildFileDataFilter("FILTER!=PASS")));
        assertEquals(Arrays.asList(1, 2, 3), filterFiles(factory.buildFileDataFilter("T>=B")));
        assertEquals(Arrays.asList(0, 1), filterFiles(factory.buildFileDataFilter("T<C")));
    }

    @Test
    public void testSampleDataFilter() {
        assertEquals(Arrays.asList(1, 2), filterSamples(factory.buildSampleDataFilter("GT=0/1,1/1")));
        assertEquals(Arrays.asList(2), filterSamples(factory.buildSampleDataFilter("GT=0/1,1/1;DP>10")));
        assertEquals(Arrays.asList(1, 2, 3), filterSamples(factory.buildSampleDataFilter("GT=0/1,1/1,DP>10")));
        assertEquals(Arrays.asList(0, 1), filterSamples(factory.buildSampleDataFilter("GT!=1/1;1/2")));
        assertEquals(Arrays.asList(0, 1, 3), filterSamples(factory.buildSampleDataFilter("T=A,B,D")));
        assertEquals(Arrays.asList(0, 2), filterSamples(factory.buildSampleDataFilter("T!=B;D")));
    }


    private List<Integer> filterFiles(Predicate<VariantRow.FileColumn> predicate) {
        return fileColumns.stream().filter(predicate).map(VariantRow.FileColumn::getFileId).collect(Collectors.toList());
    }

    private List<Integer> filterSamples(Predicate<VariantRow.SampleColumn> predicate) {
        return sampleColumns.stream().filter(predicate).map(VariantRow.SampleColumn::getSampleId).collect(Collectors.toList());
    }

    private VariantRow.SampleColumn sampleColumn(int sampleId, String gt, String... sampleData) {
        List<String> strings = new ArrayList<>(sampleData.length + 1);
        strings.add(gt);
        strings.addAll(Arrays.asList(sampleData));

        return new VariantRow.SampleColumn() {
            @Override
            public int getStudyId() {
                return 0;
            }

            @Override
            public int getSampleId() {
                return sampleId;
            }

            @Override
            public Integer getFileId() {
                return null;
            }

            @Override
            public List<String> getSampleData() {
                return strings;
            }

            @Override
            public List<String> getMutableSampleData() {
                return strings;
            }

            @Override
            public String getGT() {
                return strings.get(0);
            }

            @Override
            public String getSampleData(int idx) {
                return strings.get(idx);
            }
        };
    }

    private VariantRow.FileColumn fileColumn(final int fileId, String filter, String qual, String... info) {
        List<String> strings = Arrays.asList(new String[FILE_INFO_START_IDX + info.length]);

        strings.set(FILE_QUAL_IDX, qual);
        strings.set(FILE_FILTER_IDX, filter);
        for (int i = 0; i < info.length; i++) {
            strings.set(FILE_INFO_START_IDX + i, info[i]);
        }
        return new VariantRow.FileColumn() {
            @Override
            public int getStudyId() {
                return 0;
            }

            @Override
            public int getFileId() {
                return fileId;
            }

            @Override
            public PhoenixArray raw() {
                return null;
            }

            @Override
            public String getString(int idx) {
                return strings.get(idx);
            }
        };
    }


}