/*
 * Copyright 2015 OpenCB
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

package org.opencb.opencga.storage.mongodb.variant.converters;

import org.bson.Document;
import org.junit.BeforeClass;
import org.junit.Test;
import org.opencb.biodata.formats.variant.vcf4.VcfFormatHeader;
import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.biodata.models.variant.stats.VariantGlobalStats;

import java.util.Calendar;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 * @author Cristina Yenyxe Gonzalez Garcia <cyenyxe@ebi.ac.uk>
 */
public class DocumentToVariantSourceConverterTest {

    private static Document mongoSource;

    private static VariantSource source;


    @BeforeClass
    public static void setUpClass() {
        source = new VariantSource("file.vcf", "f", "s1", "study1");
        Map<String, Integer> samplesPosition = new LinkedHashMap<>();
        samplesPosition.put("NA000.A", 0);
        samplesPosition.put("NA001.B", 1);
        samplesPosition.put("NA002", 2);
        samplesPosition.put("NA003", 3);
        source.setSamplesPosition(samplesPosition);
        source.addMetadata("header", "##fileformat=v4.1");
        source.addMetadata("FORMAT.A", new VcfFormatHeader("id", "1", "Integer", "description"));
        VariantGlobalStats global = new VariantGlobalStats(10, 4, 7, 3, 0, 9, 4, 4, -1, 20.5f, null);
        source.setStats(global);
//        source.setType(VariantStudy.StudyType.CASE_CONTROL);

        mongoSource = new Document(DocumentToVariantSourceConverter.FILENAME_FIELD, source.getFileName())
                .append(DocumentToVariantSourceConverter.FILEID_FIELD, source.getFileId())
                .append(DocumentToVariantSourceConverter.STUDYNAME_FIELD, source.getStudyName())
                .append(DocumentToVariantSourceConverter.STUDYID_FIELD, source.getStudyId())
//                .append(DocumentToVariantSourceConverter.STUDYTYPE_FIELD, source.getType())
                .append(DocumentToVariantSourceConverter.DATE_FIELD, Calendar.getInstance().getTime())
                .append(DocumentToVariantSourceConverter.SAMPLES_FIELD, new Document()
                        .append("NA000" + DocumentToVariantSourceConverter.CHARACTER_TO_REPLACE_DOTS + "A", 0)
                        .append("NA001" + DocumentToVariantSourceConverter.CHARACTER_TO_REPLACE_DOTS + "B", 1)
                        .append("NA002", 2)
                        .append("NA003", 3)
                );
        // TODO Pending how to manage the consequence type ranking (calculate during reading?)

        Document mongoStats = new Document(DocumentToVariantSourceConverter.NUMSAMPLES_FIELD, global.getSamplesCount())
                .append(DocumentToVariantSourceConverter.NUMVARIANTS_FIELD, global.getVariantsCount())
                .append(DocumentToVariantSourceConverter.NUMSNPS_FIELD, global.getSnpsCount())
                .append(DocumentToVariantSourceConverter.NUMINDELS_FIELD, global.getIndelsCount())
                .append(DocumentToVariantSourceConverter.NUMPASSFILTERS_FIELD, global.getPassCount())
                .append(DocumentToVariantSourceConverter.NUMTRANSITIONS_FIELD, global.getTransitionsCount())
                .append(DocumentToVariantSourceConverter.NUMTRANSVERSIONS_FIELD, global.getTransversionsCount())
                .append(DocumentToVariantSourceConverter.MEANQUALITY_FIELD, (double) global.getMeanQuality());

        mongoSource = mongoSource.append(DocumentToVariantSourceConverter.STATS_FIELD, mongoStats);

        // TODO Save pedigree information

        // Metadata
        Document metadataMongo = new Document(DocumentToVariantSourceConverter.HEADER_FIELD, source.getMetadata().get("header"))
                .append("FORMAT" + DocumentToVariantSourceConverter.CHARACTER_TO_REPLACE_DOTS + "A", source.getMetadata().get("FORMAT.A"));
        mongoSource = mongoSource.append(DocumentToVariantSourceConverter.METADATA_FIELD, metadataMongo);
    }

    @Test
    public void testConvertToStorageType() {
        DocumentToVariantSourceConverter converter = new DocumentToVariantSourceConverter();
        Document converted = converter.convertToStorageType(source);

        converted.toString();

        assertEquals(mongoSource.get(DocumentToVariantSourceConverter.FILENAME_FIELD), converted.get(DocumentToVariantSourceConverter
                .FILENAME_FIELD));
        assertEquals(mongoSource.get(DocumentToVariantSourceConverter.FILEID_FIELD), converted.get(DocumentToVariantSourceConverter
                .FILEID_FIELD));
        assertEquals(mongoSource.get(DocumentToVariantSourceConverter.STUDYNAME_FIELD), converted.get(DocumentToVariantSourceConverter
                .STUDYNAME_FIELD));
        assertEquals(mongoSource.get(DocumentToVariantSourceConverter.STUDYID_FIELD), converted.get(DocumentToVariantSourceConverter
                .STUDYID_FIELD));
        // Exclude the date
        assertEquals(mongoSource.get(DocumentToVariantSourceConverter.SAMPLES_FIELD), converted.get(DocumentToVariantSourceConverter
                .SAMPLES_FIELD));

        Document convertedStats = (Document) converted.get(DocumentToVariantSourceConverter.STATS_FIELD);
        Document expectedStats = (Document) converted.get(DocumentToVariantSourceConverter.STATS_FIELD);
        assertEquals(expectedStats.get(DocumentToVariantSourceConverter.NUMSAMPLES_FIELD), convertedStats.get
                (DocumentToVariantSourceConverter.NUMSAMPLES_FIELD));
        assertEquals(expectedStats.get(DocumentToVariantSourceConverter.NUMVARIANTS_FIELD), convertedStats.get
                (DocumentToVariantSourceConverter.NUMVARIANTS_FIELD));
        assertEquals(expectedStats.get(DocumentToVariantSourceConverter.NUMSNPS_FIELD), convertedStats.get
                (DocumentToVariantSourceConverter.NUMSNPS_FIELD));
        assertEquals(expectedStats.get(DocumentToVariantSourceConverter.NUMINDELS_FIELD), convertedStats.get
                (DocumentToVariantSourceConverter.NUMINDELS_FIELD));
        assertEquals(expectedStats.get(DocumentToVariantSourceConverter.NUMPASSFILTERS_FIELD), convertedStats.get
                (DocumentToVariantSourceConverter.NUMPASSFILTERS_FIELD));
        assertEquals(expectedStats.get(DocumentToVariantSourceConverter.NUMTRANSITIONS_FIELD), convertedStats.get
                (DocumentToVariantSourceConverter.NUMTRANSITIONS_FIELD));
        assertEquals(expectedStats.get(DocumentToVariantSourceConverter.NUMTRANSVERSIONS_FIELD), convertedStats.get
                (DocumentToVariantSourceConverter.NUMTRANSVERSIONS_FIELD));
        assertEquals(expectedStats.get(DocumentToVariantSourceConverter.MEANQUALITY_FIELD), convertedStats.get
                (DocumentToVariantSourceConverter.MEANQUALITY_FIELD));

        Document convertedMetadata = (Document) converted.get(DocumentToVariantSourceConverter.METADATA_FIELD);
        Document expectedMetadata = (Document) converted.get(DocumentToVariantSourceConverter.METADATA_FIELD);
        assertEquals(expectedMetadata.get(DocumentToVariantSourceConverter.HEADER_FIELD), convertedMetadata.get
                (DocumentToVariantSourceConverter.HEADER_FIELD));
        assertEquals(expectedMetadata.get("FORMAT" + DocumentToVariantSourceConverter.CHARACTER_TO_REPLACE_DOTS + "A"), convertedMetadata
                .get("FORMAT" + DocumentToVariantSourceConverter.CHARACTER_TO_REPLACE_DOTS + "A"));
    }

    @Test
    public void testConvertToDataModelType() {
        DocumentToVariantSourceConverter converter = new DocumentToVariantSourceConverter();
        VariantSource converted = converter.convertToDataModelType(mongoSource);

        assertEquals(source.getFileName(), converted.getFileName());
        assertEquals(source.getFileId(), converted.getFileId());
        assertEquals(source.getStudyName(), converted.getStudyName());
        assertEquals(source.getStudyId(), converted.getStudyId());
        assertEquals(source.getType(), converted.getType());
        assertEquals(source.getSamplesPosition(), converted.getSamplesPosition());

        assertEquals(source.getStats(), converted.getStats());
        assertEquals(source.getMetadata(), converted.getMetadata());
        assertEquals(source.getPedigree(), converted.getPedigree());
    }

}
