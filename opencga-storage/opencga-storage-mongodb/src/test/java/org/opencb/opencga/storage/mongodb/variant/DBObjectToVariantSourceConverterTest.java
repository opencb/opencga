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

package org.opencb.opencga.storage.mongodb.variant;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
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
public class DBObjectToVariantSourceConverterTest {

    private static BasicDBObject mongoSource;

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

        mongoSource = new BasicDBObject(DBObjectToVariantSourceConverter.FILENAME_FIELD, source.getFileName())
                .append(DBObjectToVariantSourceConverter.FILEID_FIELD, source.getFileId())
                .append(DBObjectToVariantSourceConverter.STUDYNAME_FIELD, source.getStudyName())
                .append(DBObjectToVariantSourceConverter.STUDYID_FIELD, source.getStudyId())
//                .append(DBObjectToVariantSourceConverter.STUDYTYPE_FIELD, source.getType())
                .append(DBObjectToVariantSourceConverter.DATE_FIELD, Calendar.getInstance().getTime())
                .append(DBObjectToVariantSourceConverter.SAMPLES_FIELD, new BasicDBObject()
                        .append("NA000" + DBObjectToVariantSourceConverter.CHARACTER_TO_REPLACE_DOTS + "A", 0)
                        .append("NA001" + DBObjectToVariantSourceConverter.CHARACTER_TO_REPLACE_DOTS + "B", 1)
                        .append("NA002", 2)
                        .append("NA003", 3)
                );
        // TODO Pending how to manage the consequence type ranking (calculate during reading?)

        DBObject mongoStats = new BasicDBObject(DBObjectToVariantSourceConverter.NUMSAMPLES_FIELD, global.getSamplesCount())
                .append(DBObjectToVariantSourceConverter.NUMVARIANTS_FIELD, global.getVariantsCount())
                .append(DBObjectToVariantSourceConverter.NUMSNPS_FIELD, global.getSnpsCount())
                .append(DBObjectToVariantSourceConverter.NUMINDELS_FIELD, global.getIndelsCount())
                .append(DBObjectToVariantSourceConverter.NUMPASSFILTERS_FIELD, global.getPassCount())
                .append(DBObjectToVariantSourceConverter.NUMTRANSITIONS_FIELD, global.getTransitionsCount())
                .append(DBObjectToVariantSourceConverter.NUMTRANSVERSIONS_FIELD, global.getTransversionsCount())
                .append(DBObjectToVariantSourceConverter.MEANQUALITY_FIELD, (double) global.getMeanQuality());

        mongoSource = mongoSource.append(DBObjectToVariantSourceConverter.STATS_FIELD, mongoStats);

        // TODO Save pedigree information

        // Metadata
        DBObject metadataMongo = new BasicDBObject(DBObjectToVariantSourceConverter.HEADER_FIELD, source.getMetadata().get("header"))
                .append("FORMAT" + DBObjectToVariantSourceConverter.CHARACTER_TO_REPLACE_DOTS + "A", source.getMetadata().get("FORMAT.A"));
        mongoSource = mongoSource.append(DBObjectToVariantSourceConverter.METADATA_FIELD, metadataMongo);
    }

    @Test
    public void testConvertToStorageType() {
        DBObjectToVariantSourceConverter converter = new DBObjectToVariantSourceConverter();
        DBObject converted = converter.convertToStorageType(source);

        converted.toString();

        assertEquals(mongoSource.get(DBObjectToVariantSourceConverter.FILENAME_FIELD), converted.get(DBObjectToVariantSourceConverter
                .FILENAME_FIELD));
        assertEquals(mongoSource.get(DBObjectToVariantSourceConverter.FILEID_FIELD), converted.get(DBObjectToVariantSourceConverter
                .FILEID_FIELD));
        assertEquals(mongoSource.get(DBObjectToVariantSourceConverter.STUDYNAME_FIELD), converted.get(DBObjectToVariantSourceConverter
                .STUDYNAME_FIELD));
        assertEquals(mongoSource.get(DBObjectToVariantSourceConverter.STUDYID_FIELD), converted.get(DBObjectToVariantSourceConverter
                .STUDYID_FIELD));
        // Exclude the date
        assertEquals(mongoSource.get(DBObjectToVariantSourceConverter.SAMPLES_FIELD), converted.get(DBObjectToVariantSourceConverter
                .SAMPLES_FIELD));

        DBObject convertedStats = (DBObject) converted.get(DBObjectToVariantSourceConverter.STATS_FIELD);
        DBObject expectedStats = (DBObject) converted.get(DBObjectToVariantSourceConverter.STATS_FIELD);
        assertEquals(expectedStats.get(DBObjectToVariantSourceConverter.NUMSAMPLES_FIELD), convertedStats.get
                (DBObjectToVariantSourceConverter.NUMSAMPLES_FIELD));
        assertEquals(expectedStats.get(DBObjectToVariantSourceConverter.NUMVARIANTS_FIELD), convertedStats.get
                (DBObjectToVariantSourceConverter.NUMVARIANTS_FIELD));
        assertEquals(expectedStats.get(DBObjectToVariantSourceConverter.NUMSNPS_FIELD), convertedStats.get
                (DBObjectToVariantSourceConverter.NUMSNPS_FIELD));
        assertEquals(expectedStats.get(DBObjectToVariantSourceConverter.NUMINDELS_FIELD), convertedStats.get
                (DBObjectToVariantSourceConverter.NUMINDELS_FIELD));
        assertEquals(expectedStats.get(DBObjectToVariantSourceConverter.NUMPASSFILTERS_FIELD), convertedStats.get
                (DBObjectToVariantSourceConverter.NUMPASSFILTERS_FIELD));
        assertEquals(expectedStats.get(DBObjectToVariantSourceConverter.NUMTRANSITIONS_FIELD), convertedStats.get
                (DBObjectToVariantSourceConverter.NUMTRANSITIONS_FIELD));
        assertEquals(expectedStats.get(DBObjectToVariantSourceConverter.NUMTRANSVERSIONS_FIELD), convertedStats.get
                (DBObjectToVariantSourceConverter.NUMTRANSVERSIONS_FIELD));
        assertEquals(expectedStats.get(DBObjectToVariantSourceConverter.MEANQUALITY_FIELD), convertedStats.get
                (DBObjectToVariantSourceConverter.MEANQUALITY_FIELD));

        DBObject convertedMetadata = (DBObject) converted.get(DBObjectToVariantSourceConverter.METADATA_FIELD);
        DBObject expectedMetadata = (DBObject) converted.get(DBObjectToVariantSourceConverter.METADATA_FIELD);
        assertEquals(expectedMetadata.get(DBObjectToVariantSourceConverter.HEADER_FIELD), convertedMetadata.get
                (DBObjectToVariantSourceConverter.HEADER_FIELD));
        assertEquals(expectedMetadata.get("FORMAT" + DBObjectToVariantSourceConverter.CHARACTER_TO_REPLACE_DOTS + "A"), convertedMetadata
                .get("FORMAT" + DBObjectToVariantSourceConverter.CHARACTER_TO_REPLACE_DOTS + "A"));
    }

    @Test
    public void testConvertToDataModelType() {
        DBObjectToVariantSourceConverter converter = new DBObjectToVariantSourceConverter();
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
