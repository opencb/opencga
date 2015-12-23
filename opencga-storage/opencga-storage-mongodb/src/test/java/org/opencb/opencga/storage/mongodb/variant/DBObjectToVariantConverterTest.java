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

import com.google.common.collect.Lists;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import org.junit.Before;
import org.junit.Test;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.FileEntry;
import org.opencb.commons.utils.CryptoUtils;
import org.opencb.opencga.storage.core.StudyConfiguration;
import org.opencb.opencga.storage.core.variant.VariantStorageManager;

import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * @author Cristina Yenyxe Gonzalez Garcia <cyenyxe@ebi.ac.uk>
 */
public class DBObjectToVariantConverterTest {

    private BasicDBObject mongoVariant;
    private Variant variant;
    protected StudyEntry studyEntry;
    private Integer studyId;
    private Integer fileId;

    @Before
    public void setUp() {
        //Setup variant
        variant = new Variant("1", 1000, 1000, "A", "C");
        variant.setIds(Collections.singletonList("rs666"));

        //Setup variantSourceEntry
        studyId = 1;
        fileId = 2;
        studyEntry = new StudyEntry(fileId.toString(), studyId.toString());
        FileEntry fileEntry = studyEntry.getFile(fileId.toString());
        fileEntry.getAttributes().put("QUAL", "0.01");
        fileEntry.getAttributes().put("AN", "2");
        studyEntry.setFormatAsString("GT:DP");

        Map<String, String> na001 = new HashMap<>();
        na001.put("GT", "0/0");
        na001.put("DP", "4");
        studyEntry.addSampleData("NA001", na001);
        Map<String, String> na002 = new HashMap<>();
        na002.put("GT", "0/1");
        na002.put("DP", "5");
        studyEntry.addSampleData("NA002", na002);
        variant.addStudyEntry(studyEntry);

        //Setup mongoVariant
        mongoVariant = new BasicDBObject("_id", "1_1000_A_C")
                .append(DBObjectToVariantConverter.IDS_FIELD, variant.getIds())
                .append(DBObjectToVariantConverter.TYPE_FIELD, variant.getType().name())
                .append(DBObjectToVariantConverter.CHROMOSOME_FIELD, variant.getChromosome())
                .append(DBObjectToVariantConverter.START_FIELD, variant.getStart())
                .append(DBObjectToVariantConverter.END_FIELD, variant.getStart())
                .append(DBObjectToVariantConverter.LENGTH_FIELD, variant.getLength())
                .append(DBObjectToVariantConverter.REFERENCE_FIELD, variant.getReference())
                .append(DBObjectToVariantConverter.ALTERNATE_FIELD, variant.getAlternate())
                .append(DBObjectToVariantConverter.ANNOTATION_FIELD, Collections.emptyList());

        BasicDBList chunkIds = new BasicDBList();
        chunkIds.add("1_1_1k");
        chunkIds.add("1_0_10k");
        mongoVariant.append("_at", new BasicDBObject("chunkIds", chunkIds));

        BasicDBList hgvs = new BasicDBList();
        hgvs.add(new BasicDBObject("type", "genomic").append("name", "1:g.1000A>C"));
        mongoVariant.append("hgvs", hgvs);
    }

    @Test
    public void testConvertToDataModelTypeWithFiles() {
        // MongoDB object

        BasicDBObject mongoStudy = new BasicDBObject(DBObjectToStudyVariantEntryConverter.STUDYID_FIELD, Integer.parseInt(studyEntry
                .getStudyId()));

//        mongoStudy.append(DBObjectToVariantSourceEntryConverter.FORMAT_FIELD, variantSourceEntry.getFormat());
        mongoStudy.append(DBObjectToStudyVariantEntryConverter.GENOTYPES_FIELD, new BasicDBObject("0/1", Collections.singletonList(1)))
                .append("dp", Arrays.asList(4, 5));

        BasicDBObject mongoFile = new BasicDBObject(DBObjectToStudyVariantEntryConverter.FILEID_FIELD, Integer.parseInt(studyEntry
                .getFiles().get(0).getFileId()))
                .append(DBObjectToStudyVariantEntryConverter.ATTRIBUTES_FIELD, new BasicDBObject("QUAL", 0.01).append("AN", 2));

        mongoStudy.append(DBObjectToStudyVariantEntryConverter.FILES_FIELD, Collections.singletonList(mongoFile));

        mongoVariant.append(DBObjectToVariantConverter.STUDIES_FIELD, Collections.singletonList(mongoStudy));

        StudyConfiguration studyConfiguration = new StudyConfiguration(studyId, studyId.toString(), fileId, fileId.toString());//studyId,
        // fileId, sampleNames, "0/0"
        studyConfiguration.getIndexedFiles().add(fileId);
        studyConfiguration.getSamplesInFiles().put(fileId, new LinkedHashSet<>(Arrays.asList(0, 1)));
        studyConfiguration.getSampleIds().put("NA001", 0);
        studyConfiguration.getSampleIds().put("NA002", 1);
        studyConfiguration.getAttributes().put(MongoDBVariantStorageManager.DEFAULT_GENOTYPE, "0/0");
        studyConfiguration.getAttributes().put(VariantStorageManager.Options.EXTRA_GENOTYPE_FIELDS.key(), Collections.singletonList("DP"));

        DBObjectToVariantConverter converter = new DBObjectToVariantConverter(
                new DBObjectToStudyVariantEntryConverter(
                        true,
                        new DBObjectToSamplesConverter(studyConfiguration)),
                new DBObjectToVariantStatsConverter());
        Variant converted = converter.convertToDataModelType(mongoVariant);
        assertEquals("\n" + variant.toJson() + "\n" + converted.toJson(), variant, converted);
    }

    @Test
    public void testConvertToStorageTypeWithFiles() {

        variant.addStudyEntry(studyEntry);

        // MongoDB object
        BasicDBObject mongoFile = new BasicDBObject(DBObjectToStudyVariantEntryConverter.FILEID_FIELD, fileId);

        mongoFile.append(DBObjectToStudyVariantEntryConverter.ATTRIBUTES_FIELD,
                new BasicDBObject("QUAL", 0.01).append("AN", 2));
//        mongoFile.append(DBObjectToVariantSourceEntryConverter.FORMAT_FIELD, variantSourceEntry.getFormat());

        BasicDBObject mongoStudy = new BasicDBObject(DBObjectToStudyVariantEntryConverter.STUDYID_FIELD, studyId)
                .append(DBObjectToStudyVariantEntryConverter.FILES_FIELD, Collections.singletonList(mongoFile));
        BasicDBObject genotypeCodes = new BasicDBObject();
//        genotypeCodes.append("def", "0/0");
        genotypeCodes.append("0/1", Collections.singletonList(1));
        mongoStudy.append(DBObjectToStudyVariantEntryConverter.GENOTYPES_FIELD, genotypeCodes);

        BasicDBList studies = new BasicDBList();
        studies.add(mongoStudy);
        mongoVariant.append(DBObjectToVariantConverter.STUDIES_FIELD, studies);

        List<String> sampleNames = Lists.newArrayList("NA001", "NA002");
        DBObjectToVariantConverter converter = new DBObjectToVariantConverter(
                new DBObjectToStudyVariantEntryConverter(
                        true,
                        new DBObjectToSamplesConverter(studyId, fileId, sampleNames, "0/0")),
                new DBObjectToVariantStatsConverter());
        DBObject converted = converter.convertToStorageType(variant);
        assertFalse(converted.containsField(DBObjectToVariantConverter.IDS_FIELD)); //IDs must be added manually.
        converted.put(DBObjectToVariantConverter.IDS_FIELD, variant.getIds());  //Add IDs
        mongoVariant.append(DBObjectToVariantConverter.STATS_FIELD, Collections.emptyList());
        assertEquals(mongoVariant, converted);
    }

    @Test
    public void testConvertToDataModelTypeWithoutFiles() {
        DBObjectToVariantConverter converter = new DBObjectToVariantConverter();
        Variant converted = converter.convertToDataModelType(mongoVariant);
        variant.setStudies(Collections.<StudyEntry>emptyList());
        assertEquals("\n" + variant.toJson() + "\n" + converted.toJson(), variant, converted);
    }

    @Test
    public void testConvertToStorageTypeWithoutFiles() {
        DBObjectToVariantConverter converter = new DBObjectToVariantConverter();
        DBObject converted = converter.convertToStorageType(variant);
        assertFalse(converted.containsField(DBObjectToVariantConverter.IDS_FIELD)); //IDs must be added manually.
        converted.put(DBObjectToVariantConverter.IDS_FIELD, variant.getIds());  //Add IDs
        assertEquals(mongoVariant, converted);
    }

    @Test
    public void testBuildStorageId() {
        DBObjectToVariantConverter converter = new DBObjectToVariantConverter();

        // SNV
        Variant v1 = new Variant("1", 1000, 1000, "A", "C");
        assertEquals("1_1000_A_C", converter.buildStorageId(v1));

        // Indel
        Variant v2 = new Variant("1", 1000, 1002, "", "CA");
        assertEquals("1_1000__CA", converter.buildStorageId(v2));

        // Structural
        String alt = "ACGTACGTACGTACGTACGTACGTACGTACGTACGTACGTACGTACGTACGTACGTACGT";
        Variant v3 = new Variant("1", 1000, 1002, "TAG", alt);
        assertEquals("1_1000_TAG_" + new String(CryptoUtils.encryptSha1(alt)), converter.buildStorageId(v3));
    }

}
