/*
 * Copyright 2015-2017 OpenCB
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

import com.google.common.collect.Lists;
import org.bson.Document;
import org.bson.types.Binary;
import org.junit.Before;
import org.junit.Test;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.FileEntry;
import org.opencb.commons.utils.CryptoUtils;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.core.variant.adaptors.VariantField;
import org.opencb.opencga.storage.mongodb.variant.MongoDBVariantStorageEngine;
import org.opencb.opencga.storage.mongodb.variant.protobuf.VariantMongoDBProto;

import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * @author Cristina Yenyxe Gonzalez Garcia <cyenyxe@ebi.ac.uk>
 */
public class DocumentToVariantConverterTest {

    private Document mongoVariant;
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
        mongoVariant = new Document("_id", " 1:      1000:A:C")
                .append(DocumentToVariantConverter.CHROMOSOME_FIELD, variant.getChromosome())
                .append(DocumentToVariantConverter.START_FIELD, variant.getStart())
                .append(DocumentToVariantConverter.END_FIELD, variant.getStart())
                .append(DocumentToVariantConverter.LENGTH_FIELD, variant.getLength())
                .append(DocumentToVariantConverter.REFERENCE_FIELD, variant.getReference())
                .append(DocumentToVariantConverter.ALTERNATE_FIELD, variant.getAlternate())
                .append(DocumentToVariantConverter.TYPE_FIELD, variant.getType().name())
                .append(DocumentToVariantConverter.IDS_FIELD, variant.getIds())
                .append(DocumentToVariantConverter.ANNOTATION_FIELD, Collections.emptyList());

        LinkedList chunkIds = new LinkedList();
        chunkIds.add("1_1_1k");
        chunkIds.add("1_0_10k");
        mongoVariant.append(DocumentToVariantConverter.AT_FIELD, new Document("chunkIds", chunkIds));

        LinkedList hgvs = new LinkedList();
//        hgvs.add(new Document("type", "genomic").append("name", "1:g.1000A>C"));
        mongoVariant.append("hgvs", hgvs);
    }

    @Test
    public void testConvertToDataModelTypeWithFiles() {
        // MongoDB object

        Document mongoStudy = new Document(DocumentToStudyVariantEntryConverter.STUDYID_FIELD, Integer.parseInt(studyEntry
                .getStudyId()));

//        mongoStudy.append(DocumentToVariantSourceEntryConverter.FORMAT_FIELD, variantSourceEntry.getFormat());
        mongoStudy.append(DocumentToStudyVariantEntryConverter.GENOTYPES_FIELD, new Document("0/1", Collections.singletonList(1)));

        Document mongoFile = new Document(DocumentToStudyVariantEntryConverter.FILEID_FIELD, Integer.parseInt(studyEntry
                .getFiles().get(0).getFileId()))
                .append(DocumentToStudyVariantEntryConverter.ATTRIBUTES_FIELD, new Document("QUAL", 0.01).append("AN", 2))
                .append(DocumentToStudyVariantEntryConverter.SAMPLE_DATA_FIELD, new Document("dp", new Binary(VariantMongoDBProto.OtherFields.newBuilder()
                        .addIntValues(DocumentToSamplesConverter.INTEGER_COMPLEX_TYPE_CONVERTER.convertToStorageType("4"))
                        .addIntValues(DocumentToSamplesConverter.INTEGER_COMPLEX_TYPE_CONVERTER.convertToStorageType("5")).build().toByteArray())));

        mongoStudy.append(DocumentToStudyVariantEntryConverter.FILES_FIELD, Collections.singletonList(mongoFile));

        mongoVariant.append(DocumentToVariantConverter.STUDIES_FIELD, Collections.singletonList(mongoStudy));

        StudyConfiguration studyConfiguration = new StudyConfiguration(studyId, studyId.toString(), fileId, fileId.toString());//studyId,
        // fileId, sampleNames, "0/0"
        studyConfiguration.getIndexedFiles().add(fileId);
        studyConfiguration.getSamplesInFiles().put(fileId, new LinkedHashSet<>(Arrays.asList(0, 1)));
        studyConfiguration.getSampleIds().put("NA001", 0);
        studyConfiguration.getSampleIds().put("NA002", 1);
        studyConfiguration.getAttributes().put(MongoDBVariantStorageEngine.MongoDBVariantOptions.DEFAULT_GENOTYPE.key(), "0/0");
        studyConfiguration.getAttributes().put(VariantStorageEngine.Options.EXTRA_GENOTYPE_FIELDS.key(), Collections.singletonList("DP"));

        DocumentToVariantConverter converter = new DocumentToVariantConverter(
                new DocumentToStudyVariantEntryConverter(
                        true,
                        new DocumentToSamplesConverter(studyConfiguration)),
                new DocumentToVariantStatsConverter());
        Variant converted = converter.convertToDataModelType(mongoVariant);
        assertEquals("\n" + variant.toJson() + "\n" + converted.toJson(), variant, converted);
    }

    @Test
    public void testConvertToStorageTypeWithFiles() {

        variant.addStudyEntry(studyEntry);

        // MongoDB object
        Document mongoFile = new Document(DocumentToStudyVariantEntryConverter.FILEID_FIELD, fileId);

        mongoFile.append(DocumentToStudyVariantEntryConverter.ATTRIBUTES_FIELD,
                new Document("QUAL", 0.01).append("AN", 2))
                .append(DocumentToStudyVariantEntryConverter.SAMPLE_DATA_FIELD, new Document());
//        mongoFile.append(DocumentToVariantSourceEntryConverter.FORMAT_FIELD, variantSourceEntry.getFormat());

        Document mongoStudy = new Document(DocumentToStudyVariantEntryConverter.STUDYID_FIELD, studyId)
                .append(DocumentToStudyVariantEntryConverter.FILES_FIELD, Collections.singletonList(mongoFile));
        Document genotypeCodes = new Document();
//        genotypeCodes.append("def", "0/0");
        genotypeCodes.append("0/1", Collections.singletonList(1));
        mongoStudy.append(DocumentToStudyVariantEntryConverter.GENOTYPES_FIELD, genotypeCodes);

        LinkedList studies = new LinkedList();
        studies.add(mongoStudy);
        mongoVariant.append(DocumentToVariantConverter.STUDIES_FIELD, studies);

        List<String> sampleNames = Lists.newArrayList("NA001", "NA002");
        DocumentToVariantConverter converter = new DocumentToVariantConverter(
                new DocumentToStudyVariantEntryConverter(
                        true,
                        new DocumentToSamplesConverter(studyId, fileId, sampleNames, "0/0")),
                new DocumentToVariantStatsConverter());
        Document converted = converter.convertToStorageType(variant);
        assertFalse(converted.containsKey(DocumentToVariantConverter.IDS_FIELD)); //IDs must be added manually.
        converted.put(DocumentToVariantConverter.IDS_FIELD, variant.getIds());  //Add IDs
        mongoVariant.append(DocumentToVariantConverter.STATS_FIELD, Collections.emptyList());
        assertEquals("\n" + mongoVariant.toJson() + "\n" + converted.toJson(), mongoVariant, converted);
    }

    @Test
    public void testConvertToDataModelTypeWithoutFiles() {
        DocumentToVariantConverter converter = new DocumentToVariantConverter();
        Variant converted = converter.convertToDataModelType(mongoVariant);
        variant.setStudies(Collections.<StudyEntry>emptyList());
        assertEquals("\n" + variant.toJson() + "\n" + converted.toJson(), variant, converted);
    }

    @Test
    public void testConvertToStorageTypeWithoutFiles() {
        DocumentToVariantConverter converter = new DocumentToVariantConverter();
        Document converted = converter.convertToStorageType(variant);
        assertFalse(converted.containsKey(DocumentToVariantConverter.IDS_FIELD)); //IDs must be added manually.
        converted.put(DocumentToVariantConverter.IDS_FIELD, variant.getIds());  //Add IDs
        assertEquals(mongoVariant, converted);
    }

    @Test
    public void testBuildStorageId() {
        DocumentToVariantConverter converter = new DocumentToVariantConverter();

        // SNV
        Variant v1 = new Variant("1", 1000, 1000, "A", "C");
        assertEquals(" 1:      1000:A:C", converter.buildStorageId(v1));

        // Indel
        Variant v2 = new Variant("1", 1000, 1002, "", "CA");
        assertEquals(" 1:      1000::CA", converter.buildStorageId(v2));

        // Structural
        String alt = "ACGTACGTACGTACGTACGTACGTACGTACGTACGTACGTACGTACGTACGTACGTACGT";
        Variant v3 = new Variant("1", 1000, 1002, "TAG", alt);
        assertEquals(" 1:      1000:TAG:" + new String(CryptoUtils.encryptSha1(alt)), converter.buildStorageId(v3));
    }

    @Test
    public void testFieldsMap() {
        assertEquals(VariantField.values().length, DocumentToVariantConverter.FIELDS_MAP.size());
    }

}
