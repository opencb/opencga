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
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.FileEntry;
import org.opencb.commons.datastore.mongodb.GenericDocumentComplexConverter;
import org.opencb.opencga.core.testclassification.duration.ShortTests;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.metadata.models.StudyMetadata;
import org.opencb.opencga.storage.core.variant.dummy.DummyVariantStorageEngine;
import org.opencb.opencga.storage.core.variant.query.projection.VariantQueryProjection;
import org.opencb.opencga.storage.core.variant.dummy.DummyVariantStorageMetadataDBAdaptorFactory;
import org.opencb.opencga.storage.mongodb.variant.MongoDBVariantStorageOptions;

import java.util.*;

import static org.junit.Assert.assertEquals;

/**
 * @author Cristina Yenyxe Gonzalez Garcia <cyenyxe@ebi.ac.uk>
 */
@Category(ShortTests.class)
public class DocumentToStudyEntryConverterTest {

    private final Variant variant = new Variant();
    private StudyEntry studyEntry;
    private Document mongoStudy;
    private Document mongoFileWithIds;

    private List<String> sampleNames;
    private Map<String, Integer> sampleIdsMap;
    private Integer fileId;
    private Integer studyId;
    private VariantStorageMetadataManager metadataManager;
    private VariantQueryProjection variantQueryProjection;
    private StudyMetadata studyMetadata;

    @Before
    public void setUp() throws StorageEngineException {
        DummyVariantStorageEngine.clear();
        metadataManager = new VariantStorageMetadataManager(new DummyVariantStorageMetadataDBAdaptorFactory());

        // Java native class
        studyId = metadataManager.createStudy("1").getId();
        metadataManager.updateStudyMetadata(studyId, studyMetadata -> {
            studyMetadata.getAttributes().put(MongoDBVariantStorageOptions.DEFAULT_GENOTYPE.key(), "0/0");
            return studyMetadata;
        });
        sampleNames = Lists.newArrayList("NA001", "NA002", "NA003");
        fileId = metadataManager.registerFile(studyId, "file1.vcf", sampleNames);
        metadataManager.addIndexedFiles(studyId, Collections.singletonList(fileId));
//        metadataManager.updateFileMetadata(studyId, fileId, fileMetadata -> fileMetadata.setIndexStatus(TaskMetadata.Status.READY));


        studyMetadata = metadataManager.getStudyMetadata(studyId);
        List<Integer> sampleIds = metadataManager.getSampleIds(studyId, sampleNames);
        sampleIdsMap = new LinkedHashMap<>();
        for (String sampleName : sampleNames) {
            sampleIdsMap.put(sampleName, metadataManager.getSampleId(studyId, sampleName));
        }
        variantQueryProjection = new VariantQueryProjection(metadataManager, studyMetadata, sampleIds, Arrays.asList(fileId));

        studyEntry = new StudyEntry(studyId.toString());
        FileEntry fileEntry = new FileEntry(fileId.toString(), null, new HashMap<>());
        fileEntry.getData().put("QUAL", "0.01");
        fileEntry.getData().put("AN", "2.0");
        fileEntry.getData().put("do.we.accept.attribute.with.dots?", "yes");
        studyEntry.setFiles(Collections.singletonList(fileEntry));
        studyEntry.setSampleDataKeys(Collections.singletonList("GT"));


        studyEntry.setSamplesPosition(metadataManager.getSamplesPosition(studyMetadata, new LinkedHashSet<>(sampleNames)));
        studyEntry.addSampleData("NA001", "GT", "0/0");
        studyEntry.addSampleData("NA002", "GT", "0/1");
        studyEntry.addSampleData("NA003", "GT", "1/1");

        studyEntry.getSamples().forEach(s -> s.setFileIndex(0));

        // MongoDB object
        mongoStudy = new Document(DocumentToStudyEntryConverter.STUDYID_FIELD, studyId);

        Document mongoFile = new Document(DocumentToStudyEntryConverter.FILEID_FIELD, fileId);
        String dot = GenericDocumentComplexConverter.TO_REPLACE_DOTS;
        mongoFile.append(DocumentToStudyEntryConverter.ATTRIBUTES_FIELD,
                new Document("QUAL", 0.01)
                        .append("AN", 2.0)
                        .append("do" + dot + "we" + dot + "accept" + dot + "attribute" + dot + "with" + dot + "dots?", "yes")
        ).append(DocumentToStudyEntryConverter.SAMPLE_DATA_FIELD, new Document());
//        mongoFile.append(DocumentToVariantSourceEntryConverter.FORMAT_FIELD, file.getFormat());
        mongoStudy.append(DocumentToStudyEntryConverter.FILES_FIELD, Collections.singletonList(mongoFile));

        Document genotypeCodes = new Document();
//        genotypeCodes.append("def", "0/0");
        genotypeCodes.append("0/1", Collections.singletonList(sampleIds.get(1)));
        genotypeCodes.append("1/1", Collections.singletonList(sampleIds.get(2)));
        mongoStudy.append(DocumentToStudyEntryConverter.GENOTYPES_FIELD, genotypeCodes);



        mongoFileWithIds = new Document((this.mongoStudy));
        mongoFileWithIds.put(DocumentToStudyEntryConverter.GENOTYPES_FIELD, new Document());
//        ((Document) mongoFileWithIds.get("samp")).put("def", "0/0");
        ((Document) mongoFileWithIds.get(DocumentToStudyEntryConverter.GENOTYPES_FIELD)).put("0/1", Collections.singletonList(sampleIds.get(1)));
        ((Document) mongoFileWithIds.get(DocumentToStudyEntryConverter.GENOTYPES_FIELD)).put("1/1", Collections.singletonList(sampleIds.get(2)));
    }

    /* TODO move to variant converter: sourceEntry does not have stats anymore
    @Test
    public void testConvertToDataModelTypeWithStats() {
        VariantStats stats = new VariantStats(null, -1, null, null, Variant.VariantType.SNV, 0.1f, 0.01f, "A", "A/A", 10, 5, -1, -1, -1,
        -1, -1);
        stats.addGenotype(new Genotype("0/0"), 100);
        stats.addGenotype(new Genotype("0/1"), 50);
        stats.addGenotype(new Genotype("1/1"), 10);
        file.setStats(stats);
        file.getSamplesData().clear(); // TODO Samples can't be tested easily, needs a running Mongo instance
        
        Document mongoStats = new Document(DocumentToVariantStatsConverter.MAF_FIELD, 0.1);
        mongoStats.append(DocumentToVariantStatsConverter.MGF_FIELD, 0.01);
        mongoStats.append(DocumentToVariantStatsConverter.MAFALLELE_FIELD, "A");
        mongoStats.append(DocumentToVariantStatsConverter.MGFGENOTYPE_FIELD, "A/A");
        mongoStats.append(DocumentToVariantStatsConverter.MISSALLELE_FIELD, 10);
        mongoStats.append(DocumentToVariantStatsConverter.MISSGENOTYPE_FIELD, 5);
        Document genotypes = new Document();
        genotypes.append("0/0", 100);
        genotypes.append("0/1", 50);
        genotypes.append("1/1", 10);
        mongoStats.append(DocumentToVariantStatsConverter.NUMGT_FIELD, genotypes);
        mongoStudy.append(DocumentToVariantSourceEntryConverter.STATS_FIELD, mongoStats);
        
        List<String> sampleNames = null;
        DocumentToVariantSourceEntryConverter converter = new DocumentToVariantSourceEntryConverter(
                true, new DocumentToSamplesConverter(sampleNames));
        VariantSourceEntry converted = converter.convertToDataModelType(mongoStudy);
        assertEquals(file, converted);
    }

    @Test
    public void testConvertToStorageTypeWithStats() {
        VariantStats stats = new VariantStats(null, -1, null, null, Variant.VariantType.SNV, 0.1f, 0.01f, "A", "A/A", 10, 5, -1, -1, -1,
        -1, -1);
        stats.addGenotype(new Genotype("0/0"), 100);
        stats.addGenotype(new Genotype("0/1"), 50);
        stats.addGenotype(new Genotype("1/1"), 10);
        file.setStats(stats);
        
        Document mongoStats = new Document(DocumentToVariantStatsConverter.MAF_FIELD, 0.1);
        mongoStats.append(DocumentToVariantStatsConverter.MGF_FIELD, 0.01);
        mongoStats.append(DocumentToVariantStatsConverter.MAFALLELE_FIELD, "A");
        mongoStats.append(DocumentToVariantStatsConverter.MGFGENOTYPE_FIELD, "A/A");
        mongoStats.append(DocumentToVariantStatsConverter.MISSALLELE_FIELD, 10);
        mongoStats.append(DocumentToVariantStatsConverter.MISSGENOTYPE_FIELD, 5);
        Document genotypes = new Document();
        genotypes.append("0/0", 100);
        genotypes.append("0/1", 50);
        genotypes.append("1/1", 10);
        mongoStats.append(DocumentToVariantStatsConverter.NUMGT_FIELD, genotypes);
        mongoStudy.append(DocumentToVariantSourceEntryConverter.STATS_FIELD, mongoStats);

        DocumentToVariantSourceEntryConverter converter = new DocumentToVariantSourceEntryConverter(
                true, new DocumentToSamplesConverter(sampleNames));
        Document converted = converter.convertToStorageType(file);
        
        assertEquals(mongoStudy.get(DocumentToVariantStatsConverter.MAF_FIELD), converted.get(DocumentToVariantStatsConverter.MAF_FIELD));
        assertEquals(mongoStudy.get(DocumentToVariantStatsConverter.MGF_FIELD), converted.get(DocumentToVariantStatsConverter.MGF_FIELD));
        assertEquals(mongoStudy.get(DocumentToVariantStatsConverter.MAFALLELE_FIELD), converted.get(DocumentToVariantStatsConverter
        .MAFALLELE_FIELD));
        assertEquals(mongoStudy.get(DocumentToVariantStatsConverter.MGFGENOTYPE_FIELD), converted.get(DocumentToVariantStatsConverter
        .MGFGENOTYPE_FIELD));
        assertEquals(mongoStudy.get(DocumentToVariantStatsConverter.MISSALLELE_FIELD), converted.get(DocumentToVariantStatsConverter
        .MISSALLELE_FIELD));
        assertEquals(mongoStudy.get(DocumentToVariantStatsConverter.MISSGENOTYPE_FIELD), converted.get(DocumentToVariantStatsConverter
        .MISSGENOTYPE_FIELD));
        assertEquals(mongoStudy.get(DocumentToVariantStatsConverter.NUMGT_FIELD), converted.get(DocumentToVariantStatsConverter
        .NUMGT_FIELD));
    }
    */
    @Test
    public void testConvertToDataModelTypeWithoutStats() {
        List<String> sampleNames = null;

        // Test with no stats converter provided
        DocumentToStudyEntryConverter converter = new DocumentToStudyEntryConverter(true, studyId, fileId,
                new DocumentToSamplesConverter(metadataManager, variantQueryProjection));
        StudyEntry converted = converter.convertToDataModelType(mongoStudy, null);
        assertEquals(studyEntry, converted);
    }

    @Test
    public void testConvertToDataModelTypeWithoutStatsWithStatsConverter() {
        List<String> sampleNames = null;
        // Test with a stats converter provided but no stats object
        DocumentToStudyEntryConverter converter = new DocumentToStudyEntryConverter(true, studyId, fileId, new
                DocumentToSamplesConverter(metadataManager, variantQueryProjection));
        StudyEntry converted = converter.convertToDataModelType(mongoStudy, null);
        assertEquals(studyEntry, converted);
    }

    @Test
    public void testConvertToStorageTypeWithoutStats() {
        // Test with no stats converter provided
        StudyEntryToDocumentConverter converter = new StudyEntryToDocumentConverter(new SampleToDocumentConverter(studyMetadata, sampleIdsMap), true);
        Document converted = converter.convertToStorageType(variant, studyEntry);
        assertEquals(mongoStudy, converted);
    }

    @Test
    public void testConvertToStorageTypeWithoutStatsWithSampleIds() {
        DocumentToStudyEntryConverter converter;
        Document convertedMongo;
        StudyEntry convertedFile;


        // Test with no stats converter provided
        StudyEntryToDocumentConverter toDocument = new StudyEntryToDocumentConverter(new SampleToDocumentConverter(studyMetadata, sampleIdsMap), true);
        DocumentToSamplesConverter samplesConverter = new DocumentToSamplesConverter(metadataManager, variantQueryProjection);
        converter = new DocumentToStudyEntryConverter(
                true,
                studyId, fileId,
                samplesConverter);

        convertedMongo = toDocument.convertToStorageType(variant, studyEntry);
        assertEquals(mongoFileWithIds, convertedMongo);
        convertedFile = converter.convertToDataModelType(convertedMongo, null);
        assertEquals(studyEntry, convertedFile);

    }

    @Test
    public void testConvertToDataTypeWithoutStatsWithSampleIds() {
        DocumentToStudyEntryConverter converter;
        Document convertedMongo;
        StudyEntry convertedFile;


        // Test with no stats converter provided
        DocumentToSamplesConverter samplesConverter = new DocumentToSamplesConverter(metadataManager, variantQueryProjection);
        samplesConverter.setIncludeSampleId(false);
        converter = new DocumentToStudyEntryConverter(
                true,
                studyId, fileId,
                samplesConverter);
        StudyEntryToDocumentConverter toDocument = new StudyEntryToDocumentConverter(new SampleToDocumentConverter(studyMetadata, sampleIdsMap), true);

        convertedFile = converter.convertToDataModelType(mongoFileWithIds, null);
        convertedMongo = toDocument.convertToStorageType(variant, convertedFile);
        assertEquals(studyEntry, convertedFile);
        assertEquals(mongoFileWithIds, convertedMongo);

    }

}
