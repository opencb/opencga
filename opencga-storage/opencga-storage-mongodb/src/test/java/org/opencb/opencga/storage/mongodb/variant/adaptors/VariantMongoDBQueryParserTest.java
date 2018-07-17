package org.opencb.opencga.storage.mongodb.variant.adaptors;

import org.bson.Document;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.opencb.commons.datastore.core.Query;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.core.metadata.StudyConfigurationManager;
import org.opencb.opencga.storage.core.variant.adaptors.GenotypeClass;
import org.opencb.opencga.storage.core.variant.dummy.DummyProjectMetadataAdaptor;
import org.opencb.opencga.storage.core.variant.dummy.DummyStudyConfigurationAdaptor;
import org.opencb.opencga.storage.core.variant.dummy.DummyVariantFileMetadataDBAdaptor;
import org.opencb.opencga.storage.mongodb.variant.MongoDBVariantStorageEngine;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;

import static org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam.*;
import static org.opencb.opencga.storage.core.variant.adaptors.VariantQueryUtils.IS;
import static org.opencb.opencga.storage.core.variant.adaptors.VariantQueryUtils.NOT;
import static org.opencb.opencga.storage.core.variant.adaptors.VariantQueryUtils.OR;
import static org.opencb.opencga.storage.mongodb.variant.converters.DocumentToStudyVariantEntryConverter.*;
import static org.opencb.opencga.storage.mongodb.variant.converters.DocumentToVariantAnnotationConverterTest.ANY;
import static org.opencb.opencga.storage.mongodb.variant.converters.DocumentToVariantAnnotationConverterTest.checkEqualDocuments;
import static org.opencb.opencga.storage.mongodb.variant.converters.DocumentToVariantConverter.STUDIES_FIELD;

/**
 * Created on 21/08/17.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class VariantMongoDBQueryParserTest {

    private VariantMongoDBQueryParser parser;

    @Before
    public void setUp() throws Exception {
        DummyStudyConfigurationAdaptor.clear();
        StudyConfigurationManager scm = new StudyConfigurationManager(new DummyProjectMetadataAdaptor(), new DummyStudyConfigurationAdaptor(), new DummyVariantFileMetadataDBAdaptor());
        parser = new VariantMongoDBQueryParser(scm);
        scm.updateStudyConfiguration(newStudyConfiguration(1, Arrays.asList(1, 2, 3), false), null);
        scm.updateStudyConfiguration(newStudyConfiguration(2, Arrays.asList(1, 2, 3), true), null);
    }

    @After
    public void tearDown() throws Exception {
        DummyStudyConfigurationAdaptor.clear();
    }

    protected StudyConfiguration newStudyConfiguration(int studyId, List<Integer> fileIds, boolean sameSamples) {
        StudyConfiguration sc = new StudyConfiguration(studyId, "study_" + studyId);
        for (Integer fileId : fileIds) {
            sc.getFileIds().put("file_" + fileId, fileId);
            LinkedHashSet<Integer> samplesInFile = new LinkedHashSet<>();
            for (int i = 1; i <= 5; i++) {
                int sampleId;
                if (sameSamples) {
                    sampleId = studyId * 10000 + i;
                } else {
                    sampleId = studyId * 10000 + 100 * fileId + i;
                }
                samplesInFile.add(sampleId);
                sc.getSampleIds().put("sample_" + sampleId, sampleId);
            }
            sc.getSamplesInFiles().put(fileId, samplesInFile);
        }
        sc.setIndexedFiles(new LinkedHashSet<>(fileIds));
        sc.getAttributes().put(MongoDBVariantStorageEngine.MongoDBVariantOptions.LOADED_GENOTYPES.key(), "0/1,1/1,?/?");
        sc.getAttributes().put(MongoDBVariantStorageEngine.MongoDBVariantOptions.DEFAULT_GENOTYPE.key(), "0/0");

        return sc;
    }

    @Test
    public void testQuerySampleAddFile() {
        Document mongoQuery = parser.parseQuery(new Query().append(STUDY.key(), "study_1").append(SAMPLE.key(), "sample_10101"));

        Document expected = new Document(STUDIES_FIELD,
                new Document("$elemMatch", new Document()
                        .append(STUDYID_FIELD, 1)
                        .append("$and", Collections.singletonList(
                                new Document("$or", Arrays.asList(
                                        new Document(GENOTYPES_FIELD + ".0/1", 10101),
                                        new Document(GENOTYPES_FIELD + ".1/1", 10101)))))
                        .append(FILES_FIELD + '.' + FILEID_FIELD, 1)));

        checkEqualDocuments(expected, mongoQuery);
    }

    @Test
    public void testQuerySampleNotAddFile() {
        // In Study2 all the files has the same samples.
        // See #641
        Document mongoQuery = parser.parseQuery(new Query().append(STUDY.key(), "study_2").append(SAMPLE.key(), "sample_20001"));

        Document expected = new Document(STUDIES_FIELD,
                new Document("$elemMatch", new Document()
                        .append(STUDYID_FIELD, 2)
                        .append("$and", Collections.singletonList(
                                new Document("$or", Arrays.asList(
                                        new Document(GENOTYPES_FIELD + ".0/1", 20001),
                                        new Document(GENOTYPES_FIELD + ".1/1", 20001)))))));

        checkEqualDocuments(expected, mongoQuery);
    }

    @Test
    public void testQuerySamplesAddFile() {
        // Filter samples from same file
        Document mongoQuery = parser.parseQuery(new Query().append(STUDY.key(), "study_1")
                .append(SAMPLE.key(), "sample_10101;sample_10102"));

        Document expected = new Document(STUDIES_FIELD,
                new Document("$elemMatch", new Document()
                        .append(STUDYID_FIELD, 1)
                        .append("$and", Arrays.asList(
                                new Document("$or", Arrays.asList(
                                        new Document(GENOTYPES_FIELD + ".0/1", 10101),
                                        new Document(GENOTYPES_FIELD + ".1/1", 10101))),
                                new Document("$or", Arrays.asList(
                                        new Document(GENOTYPES_FIELD + ".0/1", 10102),
                                        new Document(GENOTYPES_FIELD + ".1/1", 10102)))))
                        .append(FILES_FIELD + '.' + FILEID_FIELD, 1)));

        checkEqualDocuments(expected, mongoQuery);
    }

    @Test
    public void testQuerySamplesAddFiles() {
        // Filter samples from different files
        Document mongoQuery = parser.parseQuery(new Query().append(STUDY.key(), "study_1")
                .append(SAMPLE.key(), "sample_10101;sample_10201"));

        Document expected = new Document(STUDIES_FIELD,
                new Document("$elemMatch", new Document()
                        .append(STUDYID_FIELD, 1)
                        .append("$and", Arrays.asList(
                                new Document("$or", Arrays.asList(
                                        new Document(GENOTYPES_FIELD + ".0/1", 10101),
                                        new Document(GENOTYPES_FIELD + ".1/1", 10101))),
                                new Document("$or", Arrays.asList(
                                        new Document(GENOTYPES_FIELD + ".0/1", 10201),
                                        new Document(GENOTYPES_FIELD + ".1/1", 10201)))))
                        .append(FILES_FIELD + '.' + FILEID_FIELD, new Document("$all", Arrays.asList(1, 2)))));

        checkEqualDocuments(expected, mongoQuery);
    }

    @Test
    public void testQuerySamplesFromAllFiles() {
        // If filter by samples from all files, don't add the files.fid filter
        Document mongoQuery = parser.parseQuery(new Query().append(STUDY.key(), "study_1")
                .append(SAMPLE.key(), "sample_10101;sample_10201;sample_10301"));

        Document expected = new Document(STUDIES_FIELD,
                new Document("$elemMatch", new Document()
                        .append(STUDYID_FIELD, 1)
                        .append("$and", Arrays.asList(
                                new Document("$or", Arrays.asList(
                                        new Document(GENOTYPES_FIELD + ".0/1", 10101),
                                        new Document(GENOTYPES_FIELD + ".1/1", 10101))),
                                new Document("$or", Arrays.asList(
                                        new Document(GENOTYPES_FIELD + ".0/1", 10201),
                                        new Document(GENOTYPES_FIELD + ".1/1", 10201))),
                                new Document("$or", Arrays.asList(
                                        new Document(GENOTYPES_FIELD + ".0/1", 10301),
                                        new Document(GENOTYPES_FIELD + ".1/1", 10301))))
                        )));

        checkEqualDocuments(expected, mongoQuery);
    }

    @Test
    public void testQueryGenotypesAddFiles() {
        Document mongoQuery = parser.parseQuery(new Query().append(STUDY.key(), "study_1").append(GENOTYPE.key(), "sample_10101" + IS + "0/1"));

        Document expected = new Document(STUDIES_FIELD,
                new Document("$elemMatch", new Document()
                        .append(STUDYID_FIELD, 1)
                        .append("$and", Collections.singletonList(
                                new Document("$or", Arrays.asList(
                                        new Document(GENOTYPES_FIELD + ".0/1", 10101)))))
                        .append(FILES_FIELD + '.' + FILEID_FIELD, 1)));

        checkEqualDocuments(expected, mongoQuery);
    }

    @Test
    public void testQueryAnySample() {
        // Filter samples from different files
        Document mongoQuery = parser.parseQuery(new Query().append(STUDY.key(), "study_1")
                .append(SAMPLE.key(), "sample_10101,sample_10201"));

        Document expected = new Document(STUDIES_FIELD,
                new Document("$elemMatch", new Document()
                        .append(STUDYID_FIELD, 1)
                        .append("$or", Arrays.asList(
                                new Document("$or", Arrays.asList(
                                        new Document(GENOTYPES_FIELD + ".0/1", 10101),
                                        new Document(GENOTYPES_FIELD + ".1/1", 10101))),
                                new Document("$or", Arrays.asList(
                                        new Document(GENOTYPES_FIELD + ".0/1", 10201),
                                        new Document(GENOTYPES_FIELD + ".1/1", 10201)))))
                        .append(FILES_FIELD + '.' + FILEID_FIELD, new Document("$all", Arrays.asList(1, 2)))));

        System.out.println(expected.toJson());
        System.out.println(mongoQuery.toJson());
        checkEqualDocuments(expected, mongoQuery);
    }

    @Test
    public void testQueryDefaultGenotypesNotAddFiles() {
        // FILES filter should not be used when the genotype filter is the default genotype
        Document mongoQuery = parser.parseQuery(new Query()
                .append(STUDY.key(), "study_1")
                .append(GENOTYPE.key(), "sample_10101" + IS + "0/0" + OR + "1/1"));


        Document expected = new Document(STUDIES_FIELD,
                new Document("$elemMatch", new Document()
                        .append(STUDYID_FIELD, 1)
                        .append("$and", Collections.singletonList(
                                new Document("$or", Arrays.asList(
                                        new Document("$and", Arrays.asList(
                                                ANY, // $ne 0/1
                                                ANY, // $ne 1/1
                                                ANY  // $ne ?/?
                                        )),
                                        new Document(GENOTYPES_FIELD + ".1/1", 10101) // $eq 1/1
                                ))))));

        checkEqualDocuments(expected, mongoQuery);
    }

    @Test
    public void testQueryUnknownGenotypesNotAddFiles() {
        // FILES filter should not be used when the genotype filter is the unknown genotype
        Document mongoQuery = parser.parseQuery(new Query().append(STUDY.key(), "study_1").append(GENOTYPE.key(), "sample_10101" + IS + GenotypeClass.UNKNOWN_GENOTYPE));

        Document expected = new Document(STUDIES_FIELD,
                new Document("$elemMatch", new Document()
                        .append(STUDYID_FIELD, 1)
                        .append("$and", Collections.singletonList(
                                new Document("$or", Arrays.asList(
                                        new Document(GENOTYPES_FIELD + ".?/?", 10101)))))));

        checkEqualDocuments(expected, mongoQuery);
    }

    @Test
    public void testQueryNegatedGenotypes() {
        Document mongoQuery = parser.parseQuery(new Query().append(STUDY.key(), "study_1")
                .append(GENOTYPE.key(), "sample_10101" + IS + NOT + "1/1"));

        Document expected = new Document(STUDIES_FIELD,
                new Document("$elemMatch", new Document()
                        .append(STUDYID_FIELD, 1)
                        .append("$and", Collections.singletonList(ANY))));

        checkEqualDocuments(expected, mongoQuery);
    }

}