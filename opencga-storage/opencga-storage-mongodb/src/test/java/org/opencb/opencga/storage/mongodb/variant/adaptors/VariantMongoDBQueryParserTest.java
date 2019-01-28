package org.opencb.opencga.storage.mongodb.variant.adaptors;

import org.bson.Document;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.core.metadata.StudyConfigurationManager;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.core.variant.adaptors.GenotypeClass;
import org.opencb.opencga.storage.core.variant.dummy.DummyProjectMetadataAdaptor;
import org.opencb.opencga.storage.core.variant.dummy.DummyStudyConfigurationAdaptor;
import org.opencb.opencga.storage.core.variant.dummy.DummyVariantFileMetadataDBAdaptor;
import org.opencb.opencga.storage.mongodb.variant.MongoDBVariantStorageEngine;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;

import static org.opencb.opencga.storage.core.variant.adaptors.VariantField.*;
import static org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam.*;
import static org.opencb.opencga.storage.core.variant.adaptors.VariantQueryUtils.*;
import static org.opencb.opencga.storage.mongodb.variant.converters.DocumentToStudyVariantEntryConverter.*;
import static org.opencb.opencga.storage.mongodb.variant.converters.DocumentToVariantAnnotationConverterTest.ANY;
import static org.opencb.opencga.storage.mongodb.variant.converters.DocumentToVariantAnnotationConverterTest.checkEqualDocuments;
import static org.opencb.opencga.storage.mongodb.variant.converters.DocumentToVariantConverter.*;

/**
 * Created on 21/08/17.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class VariantMongoDBQueryParserTest {

    private VariantMongoDBQueryParser parser;
    private StudyConfigurationManager scm;

    @Before
    public void setUp() throws Exception {
        DummyStudyConfigurationAdaptor.clear();
        scm = new StudyConfigurationManager(new DummyProjectMetadataAdaptor(), new DummyStudyConfigurationAdaptor(), new DummyVariantFileMetadataDBAdaptor());
        parser = new VariantMongoDBQueryParser(scm);
        scm.updateStudyConfiguration(newStudyConfiguration(1, Arrays.asList(1, 2, 3, 4), false), null);
        scm.updateStudyConfiguration(newStudyConfiguration(2, Arrays.asList(1, 2, 3, 4), true), null);
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
        sc.getAttributes().put(VariantStorageEngine.Options.LOADED_GENOTYPES.key(), "0/1,1/1,?/?");
        sc.getAttributes().put(MongoDBVariantStorageEngine.MongoDBVariantOptions.DEFAULT_GENOTYPE.key(), "0/0");

        return sc;
    }

    @Test
    public void testQuerySampleAddFile() {
        Document mongoQuery = parser.parseQuery(new Query().append(STUDY.key(), "study_1").append(SAMPLE.key(), "sample_10101"));

        Document expected = new Document(STUDIES_FIELD + '.' + STUDYID_FIELD, 1)
                .append("$and", Collections.singletonList(
                        new Document("$or", Arrays.asList(
                                new Document(STUDIES_FIELD + '.' + GENOTYPES_FIELD + ".0/1", 10101),
                                new Document(STUDIES_FIELD + '.' + GENOTYPES_FIELD + ".1/1", 10101)))))
                .append(STUDIES_FIELD + '.' + FILES_FIELD + '.' + FILEID_FIELD, 1);

        checkEqualDocuments(expected, mongoQuery);
    }

    @Test
    public void testQuerySampleNotAddFile() {
        // In Study2 all the files have the same samples.
        // See #641
        Document mongoQuery = parser.parseQuery(new Query().append(STUDY.key(), "study_2").append(SAMPLE.key(), "sample_20001"));

        Document expected = new Document(STUDIES_FIELD + '.' + STUDYID_FIELD, 2)
                .append("$and", Collections.singletonList(
                        new Document("$or", Arrays.asList(
                                new Document(STUDIES_FIELD + '.' + GENOTYPES_FIELD + ".0/1", 20001),
                                new Document(STUDIES_FIELD + '.' + GENOTYPES_FIELD + ".1/1", 20001)))));

        checkEqualDocuments(expected, mongoQuery);
    }

    @Test
    public void testQuerySampleAddMultipleFile() {
        StudyConfiguration sc = scm.getStudyConfiguration(2, null).first();
        sc.getIndexedFiles().add(4);
        sc.getSamplesInFiles().put(4, new LinkedHashSet<>());
        scm.updateStudyConfiguration(sc, null);

        // Now, in Study2, not all the files have the same samples, so the parser must add fileId : $in:[1,2,3]
        // Improvement to #641
        Document mongoQuery = parser.parseQuery(new Query().append(STUDY.key(), "study_2").append(SAMPLE.key(), "sample_20001"));

        Document expected = new Document(STUDIES_FIELD + '.' + STUDYID_FIELD, 2)
                .append("$and", Collections.singletonList(
                        new Document("$or", Arrays.asList(
                                new Document(STUDIES_FIELD + '.' + GENOTYPES_FIELD + ".0/1", 20001),
                                new Document(STUDIES_FIELD + '.' + GENOTYPES_FIELD + ".1/1", 20001)))))
                .append(STUDIES_FIELD + '.' + FILES_FIELD + '.' + FILEID_FIELD, new Document("$in", Arrays.asList(1, 2, 3)));

        checkEqualDocuments(expected, mongoQuery);
    }

    @Test
    public void testQuerySampleAddMultipleFileOr() {
        StudyConfiguration sc = scm.getStudyConfiguration(2, null).first();
        sc.getSampleIds().put("sample_20004", 4);
        sc.getSampleIds().put("sample_20005", 5);
        sc.getSampleIds().put("sample_20006", 6);
        sc.getIndexedFiles().add(4);
        sc.getSamplesInFiles().put(4, new LinkedHashSet<>(Arrays.asList(4, 5, 6)));
        sc.getIndexedFiles().add(5);
        sc.getSamplesInFiles().put(5, new LinkedHashSet<>(Arrays.asList(4, 5, 6)));
        sc.getIndexedFiles().add(10);
        sc.getSamplesInFiles().put(10, new LinkedHashSet<>(Arrays.asList(10)));
        scm.updateStudyConfiguration(sc, null);

        // Now, in Study2, not all the files have the same samples, so the parser must add fileId : $in:[1,2,3]
        // Improvement to #641
        Document mongoQuery = parser.parseQuery(new Query().append(STUDY.key(), "study_2").append(GENOTYPE.key(), "sample_20001:0/1,sample_20004:0/1"));

        Document expected = new Document(STUDIES_FIELD + '.' + STUDYID_FIELD, 2)
                .append("$or", Arrays.asList(
                        new Document("$or", Arrays.asList(new Document(STUDIES_FIELD + '.' + GENOTYPES_FIELD + ".0/1", 4)))
                                .append(STUDIES_FIELD + '.' + FILES_FIELD + '.' + FILEID_FIELD, new Document("$in", Arrays.asList(4, 5))),
                        new Document("$or", Arrays.asList(new Document(STUDIES_FIELD + '.' + GENOTYPES_FIELD + ".0/1", 20001)))
                                .append(STUDIES_FIELD + '.' + FILES_FIELD + '.' + FILEID_FIELD, new Document("$in", Arrays.asList(1, 2, 3)))
                ));

        checkEqualDocuments(expected, mongoQuery);
    }

    @Test
    public void testQuerySampleAddMultipleFileAnd() {
        StudyConfiguration sc = scm.getStudyConfiguration(2, null).first();
        sc.getSampleIds().put("sample_20004", 4);
        sc.getSampleIds().put("sample_20005", 5);
        sc.getSampleIds().put("sample_20006", 6);
        sc.getIndexedFiles().add(4);
        sc.getSamplesInFiles().put(4, new LinkedHashSet<>(Arrays.asList(4, 5, 6)));
        sc.getIndexedFiles().add(5);
        sc.getSamplesInFiles().put(5, new LinkedHashSet<>(Arrays.asList(4, 5, 6)));
        sc.getIndexedFiles().add(10);
        sc.getSamplesInFiles().put(10, new LinkedHashSet<>(Arrays.asList(10)));
        scm.updateStudyConfiguration(sc, null);

        // Now, in Study2, not all the files have the same samples, so the parser must add fileId : $in:[1,2,3]
        // Improvement to #641
        Document mongoQuery = parser.parseQuery(new Query().append(STUDY.key(), "study_2").append(GENOTYPE.key(), "sample_20001:0/1;sample_20004:0/1"));

        Document expected = new Document(STUDIES_FIELD + '.' + STUDYID_FIELD, 2)
                .append("$and", Arrays.asList(
                        new Document("$or", Arrays.asList(new Document(STUDIES_FIELD + '.' + GENOTYPES_FIELD + ".0/1", 4))),
                        new Document("$or", Arrays.asList(new Document(STUDIES_FIELD + '.' + GENOTYPES_FIELD + ".0/1", 20001))),
                        new Document(STUDIES_FIELD + '.' + FILES_FIELD + '.' + FILEID_FIELD, new Document("$in", Arrays.asList(1, 2, 3))),
                        new Document(STUDIES_FIELD + '.' + FILES_FIELD + '.' + FILEID_FIELD, new Document("$in", Arrays.asList(4, 5)))
                ));

        checkEqualDocuments(expected, mongoQuery);
    }

    @Test
    public void testQuerySamplesAddFile() {
        // Filter samples from same file
        Document mongoQuery = parser.parseQuery(new Query().append(STUDY.key(), "study_1")
                .append(SAMPLE.key(), "sample_10101;sample_10102"));

        Document expected = new Document(STUDIES_FIELD + '.' + STUDYID_FIELD, 1)
                .append("$and", Arrays.asList(
                        new Document("$or", Arrays.asList(
                                new Document(STUDIES_FIELD + '.' + GENOTYPES_FIELD + ".0/1", 10101),
                                new Document(STUDIES_FIELD + '.' + GENOTYPES_FIELD + ".1/1", 10101))),
                        new Document("$or", Arrays.asList(
                                new Document(STUDIES_FIELD + '.' + GENOTYPES_FIELD + ".0/1", 10102),
                                new Document(STUDIES_FIELD + '.' + GENOTYPES_FIELD + ".1/1", 10102)))))
                .append(STUDIES_FIELD + '.' + FILES_FIELD + '.' + FILEID_FIELD, 1);

        checkEqualDocuments(expected, mongoQuery);
    }

    @Test
    public void testQuerySamplesANDFileANDAddFile() {
        // Filter samples from same file
        Document mongoQuery = parser.parseQuery(new Query().append(STUDY.key(), "study_1")
                .append(FILE.key(), "file_1;file_3")
                .append(SAMPLE.key(), "sample_10101;sample_10201"));

        Document expected = new Document(STUDIES_FIELD + '.' + STUDYID_FIELD, 1)
                .append("$and", Arrays.asList(
                        new Document("$or", Arrays.asList(
                                new Document(STUDIES_FIELD + '.' + GENOTYPES_FIELD + ".0/1", 10101),
                                new Document(STUDIES_FIELD + '.' + GENOTYPES_FIELD + ".1/1", 10101))),
                        new Document("$or", Arrays.asList(
                                new Document(STUDIES_FIELD + '.' + GENOTYPES_FIELD + ".0/1", 10201),
                                new Document(STUDIES_FIELD + '.' + GENOTYPES_FIELD + ".1/1", 10201)))
                ))
                .append(STUDIES_FIELD + '.' + FILES_FIELD + '.' + FILEID_FIELD, new Document("$all", Arrays.asList(1, 2, 3)));

        checkEqualDocuments(expected, mongoQuery);
    }

    @Test
    public void testQuerySamplesANDFileORAddFile() {
        // Filter samples from same file
        Document mongoQuery = parser.parseQuery(new Query().append(STUDY.key(), "study_1")
                .append(FILE.key(), "file_1,file_3")
                .append(SAMPLE.key(), "sample_10101;sample_10201"));

        Document expected = new Document(STUDIES_FIELD + '.' + STUDYID_FIELD, 1)
                .append("$and", Arrays.asList(
                        new Document("$or", Arrays.asList(
                                new Document(STUDIES_FIELD + '.' + GENOTYPES_FIELD + ".0/1", 10101),
                                new Document(STUDIES_FIELD + '.' + GENOTYPES_FIELD + ".1/1", 10101))),
                        new Document("$or", Arrays.asList(
                                new Document(STUDIES_FIELD + '.' + GENOTYPES_FIELD + ".0/1", 10201),
                                new Document(STUDIES_FIELD + '.' + GENOTYPES_FIELD + ".1/1", 10201))),
                        new Document(STUDIES_FIELD + '.' + FILES_FIELD + '.' + FILEID_FIELD, new Document("$in", Arrays.asList(1, 3))),
                        new Document(STUDIES_FIELD + '.' + FILES_FIELD + '.' + FILEID_FIELD, new Document("$all", Arrays.asList(1, 2)))
                ));

        checkEqualDocuments(expected, mongoQuery);
    }

    @Test
    public void testQuerySamplesANDFileNegatedAddFile() {
        // Filter samples from same file
        Document mongoQuery = parser.parseQuery(new Query().append(STUDY.key(), "study_1")
                .append(FILE.key(), "!file_3;!file_4")
                .append(SAMPLE.key(), "sample_10101;sample_10201"));

        Document expected = new Document(STUDIES_FIELD + '.' + STUDYID_FIELD, 1)
                .append("$and", Arrays.asList(
                        new Document("$or", Arrays.asList(
                                new Document(STUDIES_FIELD + '.' + GENOTYPES_FIELD + ".0/1", 10101),
                                new Document(STUDIES_FIELD + '.' + GENOTYPES_FIELD + ".1/1", 10101))),
                        new Document("$or", Arrays.asList(
                                new Document(STUDIES_FIELD + '.' + GENOTYPES_FIELD + ".0/1", 10201),
                                new Document(STUDIES_FIELD + '.' + GENOTYPES_FIELD + ".1/1", 10201))),
                        new Document(STUDIES_FIELD + '.' + FILES_FIELD + '.' + FILEID_FIELD, new Document("$all", Arrays.asList(1, 2))),
                        new Document(STUDIES_FIELD + '.' + FILES_FIELD + '.' + FILEID_FIELD, new Document("$nin", Arrays.asList(3, 4)))
                ));

        checkEqualDocuments(expected, mongoQuery);
    }

    @Test
    public void testQuerySamplesORFileANDAddFile() {
        // Filter samples from same file
        Document mongoQuery = parser.parseQuery(new Query().append(STUDY.key(), "study_1")
                .append(FILE.key(), "file_1;file_3")
                .append(SAMPLE.key(), "sample_10101,sample_10201"));

        Document expected = new Document(STUDIES_FIELD + '.' + STUDYID_FIELD, 1)
                .append("$or", Arrays.asList(
                        new Document("$or", Arrays.asList(
                                new Document(STUDIES_FIELD + '.' + GENOTYPES_FIELD + ".0/1", 10101),
                                new Document(STUDIES_FIELD + '.' + GENOTYPES_FIELD + ".1/1", 10101)))
                                .append(STUDIES_FIELD + '.' + FILES_FIELD + '.' + FILEID_FIELD, 1),
                        new Document("$or", Arrays.asList(
                                new Document(STUDIES_FIELD + '.' + GENOTYPES_FIELD + ".0/1", 10201),
                                new Document(STUDIES_FIELD + '.' + GENOTYPES_FIELD + ".1/1", 10201)))
                                .append(STUDIES_FIELD + '.' + FILES_FIELD + '.' + FILEID_FIELD, 2)
                ))
                .append(STUDIES_FIELD + '.' + FILES_FIELD + '.' + FILEID_FIELD, new Document("$all", Arrays.asList(1, 3)));

        checkEqualDocuments(expected, mongoQuery);
    }

    @Test
    public void testQuerySamplesORFileORAddFile() {
        // Filter samples from same file
        Document mongoQuery = parser.parseQuery(new Query().append(STUDY.key(), "study_1")
                .append(FILE.key(), "file_1,file_3")
                .append(SAMPLE.key(), "sample_10101,sample_10201"));

        Document expected = new Document(STUDIES_FIELD + '.' + STUDYID_FIELD, 1)
                .append("$or", Arrays.asList(
                        new Document("$or", Arrays.asList(
                                new Document(STUDIES_FIELD + '.' + GENOTYPES_FIELD + ".0/1", 10101),
                                new Document(STUDIES_FIELD + '.' + GENOTYPES_FIELD + ".1/1", 10101)))
                                .append(STUDIES_FIELD + '.' + FILES_FIELD + '.' + FILEID_FIELD, 1),
                        new Document("$or", Arrays.asList(
                                new Document(STUDIES_FIELD + '.' + GENOTYPES_FIELD + ".0/1", 10201),
                                new Document(STUDIES_FIELD + '.' + GENOTYPES_FIELD + ".1/1", 10201)))
                                .append(STUDIES_FIELD + '.' + FILES_FIELD + '.' + FILEID_FIELD, 2)))
                .append(STUDIES_FIELD + '.' + FILES_FIELD + '.' + FILEID_FIELD, new Document("$in", Arrays.asList(1, 3)));

        checkEqualDocuments(expected, mongoQuery);
    }

    @Test
    public void testQuerySamplesORFileNegatedAddFile() {
        // Filter samples from same file
        Document mongoQuery = parser.parseQuery(new Query().append(STUDY.key(), "study_1")
                .append(FILE.key(), "!file_3;!file_4")
                .append(SAMPLE.key(), "sample_10101,sample_10201"));

        Document expected = new Document(STUDIES_FIELD + '.' + STUDYID_FIELD, 1)
                .append("$or", Arrays.asList(
                        new Document("$or", Arrays.asList(
                                new Document(STUDIES_FIELD + '.' + GENOTYPES_FIELD + ".0/1", 10101),
                                new Document(STUDIES_FIELD + '.' + GENOTYPES_FIELD + ".1/1", 10101)))
                                .append(STUDIES_FIELD + '.' + FILES_FIELD + '.' + FILEID_FIELD, 1),
                        new Document("$or", Arrays.asList(
                                new Document(STUDIES_FIELD + '.' + GENOTYPES_FIELD + ".0/1", 10201),
                                new Document(STUDIES_FIELD + '.' + GENOTYPES_FIELD + ".1/1", 10201)))
                                .append(STUDIES_FIELD + '.' + FILES_FIELD + '.' + FILEID_FIELD, 2)))
                .append(STUDIES_FIELD + '.' + FILES_FIELD + '.' + FILEID_FIELD, new Document("$nin", Arrays.asList(3, 4)));

        checkEqualDocuments(expected, mongoQuery);
    }

    @Test
    public void testQuerySamplesAddFiles() {
        // Filter samples from different files
        Document mongoQuery = parser.parseQuery(new Query().append(STUDY.key(), "study_1")
                .append(SAMPLE.key(), "sample_10101;sample_10201"));

        Document expected = new Document(STUDIES_FIELD + '.' + STUDYID_FIELD, 1)
                .append("$and", Arrays.asList(
                        new Document("$or", Arrays.asList(
                                new Document(STUDIES_FIELD + '.' + GENOTYPES_FIELD + ".0/1", 10101),
                                new Document(STUDIES_FIELD + '.' + GENOTYPES_FIELD + ".1/1", 10101))),
                        new Document("$or", Arrays.asList(
                                new Document(STUDIES_FIELD + '.' + GENOTYPES_FIELD + ".0/1", 10201),
                                new Document(STUDIES_FIELD + '.' + GENOTYPES_FIELD + ".1/1", 10201)))))
                .append(STUDIES_FIELD + '.' + FILES_FIELD + '.' + FILEID_FIELD, new Document("$all", Arrays.asList(1, 2)));

        checkEqualDocuments(expected, mongoQuery);
    }

    @Test
    public void testQueryAnySamplesFromAllFiles() {
        // If filter by any samples from all files, don't add the files.fid filter
        Document mongoQuery = parser.parseQuery(new Query().append(STUDY.key(), "study_1")
                .append(SAMPLE.key(), "sample_10101,sample_10201,sample_10301"));

        Document expected = new Document(STUDIES_FIELD + '.' + STUDYID_FIELD, 1)
                .append("$or", Arrays.asList(
                        new Document("$or", Arrays.asList(
                                new Document(STUDIES_FIELD + '.' + GENOTYPES_FIELD + ".0/1", 10101),
                                new Document(STUDIES_FIELD + '.' + GENOTYPES_FIELD + ".1/1", 10101)))
                                .append(STUDIES_FIELD + '.' + FILES_FIELD + '.' + FILEID_FIELD, 1),
                        new Document("$or", Arrays.asList(
                                new Document(STUDIES_FIELD + '.' + GENOTYPES_FIELD + ".0/1", 10201),
                                new Document(STUDIES_FIELD + '.' + GENOTYPES_FIELD + ".1/1", 10201)))
                                .append(STUDIES_FIELD + '.' + FILES_FIELD + '.' + FILEID_FIELD, 2),
                        new Document("$or", Arrays.asList(
                                new Document(STUDIES_FIELD + '.' + GENOTYPES_FIELD + ".0/1", 10301),
                                new Document(STUDIES_FIELD + '.' + GENOTYPES_FIELD + ".1/1", 10301)))
                                .append(STUDIES_FIELD + '.' + FILES_FIELD + '.' + FILEID_FIELD, 3))
                );

        checkEqualDocuments(expected, mongoQuery);
    }

    @Test
    public void testQueryAllSamplesFromAllFiles2() {
        // If filter by ALL samples from ALL files, add the files.fid filter with $all
        Document mongoQuery = parser.parseQuery(new Query().append(STUDY.key(), "study_1")
                .append(SAMPLE.key(), "sample_10101;sample_10201;sample_10301"));

        Document expected = new Document(STUDIES_FIELD + '.' + STUDYID_FIELD, 1)
                .append("$and", Arrays.asList(
                        new Document("$or", Arrays.asList(
                                new Document(STUDIES_FIELD + '.' + GENOTYPES_FIELD + ".0/1", 10101),
                                new Document(STUDIES_FIELD + '.' + GENOTYPES_FIELD + ".1/1", 10101))),
                        new Document("$or", Arrays.asList(
                                new Document(STUDIES_FIELD + '.' + GENOTYPES_FIELD + ".0/1", 10201),
                                new Document(STUDIES_FIELD + '.' + GENOTYPES_FIELD + ".1/1", 10201))),
                        new Document("$or", Arrays.asList(
                                new Document(STUDIES_FIELD + '.' + GENOTYPES_FIELD + ".0/1", 10301),
                                new Document(STUDIES_FIELD + '.' + GENOTYPES_FIELD + ".1/1", 10301))))
                )
                .append(STUDIES_FIELD + '.' + FILES_FIELD + '.' + FILEID_FIELD, new Document("$all", Arrays.asList(1, 2, 3)));

        checkEqualDocuments(expected, mongoQuery);
    }

    @Test
    public void testQueryGenotypesAddFiles() {
        Document mongoQuery = parser.parseQuery(new Query().append(STUDY.key(), "study_1").append(GENOTYPE.key(), "sample_10101" + IS + "0/1"));

        Document expected = new Document(STUDIES_FIELD + '.' + STUDYID_FIELD, 1)
                .append("$and", Collections.singletonList(
                        new Document("$or", Arrays.asList(
                                new Document(STUDIES_FIELD + '.' + GENOTYPES_FIELD + ".0/1", 10101)))))
                .append(STUDIES_FIELD + '.' + FILES_FIELD + '.' + FILEID_FIELD, 1);

        checkEqualDocuments(expected, mongoQuery);
    }

    @Test
    public void testQueryAnySample() {
        // Filter samples from different files
        Document mongoQuery = parser.parseQuery(new Query().append(STUDY.key(), "study_1")
                .append(SAMPLE.key(), "sample_10101,sample_10201"));

        Document expected = new Document(STUDIES_FIELD + '.' + STUDYID_FIELD, 1)
                .append("$or", Arrays.asList(
                        new Document("$or", Arrays.asList(
                                new Document(STUDIES_FIELD + '.' + GENOTYPES_FIELD + ".0/1", 10101),
                                new Document(STUDIES_FIELD + '.' + GENOTYPES_FIELD + ".1/1", 10101)))
                                .append(STUDIES_FIELD + '.' + FILES_FIELD + '.' + FILEID_FIELD, 1),
                        new Document("$or", Arrays.asList(
                                new Document(STUDIES_FIELD + '.' + GENOTYPES_FIELD + ".0/1", 10201),
                                new Document(STUDIES_FIELD + '.' + GENOTYPES_FIELD + ".1/1", 10201)))
                                .append(STUDIES_FIELD + '.' + FILES_FIELD + '.' + FILEID_FIELD, 2)));

        checkEqualDocuments(expected, mongoQuery);
    }

    @Test
    public void testQueryDefaultGenotypesNotAddFiles() {
        // FILES filter should not be used when the genotype filter is the default genotype
        Document mongoQuery = parser.parseQuery(new Query()
                .append(STUDY.key(), "study_1")
                .append(GENOTYPE.key(), "sample_10101" + IS + "0/0" + OR + "1/1"));


        Document expected = new Document(STUDIES_FIELD + '.' + STUDYID_FIELD, 1)
                .append("$and", Collections.singletonList(
                        new Document("$or", Arrays.asList(
                                new Document("$and", Arrays.asList(
                                        ANY, // $ne 0/1
                                        ANY, // $ne 1/1
                                        ANY  // $ne ?/?
                                )),
                                new Document(STUDIES_FIELD + '.' + GENOTYPES_FIELD + ".1/1", 10101) // $eq 1/1
                        ))))
                .append(STUDIES_FIELD + '.' + FILES_FIELD + '.' + FILEID_FIELD, 1);
        checkEqualDocuments(expected, mongoQuery);
    }

    @Test
    public void testQueryUnknownGenotypesNotAddFiles() {
        // FILES filter should not be used when the genotype filter is the unknown genotype
        Document mongoQuery = parser.parseQuery(new Query().append(STUDY.key(), "study_1").append(GENOTYPE.key(), "sample_10101" + IS + GenotypeClass.UNKNOWN_GENOTYPE));

        Document expected = new Document(STUDIES_FIELD + '.' + STUDYID_FIELD, 1)
                .append("$and", Collections.singletonList(
                        new Document("$or", Arrays.asList(
                                new Document(STUDIES_FIELD + '.' + GENOTYPES_FIELD + ".?/?", 10101)))));

        checkEqualDocuments(expected, mongoQuery);
    }

    @Test
    public void testQueryNegatedGenotypes() {
        Document mongoQuery = parser.parseQuery(new Query().append(STUDY.key(), "study_1")
                .append(GENOTYPE.key(), "sample_10101" + IS + NOT + "1/1"));

        Document expected = new Document(STUDIES_FIELD + '.' + STUDYID_FIELD, 1)
                .append("$and", Collections.singletonList(ANY));

        checkEqualDocuments(expected, mongoQuery);
    }

    @Test
    public void testProjectionIncludeSamplesExcludeFiles() {
        Document expected = new Document()
                .append(CHROMOSOME_FIELD, 1)
                .append(START_FIELD, 1)
                .append(END_FIELD, 1)
//                .append(LENGTH_FIELD, 1)
                .append(REFERENCE_FIELD, 1)
                .append(ALTERNATE_FIELD, 1)
//                .append(IDS_FIELD, 1)
                .append(TYPE_FIELD, 1)
                .append(SV_FIELD, 1)
//                .append(HGVS_FIELD, 1)
//                .append(ANNOTATION_FIELD, 1)
//                .append(CUSTOM_ANNOTATION_FIELD, 1)
//                .append(RELEASE_FIELD, 1)
                .append(STATS_FIELD, 1)
                .append(STUDIES_FIELD + '.' + STUDYID_FIELD, 1)
                .append(STUDIES_FIELD + '.' + ALTERNATES_FIELD, 1)
                .append(STUDIES_FIELD + '.' + GENOTYPES_FIELD, 1)
                .append(STUDIES_FIELD + '.' + FILES_FIELD + '.' + FILEID_FIELD, 1) // Ensure that fileId is always returned
                .append(STUDIES_FIELD + '.' + FILES_FIELD + '.' + SAMPLE_DATA_FIELD, 1);

        Document projection = parser.createProjection(new Query(), new QueryOptions(QueryOptions.INCLUDE,
                STUDIES_SAMPLES_DATA + "," + STUDIES_SECONDARY_ALTERNATES + ',' + STUDIES_STATS));
        checkEqualDocuments(expected, projection);

        projection = parser.createProjection(new Query()
                .append(INCLUDE_GENOTYPE.key(), true)
                .append(INCLUDE_FORMAT.key(), ALL)
                .append(INCLUDE_FILE.key(), NONE), new QueryOptions(QueryOptions.INCLUDE, STUDIES.fieldName()));
        checkEqualDocuments(expected, projection);

        expected.append(LENGTH_FIELD, 1);
        expected.append(HGVS_FIELD, 1);
        expected.append(IDS_FIELD, 1);
        projection = parser.createProjection(new Query().append(INCLUDE_SAMPLE.key(), ALL)
                .append(INCLUDE_FILE.key(), NONE), new QueryOptions(QueryOptions.EXCLUDE, ANNOTATION.fieldName()));
        checkEqualDocuments(expected, projection);

        expected.remove(STUDIES_FIELD + '.' + FILES_FIELD + '.' + SAMPLE_DATA_FIELD);
        projection = parser.createProjection(new Query().append(INCLUDE_SAMPLE.key(), ALL)
                .append(INCLUDE_FILE.key(), NONE).append(INCLUDE_GENOTYPE.key(), true), new QueryOptions(QueryOptions.EXCLUDE, ANNOTATION.fieldName()));
        checkEqualDocuments(expected, projection);

    }

}