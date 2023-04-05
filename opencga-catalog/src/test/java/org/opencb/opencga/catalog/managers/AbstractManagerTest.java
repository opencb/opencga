/*
 * Copyright 2015-2020 OpenCB
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

package org.opencb.opencga.catalog.managers;

import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.opencb.commons.datastore.core.DataResult;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.test.GenericTest;
import org.opencb.opencga.TestParamConstants;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.models.common.AnnotationSet;
import org.opencb.opencga.core.models.file.File;
import org.opencb.opencga.core.models.file.FileCreateParams;
import org.opencb.opencga.core.models.file.FileUpdateParams;
import org.opencb.opencga.core.models.sample.Sample;
import org.opencb.opencga.core.models.study.Study;
import org.opencb.opencga.core.models.study.Variable;
import org.opencb.opencga.core.models.study.VariableSet;
import org.opencb.opencga.core.models.user.Account;
import org.opencb.opencga.core.testclassification.duration.MediumTests;

import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

@Category(MediumTests.class)
public class AbstractManagerTest extends GenericTest {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Rule
    public CatalogManagerExternalResource catalogManagerResource = new CatalogManagerExternalResource();

    protected CatalogManager catalogManager;
    protected String opencgaToken;
    protected String token;
    protected String sessionIdUser2;
    protected String sessionIdUser3;
    protected File testFolder;
    protected String project1;
    protected String project2;
    protected long studyUid;
    protected String studyFqn;
    protected long studyUid2;
    protected String studyFqn2;
    protected String studyFqn3;
    protected String s_1;
    protected String s_2;
    protected String s_3;
    protected String s_4;
    protected String s_5;
    protected String s_6;
    protected String s_7;
    protected String s_8;
    protected String s_9;
    protected String testFile1;
    protected String testFile2;

    protected static final QueryOptions INCLUDE_RESULT = new QueryOptions(ParamConstants.INCLUDE_RESULT_PARAM, true);

    @Before
    public void setUp() throws Exception {
        catalogManager = catalogManagerResource.getCatalogManager();
        setUpCatalogManager(catalogManager);
    }

    public void setUpCatalogManager(CatalogManager catalogManager) throws IOException, CatalogException {
        opencgaToken = catalogManager.getUserManager().loginAsAdmin(TestParamConstants.ADMIN_PASSWORD).getToken();

        catalogManager.getUserManager().create("user", "User Name", "mail@ebi.ac.uk", TestParamConstants.PASSWORD, "", null, Account.AccountType.FULL, opencgaToken);
        catalogManager.getUserManager().create("user2", "User2 Name", "mail2@ebi.ac.uk", TestParamConstants.PASSWORD, "", null, Account.AccountType.FULL, opencgaToken);
        catalogManager.getUserManager().create("user3", "User3 Name", "user.2@e.mail", TestParamConstants.PASSWORD, "ACME", null, Account.AccountType.FULL, opencgaToken);

        token = catalogManager.getUserManager().login("user", TestParamConstants.PASSWORD).getToken();
        sessionIdUser2 = catalogManager.getUserManager().login("user2", TestParamConstants.PASSWORD).getToken();
        sessionIdUser3 = catalogManager.getUserManager().login("user3", TestParamConstants.PASSWORD).getToken();

        project1 = catalogManager.getProjectManager().create("1000G", "Project about some genomes", "", "Homo sapiens",
                null, "GRCh38", INCLUDE_RESULT, token).first().getId();
        project2 = catalogManager.getProjectManager().create("pmp", "Project Management Project", "life art intelligent system",
                "Homo sapiens", null, "GRCh38", INCLUDE_RESULT, sessionIdUser2).first().getId();
        catalogManager.getProjectManager().create("p1", "project 1", "", "Homo sapiens", null, "GRCh38", INCLUDE_RESULT,
                sessionIdUser3).first();

        Study study = catalogManager.getStudyManager().create(project1, "phase1", null, "Phase 1", "Done", null, null, null, null, INCLUDE_RESULT, token).first();
        studyUid = study.getUid();
        studyFqn = study.getFqn();

        study = catalogManager.getStudyManager().create(project1, "phase3", null, "Phase 3", "d", null, null, null, null, INCLUDE_RESULT, token).first();
        studyUid2 = study.getUid();
        studyFqn2 = study.getFqn();

        study = catalogManager.getStudyManager().create(project2, "s1", null, "Study 1", "", null, null, null, null, INCLUDE_RESULT, sessionIdUser2).first();
        studyFqn3 = study.getFqn();

        catalogManager.getFileManager().createFolder(studyFqn2, Paths.get("data/test/folder/").toString(), true,
                null, QueryOptions.empty(), token);

        testFolder = catalogManager.getFileManager().createFolder(studyFqn, Paths.get("data/test/folder/").toString(),
                true, null, INCLUDE_RESULT, token).first();
        ObjectMap attributes = new ObjectMap();
        attributes.put("field", "value");
        attributes.put("numValue", 5);
        catalogManager.getFileManager().update(studyFqn, testFolder.getPath(),
                new FileUpdateParams().setAttributes(attributes), new QueryOptions(), token);

        testFile1 = testFolder.getPath() + "test_1K.txt.gz";
        DataResult<File> queryResult2 = catalogManager.getFileManager().create(studyFqn,
                new FileCreateParams()
                        .setContent(RandomStringUtils.randomAlphanumeric(1000))
                        .setPath(testFile1)
                        .setType(File.Type.FILE),
                false, token);

        File fileTest1k = catalogManager.getFileManager().get(studyFqn, queryResult2.first().getPath(), INCLUDE_RESULT, token).first();
        attributes = new ObjectMap();
        attributes.put("field", "value");
        attributes.put("name", "fileTest1k");
        attributes.put("numValue", "10");
        attributes.put("boolean", false);
        catalogManager.getFileManager().update(studyFqn, fileTest1k.getPath(),
                new FileUpdateParams().setAttributes(attributes), new QueryOptions(), token);

        testFile2 = testFolder.getPath() + "test_0.5K.txt";
        DataResult<File> queryResult1 = catalogManager.getFileManager().create(studyFqn,
                new FileCreateParams()
                        .setContent(RandomStringUtils.randomAlphanumeric(500))
                        .setPath(testFile2)
                        .setBioformat(File.Bioformat.DATAMATRIX_EXPRESSION)
                        .setType(File.Type.FILE),
                false, token);

        File fileTest05k = catalogManager.getFileManager().get(studyFqn, queryResult1.first().getPath(), INCLUDE_RESULT, token).first();
        attributes = new ObjectMap();
        attributes.put("field", "valuable");
        attributes.put("name", "fileTest05k");
        attributes.put("numValue", 5);
        attributes.put("boolean", true);
        catalogManager.getFileManager().update(studyFqn, fileTest05k.getPath(),
                new FileUpdateParams().setAttributes(attributes), new QueryOptions(), token);

        DataResult<File> queryResult = catalogManager.getFileManager().create(studyFqn,
                new FileCreateParams()
                        .setContent("iVBORw0KGgoAAAANSUhEUgAAABgAAAAYCAYAAADgdz34AAAABHNCSVQICAgIfAhkiAAAAAlwSFlzAAAApgAAAKYB3X3/OAAAABl0RVh0U29mdHdhcmUAd3d3Lmlua3NjYXBlLm9yZ5vuPBoAAANCSURBVEiJtZZPbBtFFMZ/M7ubXdtdb1xSFyeilBapySVU8h8OoFaooFSqiihIVIpQBKci6KEg9Q6H9kovIHoCIVQJJCKE1ENFjnAgcaSGC6rEnxBwA04Tx43t2FnvDAfjkNibxgHxnWb2e/u992bee7tCa00YFsffekFY+nUzFtjW0LrvjRXrCDIAaPLlW0nHL0SsZtVoaF98mLrx3pdhOqLtYPHChahZcYYO7KvPFxvRl5XPp1sN3adWiD1ZAqD6XYK1b/dvE5IWryTt2udLFedwc1+9kLp+vbbpoDh+6TklxBeAi9TL0taeWpdmZzQDry0AcO+jQ12RyohqqoYoo8RDwJrU+qXkjWtfi8Xxt58BdQuwQs9qC/afLwCw8tnQbqYAPsgxE1S6F3EAIXux2oQFKm0ihMsOF71dHYx+f3NND68ghCu1YIoePPQN1pGRABkJ6Bus96CutRZMydTl+TvuiRW1m3n0eDl0vRPcEysqdXn+jsQPsrHMquGeXEaY4Yk4wxWcY5V/9scqOMOVUFthatyTy8QyqwZ+kDURKoMWxNKr2EeqVKcTNOajqKoBgOE28U4tdQl5p5bwCw7BWquaZSzAPlwjlithJtp3pTImSqQRrb2Z8PHGigD4RZuNX6JYj6wj7O4TFLbCO/Mn/m8R+h6rYSUb3ekokRY6f/YukArN979jcW+V/S8g0eT/N3VN3kTqWbQ428m9/8k0P/1aIhF36PccEl6EhOcAUCrXKZXXWS3XKd2vc/TRBG9O5ELC17MmWubD2nKhUKZa26Ba2+D3P+4/MNCFwg59oWVeYhkzgN/JDR8deKBoD7Y+ljEjGZ0sosXVTvbc6RHirr2reNy1OXd6pJsQ+gqjk8VWFYmHrwBzW/n+uMPFiRwHB2I7ih8ciHFxIkd/3Omk5tCDV1t+2nNu5sxxpDFNx+huNhVT3/zMDz8usXC3ddaHBj1GHj/As08fwTS7Kt1HBTmyN29vdwAw+/wbwLVOJ3uAD1wi/dUH7Qei66PfyuRj4Ik9is+hglfbkbfR3cnZm7chlUWLdwmprtCohX4HUtlOcQjLYCu+fzGJH2QRKvP3UNz8bWk1qMxjGTOMThZ3kvgLI5AzFfo379UAAAAASUVORK5CYII=")
                        .setPath(testFolder.getPath() + "test_0.1K.png")
                        .setFormat(File.Format.IMAGE)
                        .setType(File.Type.FILE),
                false, token);

        File test01k = catalogManager.getFileManager().get(studyFqn, queryResult.first().getPath(), INCLUDE_RESULT, token).first();
        attributes = new ObjectMap();
        attributes.put("field", "other");
        attributes.put("name", "test01k");
        attributes.put("numValue", 50);
        attributes.put("nested", new ObjectMap("num1", 45).append("num2", 33).append("text", "HelloWorld"));
        catalogManager.getFileManager().update(studyFqn, test01k.getPath(),
                new FileUpdateParams().setAttributes(attributes), new QueryOptions(), token);

        List<Variable> variables = new ArrayList<>();
        variables.addAll(Arrays.asList(
                new Variable("NAME", "", "", Variable.VariableType.STRING, "", true, false, Collections.<String>emptyList(), null, 0, "", "", null,
                        Collections.<String, Object>emptyMap()),
                new Variable("AGE", "", "", Variable.VariableType.INTEGER, null, true, false, Collections.singletonList("0:130"), null, 1, "", "",
                        null, Collections.<String, Object>emptyMap()),
                new Variable("HEIGHT", "", "", Variable.VariableType.DOUBLE, "1.5", false, false, Collections.singletonList("0:"), null, 2, "",
                        "", null, Collections.<String, Object>emptyMap()),
                new Variable("ALIVE", "", "", Variable.VariableType.BOOLEAN, "", true, false, Collections.<String>emptyList(), null, 3, "", "",
                        null, Collections.<String, Object>emptyMap()),
                new Variable("PHEN", "", "", Variable.VariableType.CATEGORICAL, "CASE", true, false, Arrays.asList("CASE", "CONTROL"), null, 4,
                        "", "", null, Collections.<String, Object>emptyMap()),
                new Variable("EXTRA", "", "", Variable.VariableType.STRING, "", false, false, Collections.emptyList(), null, 5, "", "", null,
                        Collections.<String, Object>emptyMap())
        ));
        VariableSet vs = catalogManager.getStudyManager().createVariableSet(studyFqn, "vs", "vs", true, false, "", null, variables,
                null, token).first();

        Sample sample = new Sample().setId("s_1");
        sample.setAnnotationSets(Collections.singletonList(new AnnotationSet("annot1", vs.getId(),
                new ObjectMap("NAME", "s_1").append("AGE", 6).append("ALIVE", true).append("PHEN", "CONTROL"))));
        s_1 = catalogManager.getSampleManager().create(studyFqn, sample, INCLUDE_RESULT, token).first().getId();

        sample.setId("s_2");
        sample.setAnnotationSets(Collections.singletonList(new AnnotationSet("annot1", vs.getId(),
                new ObjectMap("NAME", "s_2").append("AGE", 10).append("ALIVE", false).append("PHEN", "CASE"))));
        s_2 = catalogManager.getSampleManager().create(studyFqn, sample, INCLUDE_RESULT, token).first().getId();

        sample.setId("s_3");
        sample.setAnnotationSets(Collections.singletonList(new AnnotationSet("annot1", vs.getId(),
                new ObjectMap("NAME", "s_3").append("AGE", 15).append("ALIVE", true).append("PHEN", "CONTROL"))));
        s_3 = catalogManager.getSampleManager().create(studyFqn, sample, INCLUDE_RESULT, token).first().getId();

        sample.setId("s_4");
        sample.setAnnotationSets(Collections.singletonList(new AnnotationSet("annot1", vs.getId(),
                new ObjectMap("NAME", "s_4").append("AGE", 22).append("ALIVE", false).append("PHEN", "CONTROL"))));
        s_4 = catalogManager.getSampleManager().create(studyFqn, sample, INCLUDE_RESULT, token).first().getId();

        sample.setId("s_5");
        sample.setAnnotationSets(Collections.singletonList(new AnnotationSet("annot1", vs.getId(),
                new ObjectMap("NAME", "s_5").append("AGE", 29).append("ALIVE", true).append("PHEN", "CASE"))));
        s_5 = catalogManager.getSampleManager().create(studyFqn, sample, INCLUDE_RESULT, token).first().getId();

        sample.setId("s_6");
        sample.setAnnotationSets(Collections.singletonList(new AnnotationSet("annot2", vs.getId(),
                new ObjectMap("NAME", "s_6").append("AGE", 38).append("ALIVE", true).append("PHEN", "CONTROL"))));
        s_6 = catalogManager.getSampleManager().create(studyFqn, sample, INCLUDE_RESULT, token).first().getId();

        sample.setId("s_7");
        sample.setAnnotationSets(Collections.singletonList(new AnnotationSet("annot2", vs.getId(),
                new ObjectMap("NAME", "s_7").append("AGE", 46).append("ALIVE", false).append("PHEN", "CASE"))));
        s_7 = catalogManager.getSampleManager().create(studyFqn, sample, INCLUDE_RESULT, token).first().getId();

        sample.setId("s_8");
        sample.setAnnotationSets(Collections.singletonList(new AnnotationSet("annot2", vs.getId(),
                new ObjectMap("NAME", "s_8").append("AGE", 72).append("ALIVE", true).append("PHEN", "CONTROL"))));
        s_8 = catalogManager.getSampleManager().create(studyFqn, sample, INCLUDE_RESULT, token).first().getId();

        sample.setId("s_9");
        sample.setAnnotationSets(Collections.emptyList());
        s_9 = catalogManager.getSampleManager().create(studyFqn, sample, INCLUDE_RESULT, token).first().getId();

        catalogManager.getFileManager().update(studyFqn, test01k.getPath(), new FileUpdateParams()
                .setSampleIds(Arrays.asList(s_1, s_2, s_3, s_4, s_5)), INCLUDE_RESULT, token);
    }


    /* TYPE_FILE UTILS */
    public static java.io.File createDebugFile() throws IOException {
        String fileTestName = "/tmp/fileTest_" + RandomStringUtils.randomAlphanumeric(5);
        return createDebugFile(fileTestName);
    }

    public static java.io.File createDebugFile(String fileTestName) throws IOException {
        return createDebugFile(fileTestName, 200);
    }

    public static java.io.File createDebugFile(String fileTestName, int lines) throws IOException {
        DataOutputStream os = new DataOutputStream(new FileOutputStream(fileTestName));

        os.writeBytes("Debug file name: " + fileTestName + "\n");
        for (int i = 0; i < 100; i++) {
            os.writeBytes(i + ", ");
        }
        for (int i = 0; i < lines; i++) {
            os.writeBytes(RandomStringUtils.randomAlphanumeric(500));
            os.write('\n');
        }
        os.close();

        return Paths.get(fileTestName).toFile();
    }

    public static String createRandomString(int lines) {
        StringBuilder stringBuilder = new StringBuilder(lines);
        for (int i = 0; i < 100; i++) {
            stringBuilder.append(i + ", ");
        }
        for (int i = 0; i < lines; i++) {
            stringBuilder.append(RandomStringUtils.randomAlphanumeric(500));
            stringBuilder.append("\n");
        }
        return stringBuilder.toString();
    }

    public static String getDummyVCFContent() {
        return "##fileformat=VCFv4.0\n" +
                "##fileDate=20090805\n" +
                "##source=myImputationProgramV3.1\n" +
                "##reference=1000GenomesPilot-NCBI36\n" +
                "##phasing=partial\n" +
                "#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT\tNA00001\tNA00002\tNA00003\n" +
                "20\t14370\trs6054257\tG\tA\t29\tPASS\tNS=3;DP=14;AF=0.5;DB;H2\tGT:GQ:DP:HQ\t0|0:48:1:51,51\t1|0:48:8:51,51\t1/1:43:5:.,.\n" +
                "20\t17330\t.\tT\tA\t3\tq10\tNS=3;DP=11;AF=0.017\tGT:GQ:DP:HQ\t0|0:49:3:58,50\t0|1:3:5:65,3\t0/0:41:3\n" +
                "20\t1110696\trs6040355\tA\tG,T\t67\tPASS\tNS=2;DP=10;AF=0.333,0.667;AA=T;DB\tGT:GQ:DP:HQ\t1|2:21:6:23,27\t2|1:2:0:18,2\t2/2:35:4\n" +
                "20\t1230237\t.\tT\t.\t47\tPASS\tNS=3;DP=13;AA=T\tGT:GQ:DP:HQ\t0|0:54:7:56,60\t0|0:48:4:51,51\t0/0:61:2\n" +
                "20\t1234567\tmicrosat1\tGTCT\tG,GTACT\t50\tPASS\tNS=3;DP=9;AA=G\tGT:GQ:DP\t0/1:35:4\t0/2:17:2\t1/1:40:3";
    }

}
