package org.opencb.opencga.catalog.managers;

import org.apache.commons.lang3.RandomStringUtils;
import org.opencb.biodata.models.clinical.Disorder;
import org.opencb.biodata.models.clinical.Phenotype;
import org.opencb.opencga.core.models.clinical.ClinicalAnalysis;
import org.opencb.opencga.core.models.clinical.Interpretation;
import org.opencb.opencga.core.models.family.Family;
import org.opencb.opencga.core.models.individual.Individual;
import org.opencb.opencga.core.models.panel.Panel;
import org.opencb.opencga.core.models.sample.Sample;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class DummyModelUtils {

    public static Disorder getDummyDisorder() {
        return getDummyDisorder(RandomStringUtils.randomAlphabetic(10));
    }

    public static Disorder getDummyDisorder(String id) {
        return new Disorder(id, id, "HPO", null, "", null);
    }

    public static Panel getDummyPanel(String id) {
        return new Panel().setId(id);
    }

    public static Sample getDummySample() {
        return getDummySample(RandomStringUtils.randomAlphabetic(10));
    }

    public static Sample getDummySample(String id) {
        return new Sample().setId(id);
    }

    public static Individual getDummyIndividual(List<Sample> sampleList, List<Disorder> disorderList, List<Phenotype> phenotypeList) {
        return getDummyIndividual(RandomStringUtils.randomAlphabetic(10), sampleList, disorderList, phenotypeList);
    }

    public static Individual getDummyIndividual(String id, List<Sample> sampleList, List<Disorder> disorderList,
                                                List<Phenotype> phenotypeList) {
        return new Individual()
                .setId(id)
                .setSamples(sampleList)
                .setDisorders(disorderList)
                .setPhenotypes(phenotypeList);
    }

    public static Family getDummyFamily() {
        return getDummyFamily(RandomStringUtils.randomAlphabetic(10));
    }

    public static Family getDummyFamily(String id, List<Individual> members) {
        return new Family()
                .setId(id)
                .setMembers(members);
    }

    public static Family getDummyCaseFamily(String id) {
        Sample sample1 = getDummySample();
        Sample sample2 = getDummySample();
        Sample sample3 = getDummySample();
        Sample sample4 = getDummySample();
        Sample sample5 = getDummySample();

        Disorder disease1 = getDummyDisorder();
        Disorder disease2 = getDummyDisorder();

        Individual father = getDummyIndividual("father", Collections.singletonList(sample1), Collections.singletonList(disease1), null);
        Individual mother = getDummyIndividual("mother", Collections.singletonList(sample3), Collections.singletonList(disease2), null);
        Individual child1 = getDummyIndividual("child1", Collections.singletonList(sample2), Arrays.asList(disease1, disease2), null)
                .setFather(father)
                .setMother(mother);
        Individual child2 = getDummyIndividual("child2", Collections.singletonList(sample4), Arrays.asList(disease1, disease2), null)
                .setFather(father)
                .setMother(mother);
        Individual child3 = getDummyIndividual("child3", Collections.singletonList(sample5), Arrays.asList(disease1, disease2), null)
                .setFather(father)
                .setMother(mother);

        return new Family(id, id, null, null,
                Arrays.asList(child1, child2, child3, father, mother), "", -1,
                Collections.emptyList(), Collections.emptyMap());
    }

    public static Family getDummyFamily(String id) {
        Disorder disease1 = getDummyDisorder();
        Disorder disease2 = getDummyDisorder();

        Individual father = getDummyIndividual("father", null, Collections.singletonList(disease1), null);
        Individual mother = getDummyIndividual("mother", null, Collections.singletonList(disease2), null);

        // We create a new father and mother with the same information to mimic the behaviour of the webservices. Otherwise, we would be
        // ingesting references to exactly the same object and this test would not work exactly the same way.
        Individual relFather = new Individual().setId("father").setDisorders(Arrays.asList(disease1))
                .setSamples(Collections.singletonList(new Sample().setId("sample1")));
        Individual relMother = new Individual().setId("mother").setDisorders(Arrays.asList(disease2))
                .setSamples(Arrays.asList(new Sample().setId("sample3")));

        Individual relChild1 = new Individual().setId("child1")
                .setDisorders(Arrays.asList(disease1, disease2))
                .setFather(father)
                .setMother(mother)
                .setSamples(Arrays.asList(
                        new Sample().setId("sample2"),
                        new Sample().setId("sample4")
                ))
                .setParentalConsanguinity(true);
        Individual relChild2 = new Individual().setId("child2")
                .setDisorders(Arrays.asList(disease1))
                .setFather(father)
                .setMother(mother)
                .setSamples(Arrays.asList(
                        new Sample().setId("sample5"),
                        new Sample().setId("sample6")
                ))
                .setParentalConsanguinity(true);
        Individual relChild3 = new Individual().setId("child3")
                .setDisorders(Arrays.asList(disease1))
                .setFather(father)
                .setMother(mother)
                .setSamples(Arrays.asList(
                        new Sample().setId("sample7"),
                        new Sample().setId("sample8")
                ))
                .setParentalConsanguinity(true);

        return new Family(id, id, null, null,
                Arrays.asList(relChild1, relChild2, relChild3, relFather, relMother), "", -1,
                Collections.emptyList(), Collections.emptyMap());
    }

    public static ClinicalAnalysis getDummyClinicalAnalysis(Individual proband, Family family, List<Panel> panelList) {
        return getDummyClinicalAnalysis(RandomStringUtils.randomAlphabetic(10), proband, family, panelList);
    }

    public static ClinicalAnalysis getDummyClinicalAnalysis(String id, Individual proband, Family family, List<Panel> panelList) {
        return new ClinicalAnalysis()
                .setId(id)
                .setType(ClinicalAnalysis.Type.FAMILY)
                .setProband(proband)
                .setFamily(family)
                .setPanels(panelList);
    }

    public static Interpretation getDummyInterpretation(List<Panel> panelList) {
        return new Interpretation()
                .setPanels(panelList);
    }

}
