package org.opencb.opencga.catalog.managers;

import org.apache.commons.lang3.RandomStringUtils;
import org.opencb.biodata.models.clinical.Disorder;
import org.opencb.biodata.models.clinical.Phenotype;
import org.opencb.biodata.models.core.SexOntologyTermAnnotation;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.core.models.clinical.ClinicalAnalysis;
import org.opencb.opencga.core.models.clinical.Interpretation;
import org.opencb.opencga.core.models.family.Family;
import org.opencb.opencga.core.models.individual.Individual;
import org.opencb.opencga.core.models.panel.Panel;
import org.opencb.opencga.core.models.sample.Sample;

import java.util.*;

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

    public static Individual getDummyIndividual(String id, List<Sample> sampleList, List<Disorder> disorderList, List<Phenotype> phenotypeList) {
        return new Individual()
                .setId(id)
                .setSamples(sampleList)
                .setDisorders(disorderList)
                .setPhenotypes(phenotypeList);
    }

    public static Family getCompleteFamily(String id) {
        Sample fSample = getDummySample("s_father_" + id);
        Sample mSample = getDummySample("s_mother_" + id);
        Sample bSample = getDummySample("s_boy_" + id);
        Sample gSample = getDummySample("s_girl_" + id);

        Disorder disease1 = getDummyDisorder();
        Disorder disease2 = getDummyDisorder();

        Individual father = getDummyIndividual("father_" + id, Collections.singletonList(fSample), Collections.singletonList(disease2), null)
                .setSex(new SexOntologyTermAnnotation().setId("MALE"));
        Individual mother = getDummyIndividual("mother_" + id, Collections.singletonList(mSample), Collections.singletonList(disease1), null)
                .setSex(new SexOntologyTermAnnotation().setId("FEMALE"));;
        Individual boy = getDummyIndividual("boy_" + id, Collections.singletonList(bSample), Collections.singletonList(disease1), null)
                .setFather(father)
                .setMother(mother)
                .setSex(new SexOntologyTermAnnotation().setId("MALE"));;
        Individual girl = getDummyIndividual("girl_" + id, Collections.singletonList(gSample), Collections.singletonList(disease2), null)
                .setFather(father)
                .setMother(mother)
                .setSex(new SexOntologyTermAnnotation().setId("FEMALE"));;

        return getDummyFamily("family_" + id, Arrays.asList(father, mother, boy, girl));
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

    public static void createFullFamily(CatalogManager catalogManager, String study, Family family, String token) throws CatalogException {
        Set<String> createdIndividuals = new HashSet<>();
        while (createdIndividuals.size() < family.getMembers().size()) {
            for (Individual member : family.getMembers()) {
                if ((member.getFather() == null || createdIndividuals.contains(member.getFather().getId()))
                        && (member.getMother() == null || createdIndividuals.contains(member.getMother().getId()))) {
                    catalogManager.getIndividualManager().create(study, member, QueryOptions.empty(), token);
                    createdIndividuals.add(member.getId());
                }
            }
        }

        Family tmpFamily = new Family().setId(family.getId());
        catalogManager.getFamilyManager().create(study, tmpFamily, new ArrayList<>(createdIndividuals), QueryOptions.empty(), token);
    }

    public static ClinicalAnalysis getDummyClinicalAnalysis(Individual proband, Family family, List<Panel> panelList) {
        return getDummyClinicalAnalysis(RandomStringUtils.randomAlphabetic(10), proband, family, panelList);
    }

    public static ClinicalAnalysis getDummyClinicalAnalysis(String id, Individual proband, Family family, List<Panel> panelList) {
        ClinicalAnalysis clinicalAnalysis = new ClinicalAnalysis()
                .setId(id)
                .setProband(proband)
                .setFamily(family)
                .setPanels(panelList);
        if (family != null) {
            clinicalAnalysis.setType(ClinicalAnalysis.Type.FAMILY);
        } else {
            clinicalAnalysis.setType(ClinicalAnalysis.Type.SINGLE);
        }
        return clinicalAnalysis;
    }

    public static Interpretation getDummyInterpretation(List<Panel> panelList) {
        return new Interpretation()
                .setPanels(panelList);
    }

}
