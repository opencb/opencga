package org.opencb.opencga.core.models;

import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.models.core.pedigree.Pedigree;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ClinicalProperty {

    public enum ModeOfInheritance {
        MONOALLELIC,
        MONOALLELIC_NOT_IMPRINTED,
        MONOALLELIC_MATERNALLY_IMPRINTED,
        MONOALLELIC_PATERNALLY_IMPRINTED,
        BIALLELIC,
        MONOALLELIC_AND_BIALLELIC,
        MONOALLELIC_AND_MORE_SEVERE_BIALLELIC,
        XLINKED_BIALLELIC,
        XLINKED_MONOALLELIC,
        YLINKED,
        MITOCHRONDRIAL,

        // Not modes of inheritance, but...
        DE_NOVO,
        COMPOUND_HETEROZYGOUS,

        UNKNOWN
    }

    public enum Penetrance {
        COMPLETE,
        INCOMPLETE
    }

    public enum RoleInCancer {
        ONCOGENE,
        TUMOR_SUPPRESSOR_GENE,
        BOTH
    }


    public static Pedigree getPedigreeFromFamily(Family family) {
        List<org.opencb.biodata.models.core.pedigree.Individual> individuals = parseMembersToBiodataIndividuals(family.getMembers());
        return new Pedigree(family.getId(), individuals, family.getPhenotypes(), family.getAttributes());
    }

    private static List<org.opencb.biodata.models.core.pedigree.Individual> parseMembersToBiodataIndividuals(List<Individual> members) {
        Map<String, org.opencb.biodata.models.core.pedigree.Individual> individualMap = new HashMap();

        // Parse all the individuals
        for (Individual member : members) {
            org.opencb.biodata.models.core.pedigree.Individual individual =
                    new org.opencb.biodata.models.core.pedigree.Individual(member.getId(), member.getName(), null, null,
                            member.getMultiples(),
                            org.opencb.biodata.models.core.pedigree.Individual.Sex.getEnum(member.getSex().toString()),
                            member.getLifeStatus(),
                            org.opencb.biodata.models.core.pedigree.Individual.AffectionStatus.getEnum(member.getAffectationStatus()
                                    .toString()), member.getPhenotypes(), member.getAttributes());
            individualMap.put(individual.getId(), individual);
        }

        // Fill parent information
        for (Individual member : members) {
            if (member.getFather() != null && StringUtils.isNotEmpty(member.getFather().getId())) {
                individualMap.get(member.getId()).setFather(individualMap.get(member.getFather().getId()));
            }
            if (member.getMother() != null && StringUtils.isNotEmpty(member.getMother().getId())) {
                individualMap.get(member.getId()).setMother(individualMap.get(member.getMother().getId()));
            }
        }

        return new ArrayList<>(individualMap.values());
    }

}
