package org.opencb.opencga.core.models;

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

}
