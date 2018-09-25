package org.opencb.opencga.core.models;

import org.opencb.biodata.models.variant.avro.*;
import org.opencb.commons.utils.ListUtils;
import org.opencb.opencga.core.models.clinical.ReportedEvent;
import org.opencb.opencga.core.models.clinical.ReportedVariant;

import java.util.*;

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
}
