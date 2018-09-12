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

package org.opencb.opencga.analysis.clinical;

import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.models.commons.Phenotype;
import org.opencb.biodata.models.core.pedigree.Individual;
import org.opencb.biodata.models.core.pedigree.Pedigree;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.tools.pedigree.ModeOfInheritance;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.analysis.AnalysisResult;
import org.opencb.opencga.analysis.OpenCgaAnalysis;
import org.opencb.opencga.analysis.exceptions.AnalysisException;
import org.opencb.opencga.core.models.ClinicalAnalysis;
import org.opencb.opencga.core.models.Family;
import org.opencb.opencga.core.models.clinical.Interpretation;
import org.opencb.opencga.core.models.clinical.ReportedEvent;
import org.opencb.opencga.core.models.clinical.ReportedVariant;

import java.util.*;

public class TieringAnalysis extends OpenCgaAnalysis<Interpretation> {

    private String clinicalAnalysisId;

    public TieringAnalysis(String opencgaHome, String studyStr, String token) {
        super(opencgaHome, studyStr, token);
    }

    public TieringAnalysis(String opencgaHome, String studyStr, String token, String clinicalAnalysisId, ObjectMap config) {
        super(opencgaHome, studyStr, token);

        this.clinicalAnalysisId = clinicalAnalysisId;
    }

    @Override
    public AnalysisResult<Interpretation> execute() throws Exception {
        // checks

        // set defaults

        QueryResult<ClinicalAnalysis> clinicalAnalysisQueryResult = catalogManager.getClinicalAnalysisManager().get(studyStr,
                clinicalAnalysisId, QueryOptions.empty(), token);
        if (clinicalAnalysisQueryResult.getNumResults() == 0) {
            throw new AnalysisException("Clinical analysis " + clinicalAnalysisId + " not found in study " + studyStr);
        }

        ClinicalAnalysis clinicalAnalysis = clinicalAnalysisQueryResult.first();

        if (clinicalAnalysis.getFamily() == null || StringUtils.isEmpty(clinicalAnalysis.getFamily().getId())) {
            throw new AnalysisException("Missing family in clinical analysis " + clinicalAnalysisId);
        }

        Pedigree pedigree = getPedigreeFromFamily(clinicalAnalysis.getFamily());
        List<Phenotype> phenotypes = clinicalAnalysis.getProband().getPhenotypes();

        for (Phenotype phenotype : phenotypes) {
            Map<String, List<String>> genotypes = ModeOfInheritance.dominant(pedigree, phenotype, false);
            genotypes = ModeOfInheritance.dominant(pedigree, phenotype, true);
            genotypes = ModeOfInheritance.recessive(pedigree, phenotype, false);
            genotypes = ModeOfInheritance.recessive(pedigree, phenotype, true);

            genotypes = ModeOfInheritance.xLinked(pedigree, phenotype, false);
            genotypes = ModeOfInheritance.xLinked(pedigree, phenotype, true);
            genotypes = ModeOfInheritance.yLinked(pedigree, phenotype);
        }



        // createInterpretation()

        // dominant() + recessive() ...

        // BAM coverage

        return null;
    }

    private Pedigree getPedigreeFromFamily(Family family) {
        List<Individual> individuals = parseMembersToBiodataIndividuals(family.getMembers());
        return new Pedigree(family.getId(), individuals, family.getPhenotypes(), family.getAttributes());
    }

    private List<Individual> parseMembersToBiodataIndividuals(List<org.opencb.opencga.core.models.Individual> members) {
        Map<String, Individual> individualMap = new HashMap();

        // Parse all the individuals
        for (org.opencb.opencga.core.models.Individual member : members) {
            Individual individual = new Individual(member.getId(), member.getName(), null, null, member.getMultiples(),
                    Individual.Sex.getEnum(member.getSex().toString()), member.getLifeStatus(),
                    Individual.AffectionStatus.getEnum(member.getAffectationStatus().toString()), member.getPhenotypes(),
                    member.getAttributes());
            individualMap.put(individual.getId(), individual);
        }

        // Fill parent information
        for (org.opencb.opencga.core.models.Individual member : members) {
            if (member.getFather() != null && StringUtils.isNotEmpty(member.getFather().getId())) {
                individualMap.get(member.getId()).setFather(individualMap.get(member.getFather().getId()));
            }
            if (member.getMother() != null && StringUtils.isNotEmpty(member.getMother().getId())) {
                individualMap.get(member.getId()).setMother(individualMap.get(member.getMother().getId()));
            }
        }

        return new ArrayList<>(individualMap.values());
    }

    private List<ReportedVariant> dominant() {
        return null;
    }

    private List<ReportedVariant> recessive() {
        // MoI -> genotypes
        // Variant Query query -> (biotype, gene, genoptype)
        // Iterator for (Var) -> getReportedEvents(rv)
        // create RV
        return null;
    }


    private List<ReportedEvent> getReportedEvents(Variant variant) {
        return null;
    }

    private Interpretation createInterpretation() {
        return null;
    }
}
