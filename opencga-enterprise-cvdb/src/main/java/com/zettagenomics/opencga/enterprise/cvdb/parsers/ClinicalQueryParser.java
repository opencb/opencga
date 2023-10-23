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

package com.zettagenomics.opencga.enterprise.cvdb.parsers;

import com.zettagenomics.opencga.enterprise.cvdb.exceptions.CvdbException;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.solr.client.solrj.SolrQuery;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.variant.search.solr.SolrQueryParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static org.opencb.commons.datastore.core.QueryParam.Type.TEXT_ARRAY;

public class ClinicalQueryParser {

    SolrQueryParser solrParser;
    protected static Logger logger = LoggerFactory.getLogger(ClinicalQueryParser.class);

    protected ClinicalQueryParser(VariantStorageMetadataManager variantStorageMetadataManager) {
        solrParser = new SolrQueryParser(variantStorageMetadataManager);
    }

    public SolrQuery parse(Query query, QueryOptions queryOptions) throws CvdbException {
        return solrParser.parse(query, queryOptions);
    }

    public List<String> clinicalAnalysisFilters(Query query) {
        List<String> filters = new ArrayList<>();

        // <field name="id" type="string" indexed="true" stored="true" required="true" multiValued="false" />
        addStringFilters("id", query.getString(ClinicalQueryParam.CA_ID_NAME), filters);

        // <field name="description" type="text_en" indexed="true" stored="true" multiValued="false"/>

        // <field name="type" type="text_en" indexed="true" stored="true" multiValued="false"/>
        addStringFilters("type", query.getString(ClinicalQueryParam.CA_TYPE_NAME), filters);

        // <field name="disorderId" type="string" indexed="true" stored="true" multiValued="false"/>
        addStringFilters("disorderId", query.getString(ClinicalQueryParam.CA_DISORDER_ID_NAME), filters);

        // <field name="fileNames" type="string" indexed="true" stored="true" multiValued="true"/>
        addStringFilters("fileNames", query.getString(ClinicalQueryParam.CA_FILENAME_NAME), filters);

        // <field name="probandId" type="string" indexed="true" stored="true" multiValued="false"/>
        addStringFilters("probandId", query.getString(ClinicalQueryParam.CA_PROBAND_ID_NAME), filters);

        // <field name="familyId" type="string" indexed="true" stored="true" multiValued="false"/>
        addStringFilters("familyId", query.getString(ClinicalQueryParam.CA_FAMILY_ID_NAME), filters);

        // <field name="familyPhenotypeNames" type="string" indexed="true" stored="true" multiValued="true"/>
        addStringFilters("familyPhenotypeNames", query.getString(ClinicalQueryParam.CA_FAMILY_PHENOTYPE_NAME_NAME), filters);

        // <field name="familyMemberIds" type="string" indexed="true" stored="true" multiValued="true"/>
        addStringFilters("familyMemberIds", query.getString(ClinicalQueryParam.CA_FAMILY_MEMBER_ID_NAME), filters);

        // <field name="report" type="string" indexed="true" stored="true" multiValued="false"/>

        // <field name="status" type="string" indexed="true" stored="true" multiValued="false"/>

        // <field name="locked" type="boolean" indexed="true" stored="true" multiValued="false"/>

        return filters;
    }

    public List<String> clinicalInterpretationFilters(Query query) {
        List<String> filters = new ArrayList<>();

        addStringFilters("id", query.getString(ClinicalQueryParam.CI_ID_NAME), filters);

        // <field name="primary" type="boolean" indexed="true" stored="true" multiValued="false"/>
        // <field name="description" type="string" indexed="true" stored="true" multiValued="false"/>

        // <!-- Panel IDs contain both IDs and names -->
        // <field name="panelIds" type="string" indexed="true" stored="true" multiValued="true"/>
        addStringFilters("panelIds", query.getString(ClinicalQueryParam.CI_PANEL_ID_NAME), filters);

        // <field name="analystId" type="string" indexed="true" stored="true" multiValued="false"/>
        addStringFilters("analystId", query.getString(ClinicalQueryParam.CI_ANALYIST_ID_NAME), filters);

        // <field name="analystName" type="string" indexed="true" stored="true" multiValued="false"/>
        addStringFilters("analystName", query.getString(ClinicalQueryParam.CI_ANALYIST_NAME_NAME), filters);

        // <field name="analystEmail" type="string" indexed="true" stored="true" multiValued="false"/>
        addStringFilters("analystEmail", query.getString(ClinicalQueryParam.CI_ANALYIST_EMAIL_NAME), filters);

        // <field name="analystAssignedBy" type="string" indexed="true" stored="true" multiValued="false"/>
        addStringFilters("analystAssignedBy", query.getString(ClinicalQueryParam.CI_ANALYIST_ASSIGNED_BY_NAME), filters);

        // <field name="analystDate" type="string" indexed="true" stored="true" multiValued="false"/>

        // <field name="methodName" type="string" indexed="true" stored="true" multiValued="false"/>
        addStringFilters("methodName", query.getString(ClinicalQueryParam.CI_METHOD_NAME_NAME), filters);

        // <field name="methodVersion" type="string" indexed="true" stored="true" multiValued="false"/>
        addStringFilters("methodVersion", query.getString(ClinicalQueryParam.CI_METHOD_VERSION_NAME), filters);

        // <field name="methodCommit" type="string" indexed="true" stored="true" multiValued="false"/>
        addStringFilters("methodCommit", query.getString(ClinicalQueryParam.CI_METHOD_COMMIT_NAME), filters);

        // <!-- Method software/dependencies are stores: name == version -->
        // <field name="methodDependencies" type="string" indexed="true" stored="true" multiValued="true"/>

        // <!-- Comments are stores: author == message == tag1:tag2:.. == date -->
        // <field name="comments" type="text_en" indexed="true" stored="true" multiValued="true"/>

        // <field name="locked" type="boolean" indexed="true" stored="true" multiValued="false"/>

        // <field name="statusId" type="string" indexed="true" stored="true" multiValued="false"/>
        addStringFilters("statusId", query.getString(ClinicalQueryParam.CI_STATUS_ID_NAME), filters);

        // <field name="statusName" type="string" indexed="true" stored="true" multiValued="false"/>
        addStringFilters("statusName", query.getString(ClinicalQueryParam.CI_STATUS_NAME_NAME), filters);

        // <field name="statusDescription" type="string" indexed="true" stored="true" multiValued="false"/>

        // <field name="statusDate" type="string" indexed="true" stored="true" multiValued="false"/>

        // <field name="creationDate" type="string" indexed="true" stored="true" multiValued="false"/>

        // <field name="modificationDate" type="string" indexed="true" stored="true" multiValued="false"/>

        // <field name="version" type="int" indexed="true" stored="true" multiValued="false"/>

        return filters;
    }

    public List<String> clinicalVariantFilters(Query query) {
        List<String> filters = new ArrayList<>();

//    <field name="primary" type="boolean" indexed="true" stored="true" multiValued="false"/>
//	<!-- Comments are stores: author == message == tag1:tag2:.. == date -->
//    <field name="comments" type="text_en" indexed="true" stored="true" multiValued="true"/>
//	<!-- Filters are stored in two dynamic fields: one for string values, the other one for numeric ones -->
//	<dynamicField name="annotations_*" type="string" indexed="false" stored="true" multiValued="false"/>
//    <dynamicField name="annotationScores_*" type="float" indexed="false" stored="true" multiValued="false"/>
//    <field name="discussionAuthor" type="string" indexed="true" stored="true" multiValued="false"/>
//    <field name="discussionDate" type="string" indexed="true" stored="true" multiValued="false"/>
//    <field name="discussionText" type="string" indexed="true" stored="true" multiValued="false"/>
//    <field name="confidenceValue" type="string" indexed="true" stored="true" multiValued="false"/>
//    <field name="confidenceAuthor" type="string" indexed="true" stored="true" multiValued="false"/>
//    <field name="confidenceDate" type="string" indexed="true" stored="true" multiValued="false"/>
//    <field name="tags" type="string" indexed="true" stored="true" multiValued="true"/>
//    <field name="status" type="string" indexed="true" stored="true" multiValued="false"/>
//    <field name="json" type="text_en" indexed="false" stored="true" multiValued="false"/>
//    <!-- Variant fields copied from OpenCGA -->

        // <field name="variantId" type="string" indexed="false" stored="true" multiValued="false"/>
        addStringFilters("variantId", query.getString(ClinicalQueryParam.CV_VARIANT_ID.key()), filters);

//    <field name="chromosome" type="string" indexed="true" stored="true" multiValued="false"/>
//    <field name="start" type="int" indexed="true" stored="true" multiValued="false"/>
//    <field name="end" type="int" indexed="true" stored="true" multiValued="false"/>
//    <field name="xrefs" type="string" indexed="true" stored="true" multiValued="true"/>
//    <field name="type" type="string" indexed="true" stored="true" multiValued="false"/>
//    <field name="release" type="int" indexed="true" stored="true" multiValued="false"/>
//    <field name="studies" type="string" indexed="true" stored="true" multiValued="true"/>
//    <field name="phastCons" type="double" indexed="true" stored="true" multiValued="false"/>
//    <field name="phylop" type="double" indexed="true" stored="true" multiValued="false"/>
//    <field name="gerp" type="double" indexed="true" stored="true" multiValued="false"/>
//    <field name="caddRaw" type="double" indexed="true" stored="true" multiValued="false"/>
//    <field name="caddScaled" type="double" indexed="true" stored="true" multiValued="false"/>
//    <field name="sift" type="double" indexed="true" stored="true" multiValued="false"/>
//    <field name="siftDesc" type="string" indexed="true" stored="true" multiValued="false"/>
//    <field name="polyphen" type="double" indexed="true" stored="true" multiValued="false"/>
//    <field name="polyphenDesc" type="string" indexed="true" stored="true" multiValued="false"/>
//    <field name="genes" type="string" indexed="false" stored="true" multiValued="true"/>
//    <field name="biotypes" type="string" indexed="true" stored="true" multiValued="true"/>
//    <field name="soAcc" type="int" indexed="true" stored="true" multiValued="true"/>
//    <field name="geneToSoAcc" type="string" indexed="true" stored="true" multiValued="true"/>
//    <!-- Clinical significance -->
//    <field name="clinicalSig" type="string" indexed="true" stored="true" multiValued="true"/>
//    <!--
//                The field 'traits' contains info about Clinvar (CV), COSMIC (CN), HPOs (HP), protein features (PD), protein keywords (KW):
//        CV - accession1 - trait1 - clinicalSignificance1
//        CV - accession2 - trait2 - clinicalSignificance2
//            ...
//        CM - mutationID1 - primaryHistology1 - HistologySubtype1
//        CM - mutationID2 - primaryHistology2 - HistologySubtype2
//            ...
//        HP - HPO1 - trait1 - name1
//        HP - HPO2 - trait2 - name2
//            ...
//        PD - ID1 - description1
//        PD - ID2 - description2
//            ...
//        KW - uniprotAccession1 - keyword1
//        KW - uniprotAccession2 - keyword2
//            ...
//        -->
//    <field name="traits" type="text_en" indexed="true" stored="true" multiValued="true"/>
//    <!--
//                The field 'other' contains info about display consequence type, HGVS, cytobands, repeats, score and trancripts:
//        DCT - displayConsequenceType
//        HGVS - hgvs1
//        HGVS - hgvs2
//            ...
//        CB - cytobandName1 - stain1 - start1 - end1
//        CB - cytobandName2 - stain2 - start2 - end2
//            ...
//        RP - repeatName1 - id1 - source1 - copyNumber1 - percentageMatch1 - start1 - end1
//        RP - repeatName2 - id2 - source2 - copyNumber2 - percentageMatch2 - start2 - end2
//            ...
//        SC - studyId - scoreId - score - pvalue - cohort1 - cohort2
//        SC - studyId - scoreId - score - pvalue - cohort1 - cohort2
//            ...
//        TRANS - transcriptId1 - biotype1 - cdnaPostion1 - cdsPosition1 - codon1 - uniprotAccession1 - uniprotAccession1 - uniprotVariantId1 - annotationFlags1 - position1 - aaChange1 - siftScore1 - siftDescr1 - poliphenScore1 - poliphenDescr1
//        TRANS - transcriptId2 - biotype2 - cdnaPostion2 - cdsPosition2 - codon2 - uniprotAccession2 - uniprotAccession2 - uniprotVariantId2 - annotationFlags2 - position2 - aaChange2 - siftScore2 - siftDescr2 - poliphenScore2 - poliphenDescr2
//            ...
//        For TRANS, transcript annotation flags are separated by , (e.g.: CCDS,basic)
//        -->
//    <field name="other" type="string" indexed="false" stored="true" multiValued="true"/>
//    <dynamicField name="passStats_*" type="float" indexed="true" stored="true" multiValued="false"/>
//    <dynamicField name="altStats_*" type="float" indexed="true" stored="true" multiValued="false"/>
//    <dynamicField name="popFreq_*" type="float" indexed="true" stored="true" multiValued="false"/>
//    <dynamicField name="score_*" type="float" indexed="true" stored="true" multiValued="false"/>
//    <dynamicField name="scorePValue_*" type="float" indexed="true" stored="true" multiValued="false"/>
//    <!-- These fields are only present when indexing one individual or a family -->
//    <dynamicField name="gt_*" type="string" indexed="true" stored="true" multiValued="false"/>
//    <dynamicField name="dp_*" type="int" indexed="true" stored="true" multiValued="false"/>
//    <dynamicField name="sampleFormat_*" type="string" indexed="false" stored="true" multiValued="false"/>
//    <dynamicField name="qual_*" type="float" indexed="true" stored="true" multiValued="false"/>
//    <dynamicField name="filter_*" type="string" indexed="true" stored="true" multiValued="false"/>
//    <dynamicField name="fileInfo_*" type="string" indexed="false" stored="true" multiValued="false"/>
        return filters;
    }

    public List<String> clinicalVariantEvidenceFilters(Query query) {
        List<String> filters = new ArrayList<>();

        // <field name="phenotypeNames" type="string" indexed="true" stored="true" multiValued="true"/>
        addStringFilters("phenotypeNames", query.getString(ClinicalQueryParam.CVE_PHENOTYPE_NAME_NAME), filters);

        // <field name="geneName" type="string" indexed="true" stored="true" multiValued="false"/>
        addStringFilters("geneName", query.getString(ClinicalQueryParam.CVE_GENE_NAME_NAME), filters);

        // <field name="consequenceTypeIds" type="string" indexed="true" stored="true" multiValued="true"/>
        addStringFilters("consequenceTypeIds", query.getString(ClinicalQueryParam.CVE_CONSEQUENCE_TYPE_ID_NAME), filters);

        // <field name="xrefIds" type="string" indexed="true" stored="true" multiValued="true"/>
        addStringFilters("xrefIds", query.getString(ClinicalQueryParam.CVE_XREF_ID_NAME), filters);

        // <field name="panelId" type="string" indexed="true" stored="true" multiValued="false"/>
        addStringFilters("panelId", query.getString(ClinicalQueryParam.CVE_PANEL_ID_NAME), filters);

        // <field name="acmgs" type="string" indexed="true" stored="true" multiValued="true"/>
        addStringFilters("acmgs", query.getString(ClinicalQueryParam.CVE_ACGM_NAME), filters);

        // <field name="tier" type="string" indexed="true" stored="true" multiValued="false"/>
        addStringFilters("tier", query.getString(ClinicalQueryParam.CVE_TIER_NAME), filters);

        // <field name="clinicalSignificance" type="string" indexed="true" stored="true" multiValued="false"/>
        addStringFilters("clinicalSignificance", query.getString(ClinicalQueryParam.CVE_CLINICAL_SIGNIFICANCE_NAME), filters);

        // <field name="drugResponse" type="string" indexed="true" stored="true" multiValued="false"/>
        addStringFilters("drugResponse", query.getString(ClinicalQueryParam.CVE_DRUG_RESPONSE_NAME), filters);

        // <field name="traitAssociation" type="string" indexed="true" stored="true" multiValued="false"/>
        addStringFilters("traitAssociation", query.getString(ClinicalQueryParam.CVE_TRAIT_ASSOCIATION_NAME), filters);

        // <field name="functionalEffect" type="string" indexed="true" stored="true" multiValued="false"/>
        addStringFilters("functionalEffect", query.getString(ClinicalQueryParam.CVE_FUNCTIONAL_EFFECT_NAME), filters);

        // <field name="tumorigenesis" type="string" indexed="true" stored="true" multiValued="false"/>
        addStringFilters("tumorigenesis", query.getString(ClinicalQueryParam.CVE_TUMORIGENESIS_NAME), filters);

        // <field name="otherClassifications" type="string" indexed="true" stored="true" multiValued="true"/>
        addStringFilters("otherClassifications", query.getString(ClinicalQueryParam.CVE_OTHER_CLASSIFICATION_NAME), filters);

        // <field name="rolesInCancer" type="string" indexed="true" stored="true" multiValued="true"/>
        addStringFilters("rolesInCancer", query.getString(ClinicalQueryParam.CVE_ROL_IN_CANCER_NAME), filters);

        // <dynamicField name="score_*" type="double" indexed="true" stored="true" multiValued="false"/>

        return filters;
    }

    protected void addStringFilters(String fieldName, String fieldValue, List<String> filters) {
        if (StringUtils.isNotEmpty(fieldValue)) {
            List<String> values = Arrays.asList(fieldValue.split(","));

            StringBuilder sb = new StringBuilder();
            for (String value : values) {
                if (sb.length() > 0) {
                    sb.append(" OR ");
                }
                sb.append(fieldName).append(": \"").append(value).append("\"");
            }
            filters.add(sb.toString());
        }
    }

    protected void addStringFilters(List<String> filters, SolrQuery solrQuery) {
        addStringFilters(filters, null, solrQuery);
    }

    protected void addStringFilters(List<String> filters, String join, SolrQuery solrQuery) {
        if (CollectionUtils.isEmpty(filters)) {
            // Nothing to do
            return;
        }

        if (StringUtils.isEmpty(join)) {
            // No join query
            for (String filter : filters) {
                solrQuery.addFilterQuery(filter);
            }
        } else {
            // Join query
            String joinFilterQuery = join + "(" + StringUtils.join(filters, " AND ") + ")";
            solrQuery.addFilterQuery(joinFilterQuery);
        }
    }
}
