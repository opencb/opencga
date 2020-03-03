package org.opencb.opencga.catalog.stats.solr.converters;

import org.apache.commons.lang3.NotImplementedException;
import org.opencb.commons.datastore.core.ComplexTypeConverter;
import org.opencb.commons.datastore.core.QueryParam;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.stats.solr.FamilySolrModel;
import org.opencb.opencga.catalog.utils.AnnotationUtils;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.models.common.AnnotationSet;
import org.opencb.opencga.core.models.family.Family;
import org.opencb.opencga.core.models.study.Study;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDate;
import java.time.ZoneId;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Created by wasim on 04/07/18.
 */
public class CatalogFamilyToSolrFamilyConverter implements ComplexTypeConverter<Family, FamilySolrModel> {

    private Study study;
    private Map<String, Map<String, QueryParam.Type>> variableMap;

    protected static Logger logger = LoggerFactory.getLogger(CatalogFamilyToSolrFamilyConverter.class);

    public CatalogFamilyToSolrFamilyConverter(Study study) {
        this.study = study;
        this.variableMap = new HashMap<>();
        if (this.study.getVariableSets() != null) {
            this.study.getVariableSets().forEach(variableSet -> {
                try {
                    this.variableMap.put(variableSet.getId(), AnnotationUtils.getVariableMap(variableSet));
                } catch (CatalogDBException e) {
                    logger.warn("Could not parse variableSet {}: {}", variableSet.getId(), e.getMessage(), e);
                }
            });
        }
    }

    @Override
    public Family convertToDataModelType(FamilySolrModel object) {
        throw new NotImplementedException("Operation not supported");
    }

    @Override
    public FamilySolrModel convertToStorageType(Family family) {
        FamilySolrModel familySolrModel = new FamilySolrModel();

        familySolrModel.setId(family.getUuid());
        familySolrModel.setUid(family.getUid());
        familySolrModel.setStudyId(study.getFqn().replace(":", "__"));

        Date date = TimeUtils.toDate(family.getCreationDate());
        LocalDate localDate = date.toInstant().atZone(ZoneId.systemDefault()).toLocalDate();

        familySolrModel.setCreationYear(localDate.getYear());
        familySolrModel.setCreationMonth(localDate.getMonth().toString());
        familySolrModel.setCreationDay(localDate.getDayOfMonth());
        familySolrModel.setCreationDayOfWeek(localDate.getDayOfWeek().toString());
        familySolrModel.setStatus(family.getInternal().getStatus().getName());

        if (family.getInternal().getStatus() != null) {
            familySolrModel.setStatus(family.getInternal().getStatus().getName());
        }
        familySolrModel.setPhenotypes(SolrConverterUtil.populatePhenotypes(family.getPhenotypes()));
        familySolrModel.setPhenotypes(SolrConverterUtil.populateDisorders(family.getDisorders()));

        familySolrModel.setNumMembers(family.getMembers() != null ? family.getMembers().size() : 0);
        familySolrModel.setExpectedSize(family.getExpectedSize());

        familySolrModel.setRelease(family.getRelease());
        familySolrModel.setVersion(family.getVersion());
        familySolrModel.setAnnotations(SolrConverterUtil.populateAnnotations(variableMap, family.getAnnotationSets()));

        if (family.getAnnotationSets() != null) {
            familySolrModel.setAnnotationSets(family.getAnnotationSets().stream().map(AnnotationSet::getId).collect(Collectors.toList()));
        } else {
            familySolrModel.setAnnotationSets(Collections.emptyList());
        }

        // Extract the permissions
        Map<String, Set<String>> familyAcl =
                SolrConverterUtil.parseInternalOpenCGAAcls((List<Map<String, Object>>) family.getAttributes().get("OPENCGA_ACL"));
        List<String> effectivePermissions =
                SolrConverterUtil.getEffectivePermissions((Map<String, Set<String>>) study.getAttributes().get("OPENCGA_ACL"), familyAcl,
                        "FAMILY");
        familySolrModel.setAcl(effectivePermissions);

        return familySolrModel;
    }
}
