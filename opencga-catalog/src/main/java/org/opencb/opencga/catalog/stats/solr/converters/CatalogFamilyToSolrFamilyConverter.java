package org.opencb.opencga.catalog.stats.solr.converters;

import org.apache.commons.lang3.NotImplementedException;
import org.opencb.commons.datastore.core.ComplexTypeConverter;
import org.opencb.opencga.catalog.stats.solr.FamilySolrModel;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.models.Family;
import org.opencb.opencga.core.models.Study;

import java.time.LocalDate;
import java.time.ZoneId;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created by wasim on 04/07/18.
 */
public class CatalogFamilyToSolrFamilyConverter implements ComplexTypeConverter<Family, FamilySolrModel> {

    private Study study;

    public CatalogFamilyToSolrFamilyConverter(Study study) {
        this.study = study;
    }

    @Override
    public Family convertToDataModelType(FamilySolrModel object) {
        throw new NotImplementedException("Operation not supported");
    }

    @Override
    public FamilySolrModel convertToStorageType(Family family) {
        FamilySolrModel familySolrModel = new FamilySolrModel();

        familySolrModel.setUid(family.getUid());
        familySolrModel.setStudyId(study.getFqn().replace(":", "__"));

        Date date = TimeUtils.toDate(family.getCreationDate());
        LocalDate localDate = date.toInstant().atZone(ZoneId.systemDefault()).toLocalDate();

        familySolrModel.setCreationYear(localDate.getYear());
        familySolrModel.setCreationMonth(localDate.getMonth().toString());
        familySolrModel.setCreationDay(localDate.getDayOfMonth());
        familySolrModel.setCreationDayOfWeek(localDate.getDayOfWeek().toString());
        familySolrModel.setStatus(family.getStatus().getName());

        if (family.getStatus() != null) {
            familySolrModel.setStatus(family.getStatus().getName());
        }
        familySolrModel.setPhenotypes(SolrConverterUtil.populatePhenotypes(family.getPhenotypes()));

        familySolrModel.setNumMembers(family.getMembers() != null ? family.getMembers().size() : 0);
        familySolrModel.setExpectedSize(family.getExpectedSize());

        familySolrModel.setRelease(family.getRelease());
        familySolrModel.setVersion(family.getVersion());
        familySolrModel.setAnnotations(SolrConverterUtil.populateAnnotations(family.getAnnotationSets()));

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
