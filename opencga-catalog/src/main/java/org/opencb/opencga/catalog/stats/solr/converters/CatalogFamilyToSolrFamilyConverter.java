package org.opencb.opencga.catalog.stats.solr.converters;

import org.opencb.commons.datastore.core.ComplexTypeConverter;
import org.opencb.opencga.catalog.stats.solr.FamilySolrModel;
import org.opencb.opencga.core.models.Family;

/**
 * Created by wasim on 04/07/18.
 */
public class CatalogFamilyToSolrFamilyConverter implements ComplexTypeConverter<Family, FamilySolrModel> {
    @Override
    public Family convertToDataModelType(FamilySolrModel object) {
        try {
            throw new Exception("Not supported operation!!");
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public FamilySolrModel convertToStorageType(Family family) {
        FamilySolrModel familySolrModel = new FamilySolrModel();

        familySolrModel.setUid(family.getUid());
        familySolrModel.setStudyId(SolrConverterUtil.getStudyId(family.getStudyUid()));
        familySolrModel.setCreationDate(family.getCreationDate());

        if (family.getStatus() != null) {
            familySolrModel.setStatus(family.getStatus().getName());
        }
        familySolrModel.setPhenotypes(SolrConverterUtil.populatePhenotypes(family.getPhenotypes()));

        if (family.getMembers() != null) {
            familySolrModel.setFamilyMembers(family.getMembers().size());
        }

        familySolrModel.setRelease(family.getRelease());
        familySolrModel.setVersion(family.getVersion());
        familySolrModel.setAnnotations(SolrConverterUtil.populateAnnotations(family.getAnnotationSets()));

        return familySolrModel;
    }
}
