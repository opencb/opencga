package org.opencb.opencga.catalog.stats.solr.converters;

import org.apache.commons.lang3.NotImplementedException;
import org.opencb.commons.datastore.core.ComplexTypeConverter;
import org.opencb.opencga.catalog.stats.solr.FileSolrModel;
import org.opencb.opencga.core.models.File;
import org.opencb.opencga.core.models.Study;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created by wasim on 04/07/18.
 */
public class CatalogFileToSolrFileConverter implements ComplexTypeConverter<File, FileSolrModel> {

    private Study study;

    public CatalogFileToSolrFileConverter(Study study) {
        this.study = study;
    }

    @Override
    public File convertToDataModelType(FileSolrModel object) {
        throw new NotImplementedException("Operation not supported");
    }

    @Override
    public FileSolrModel convertToStorageType(File file) {
        FileSolrModel fileSolrModel = new FileSolrModel();

        fileSolrModel.setUid(file.getUid());
        fileSolrModel.setName(file.getName());
        fileSolrModel.setStudyId(study.getFqn().replace(":", "__"));
        fileSolrModel.setType(file.getType().name());
        if (file.getFormat() != null) {
            fileSolrModel.setFormat(file.getFormat().name());
        }
        if (file.getBioformat() != null) {
            fileSolrModel.setBioformat(file.getBioformat().name());
        }
        fileSolrModel.setRelease(file.getRelease());
        fileSolrModel.setCreationDate(file.getCreationDate());
        fileSolrModel.setStatus(file.getStatus().getName());
        fileSolrModel.setExternal(file.isExternal());
        fileSolrModel.setSize(file.getSize());
        if (file.getSoftware() != null) {
            fileSolrModel.setSoftware(file.getSoftware().getName());
        }
        fileSolrModel.setExperiment(file.getExperiment().getName());
        if (file.getSamples() != null) {
            fileSolrModel.setSamples(file.getSamples().size());
        }

        fileSolrModel.setAnnotations(SolrConverterUtil.populateAnnotations(file.getAnnotationSets()));

        // Extract the permissions
        Map<String, Set<String>> fileAcl =
                SolrConverterUtil.parseInternalOpenCGAAcls((List<Map<String, Object>>) file.getAttributes().get("OPENCGA_ACL"));
        List<String> effectivePermissions =
                SolrConverterUtil.getEffectivePermissions((Map<String, Set<String>>) study.getAttributes().get("OPENCGA_ACL"), fileAcl,
                        "FILE");
        fileSolrModel.setAcl(effectivePermissions);

        return fileSolrModel;
    }
}
