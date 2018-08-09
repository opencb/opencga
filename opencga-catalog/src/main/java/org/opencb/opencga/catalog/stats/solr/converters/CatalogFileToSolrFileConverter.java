package org.opencb.opencga.catalog.stats.solr.converters;

import org.opencb.commons.datastore.core.ComplexTypeConverter;
import org.opencb.opencga.catalog.stats.solr.FileSolrModel;
import org.opencb.opencga.core.models.File;

/**
 * Created by wasim on 04/07/18.
 */
public class CatalogFileToSolrFileConverter implements ComplexTypeConverter<File, FileSolrModel> {

    @Override
    public File convertToDataModelType(FileSolrModel object) {
        try {
            throw new Exception("Not supported operation!!");
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;

    }

    @Override
    public FileSolrModel convertToStorageType(File file) {
        FileSolrModel fileSolrModel = new FileSolrModel();

        fileSolrModel.setUid(file.getUid());
        fileSolrModel.setName(file.getName());
        fileSolrModel.setType(file.getType().name());
        if (file.getFormat() != null) {
            fileSolrModel.setFormat(file.getFormat().name());
        }
        if (file.getBioformat() != null) {
            fileSolrModel.setBioformart(file.getBioformat().name());
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

        return fileSolrModel;
    }
}
