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
        return fileSolrModel;
    }
}
