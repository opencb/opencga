package org.opencb.opencga.storage.core.variant.io;

import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.metadata.ExportMetadata;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;

import java.io.IOException;
import java.net.URI;

/**
 * Created on 07/12/16.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public abstract class VariantImporter {

    protected final VariantDBAdaptor dbAdaptor;

    public VariantImporter(VariantDBAdaptor dbAdaptor) {
        this.dbAdaptor = dbAdaptor;
    }

    public void importData(URI inputUri) throws StorageEngineException, IOException {
        VariantMetadataImporter metadataImporter = new VariantMetadataImporter();
        ExportMetadata exportMetadata = metadataImporter.importMetaData(inputUri, dbAdaptor.getStudyConfigurationManager());

        importData(inputUri, exportMetadata);
    }

    public abstract void importData(URI input, ExportMetadata metadata) throws StorageEngineException, IOException;

}
