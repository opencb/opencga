package org.opencb.opencga.storage.core.variant.annotation.annotators;

import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.VariantAnnotation;
import org.opencb.cellbase.client.config.ClientConfiguration;
import org.opencb.cellbase.client.rest.CellBaseClient;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryResponse;
import org.opencb.opencga.storage.core.config.StorageConfiguration;
import org.opencb.opencga.storage.core.variant.annotation.VariantAnnotatorException;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Created on 23/11/16.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class CellBaseRestVariantAnnotator extends AbstractCellBaseVariantAnnotator {
    private static final int TIMEOUT = 10000;

    private CellBaseClient cellBaseClient = null;

    public CellBaseRestVariantAnnotator(StorageConfiguration storageConfiguration, ObjectMap options) throws VariantAnnotatorException {
        super(storageConfiguration, options);

        List<String> hosts = storageConfiguration.getCellbase().getHosts();
        if (hosts.isEmpty()) {
            throw new VariantAnnotatorException("Missing defaultValue \"CellBase Hosts\"");
        }

        String cellbaseRest = hosts.get(0);
        checkNotNull(cellbaseRest, "cellbase hosts");
        ClientConfiguration clientConfiguration = storageConfiguration.getCellbase().toClientConfiguration();
        clientConfiguration.getRest().setTimeout(TIMEOUT);
        CellBaseClient cellBaseClient;
        cellBaseClient = new CellBaseClient(species, assembly, clientConfiguration);
        this.cellBaseClient = cellBaseClient;
    }

    @Override
    protected List<VariantAnnotation> annotateFiltered(List<Variant> variants) throws VariantAnnotatorException {
        try {
            QueryResponse<VariantAnnotation> queryResponse = cellBaseClient.getVariantClient()
                    .getAnnotations(variants.stream().map(Variant::toString).collect(Collectors.toList()),
                            queryOptions, true);
            return getVariantAnnotationList(variants, queryResponse.getResponse());
        } catch (IOException e) {
            throw new VariantAnnotatorException("Error fetching variants from Client");
        }
    }
}
