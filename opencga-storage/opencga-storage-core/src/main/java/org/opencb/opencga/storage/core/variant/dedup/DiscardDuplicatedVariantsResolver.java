package org.opencb.opencga.storage.core.variant.dedup;

import org.opencb.biodata.models.variant.Variant;
import org.opencb.opencga.storage.core.io.managers.IOConnector;

import java.net.URI;
import java.util.List;

/**
 * Created on 12/09/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class DiscardDuplicatedVariantsResolver extends AbstractDuplicatedVariantsResolver {

    public DiscardDuplicatedVariantsResolver(String variantFile, IOConnector ioConnector, URI duplicatedVariantsOutputFile) {
        super(variantFile, ioConnector, duplicatedVariantsOutputFile);
    }

    @Override
    Variant internalResolveDuplicatedVariants(List<Variant> list) {
        return null;
    }
}
