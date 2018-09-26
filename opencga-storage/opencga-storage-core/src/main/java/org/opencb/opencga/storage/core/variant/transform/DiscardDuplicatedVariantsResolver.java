package org.opencb.opencga.storage.core.variant.transform;

import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.tools.variant.VariantDeduplicationTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;

/**
 * Created on 12/09/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class DiscardDuplicatedVariantsResolver implements VariantDeduplicationTask.DuplicatedVariantsResolver {

    private final Logger logger = LoggerFactory.getLogger(DiscardDuplicatedVariantsResolver.class);
    private final int fileId;
    private int duplicatedLocus;
    private int duplicatedVariants;

    public DiscardDuplicatedVariantsResolver(int fileId) {
        this.fileId = fileId;
    }

    @Override
    public List<Variant> resolveDuplicatedVariants(List<Variant> list) {
        duplicatedLocus++;
        duplicatedVariants += list.size();
        if (list.size() > 1) {
            logger.warn("Found {} duplicated variants for file {} in variant {}.", list.size(), fileId, list.get(0));
            return Collections.emptyList();
        } else {
            throw new IllegalStateException("Unexpected list of " + list.size() + " duplicated variants : " + list);
        }
    }

    public int getDuplicatedLocus() {
        return duplicatedLocus;
    }

    public int getDuplicatedVariants() {
        return duplicatedVariants;
    }
}
