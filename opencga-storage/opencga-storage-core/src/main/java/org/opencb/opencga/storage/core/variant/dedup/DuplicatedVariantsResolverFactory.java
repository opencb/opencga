package org.opencb.opencga.storage.core.variant.dedup;

import org.opencb.biodata.tools.variant.VariantDeduplicationTask;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.storage.core.io.managers.IOConnectorProvider;
import org.opencb.opencga.storage.core.variant.io.VariantReaderUtils;

import java.net.URI;

import static org.opencb.opencga.storage.core.variant.VariantStorageOptions.DEDUPLICATION_BUFFER_SIZE;
import static org.opencb.opencga.storage.core.variant.VariantStorageOptions.DEDUPLICATION_POLICY;

public class DuplicatedVariantsResolverFactory {

    private final ObjectMap configuration;
    private final IOConnectorProvider ioConnectorProvider;

    public DuplicatedVariantsResolverFactory(ObjectMap configuration, IOConnectorProvider ioConnectorProvider) {
        this.configuration = configuration;
        this.ioConnectorProvider = ioConnectorProvider;
    }

    public VariantDeduplicationTask getTask(String variantsFile, URI outdir) {
        AbstractDuplicatedVariantsResolver resolver = getResolver(variantsFile, outdir);
        int bufferSize = configuration.getInt(DEDUPLICATION_BUFFER_SIZE.key(), DEDUPLICATION_BUFFER_SIZE.defaultValue());

        return new VariantDeduplicationTask(resolver, bufferSize) {
            @Override
            public void post() throws Exception {
                super.post();
                resolver.close();
            }
        };
    }

    public AbstractDuplicatedVariantsResolver getResolver(String variantsFile, URI outdir) {
        URI output = outdir.resolve(
                VariantReaderUtils.getOriginalFromTransformedFile(variantsFile) + "." + VariantReaderUtils.DUPLICATED_FILE + ".tsv");

        String deduplicationPolicy = configuration.getString(DEDUPLICATION_POLICY.key(), DEDUPLICATION_POLICY.defaultValue());

        switch (deduplicationPolicy.toLowerCase()) {
            case "discard":
                return new DiscardDuplicatedVariantsResolver(variantsFile, ioConnectorProvider, output);
            case "maxqual":
            case "max_qual":
                return new MaxQualDuplicatedVariantsResolver(variantsFile, ioConnectorProvider, output);
            default:
                throw new IllegalArgumentException("Unknown deduplication variants policy " + deduplicationPolicy);
        }

    }

}
