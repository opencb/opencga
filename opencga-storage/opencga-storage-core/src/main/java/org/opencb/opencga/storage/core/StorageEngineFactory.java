package org.opencb.opencga.storage.core;

import org.opencb.biodata.formats.variant.io.VariantWriter;
import org.opencb.opencga.storage.core.alignment.adaptors.AlignmentQueryBuilder;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;


/**
 * Created by imedina on 12/08/14.
 */
public interface StorageEngineFactory {


//    VariantReader createVariantDBReader();

    VariantDBAdaptor createVariantDBAdaptor();

    VariantWriter createVariantDBWriter();



    AlignmentQueryBuilder createAlignmentDBAdapator();




}
