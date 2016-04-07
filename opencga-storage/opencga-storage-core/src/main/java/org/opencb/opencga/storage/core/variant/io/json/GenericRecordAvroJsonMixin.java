package org.opencb.opencga.storage.core.variant.io.json;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * Created on 09/10/15.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
@JsonIgnoreProperties({"schema"})
public abstract class GenericRecordAvroJsonMixin {
}
