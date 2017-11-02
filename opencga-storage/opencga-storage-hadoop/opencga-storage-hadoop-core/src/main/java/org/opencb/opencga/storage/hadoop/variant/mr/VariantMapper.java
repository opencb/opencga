package org.opencb.opencga.storage.hadoop.variant.mr;

import org.apache.hadoop.mapreduce.Mapper;
import org.opencb.biodata.models.variant.Variant;

/**
 * Created on 27/10/17.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class VariantMapper<KEYOUT, VALUEOUT> extends Mapper<Object, Variant, KEYOUT, VALUEOUT> {
}
