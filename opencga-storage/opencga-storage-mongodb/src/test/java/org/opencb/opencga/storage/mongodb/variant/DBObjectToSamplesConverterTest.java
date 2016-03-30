package org.opencb.opencga.storage.mongodb.variant;

import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Created on 07/03/16.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class DBObjectToSamplesConverterTest {

    @Test
    public void testIntValue() throws Exception {
        testInteger("10");
        testInteger("2");
        testInteger("1");
        testInteger("0");
        testInteger(".");
        testInteger("-1");
        testInteger("-10");

    }

    @Test
    public void testFloatValue() throws Exception {

        testFloat("10", 10001, "10.0");
        testFloat("10.000", 10001, "10.0");
        testFloat("10.0", 10001);
        testFloat("2", 2001, "2.0");
        testFloat("2.0", 2001);
        testFloat("1", 1001, "1.0");
        testFloat("1.0", 1001);
        testFloat("0.001", 2);
        testFloat("0.0019999", 2, "0.001");
        testFloat("0", 1, "0.0");
        testFloat("0.0", 1);

        testFloat(".", 0, ".");
        testFloat("..", 0, ".");
        testFloat("NOT_A_NUMBER", 0, ".");

        testFloat("-0.001", -1);
        testFloat("-1", -1000, "-1.0");
        testFloat("-1.0", -1000);
        testFloat("-10", -10000, "-10.0");
        testFloat("-10.0", -10000);


    }

    public void testInteger(String dataModelType) {
        assertEquals(dataModelType, DBObjectToSamplesConverter.INTEGER_COMPLEX_TYPE_CONVERTER.convertToDataModelType(DBObjectToSamplesConverter.INTEGER_COMPLEX_TYPE_CONVERTER.convertToStorageType(dataModelType)));
    }

    public void testFloat(String dataModelType, Integer i) {
        testFloat(dataModelType, i, dataModelType);
    }

    public void testFloat(String dataModelType, Integer i, String expected) {
        Integer storageType = DBObjectToSamplesConverter.FLOAT_COMPLEX_TYPE_CONVERTER.convertToStorageType(dataModelType);
        assertEquals(i, storageType);
        assertEquals(expected, DBObjectToSamplesConverter.FLOAT_COMPLEX_TYPE_CONVERTER.convertToDataModelType(storageType));
    }
}