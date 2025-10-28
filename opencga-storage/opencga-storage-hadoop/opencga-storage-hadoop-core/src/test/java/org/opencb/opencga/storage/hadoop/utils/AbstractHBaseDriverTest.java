package org.opencb.opencga.storage.hadoop.utils;

import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.core.testclassification.duration.ShortTests;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.util.Arrays;

import static org.junit.Assert.assertArrayEquals;

@Category(ShortTests.class)
public class AbstractHBaseDriverTest {


    @Test
    public void testBuildArgs() {
        ObjectMap options = new ObjectMap();
        assertArrayEquals(new String[]{"myTable"}, AbstractHBaseDriver.buildArgs("myTable", options));

        options.put("key", "value");
        options.put("list", Arrays.asList(1, 2, 3));
        options.put("null", null);
        options.put("key with spaces", "value with spaces");
        options.put("boolean", "true");
        options.put("nested", new ObjectMap()
                .append("key1", "value1")
                .append("key2", "value2")
                .append("key3", new ObjectMap("deep", "nested"))
                .append("key4_void", new ObjectMap()));
        assertArrayEquals(new String[]{"myTable",
                        "key", "value",
                        "list", "1,2,3",
                        "key with spaces", "value with spaces",
                        "boolean", "true",
                        "nested.key1", "value1",
                        "nested.key2", "value2",
                        "nested.key3.deep", "nested"},
                AbstractHBaseDriver.buildArgs("myTable", options));
    }

    @Test
    public void testStdinParseArgs() throws Exception {
        String[] args = new String[]{
                "myTable",
                "key", "value%$£?$~\"\'#\t#",
                "list", "1,2,3",
                "key with spaces", "value with spaces",
                "keyWeirdChars", "value%$£?$~\"\'#\t#",
                "boolean", "true",
                "void", "",
                "value_with_lines", "line1\nline2",
                "value_with_multiple_lines", "\n\nline1\nline2\n\nline3\n\n\n",
                "value_with_escaped_n", "line1\\nline2",
                "value_with_other_escaped", "line1\\tline2\\bline3\\\\",
                "value_with_escaped_quotes", "line1\\\"line2\\'line3",
                "value_with_escaped_beginning", "\\nline1\\nline2\\n",
                "value_with_escaped_end", "line1\\nline2\\n",
                "value_with_escaped_both", "\\nline1\\nline2\\n",
                "value_with_CR_LF", "line1\r\nline2\r\nline3",
                "value_with_CR", "line1\rline2\rline3",
                "value_with_LF", "line1\nline2\nline3",
                "nested.key1", "value1",
                "nested.key2", "value2",
                "nested.key3.deep", "nested",
                "SuperLongKeyNameThatShouldBeAllowedBecauseWhyNotEvenIfItIsCrazyAndWeAreJustTestingTheLimitsOfTheSystemHereSuperLongKeyNameThatShouldBeAllowedBecauseWhyNotEvenIfItIsCrazyAndWeAreJustTestingTheLimitsOfTheSystemHereSuperLongKeyNameThatShouldBeAllowedBecauseWhyNotEvenIfItIsCrazyAndWeAreJustTestingTheLimitsOfTheSystemHereSuperLongKeyNameThatShouldBeAllowedBecauseWhyNotEvenIfItIsCrazyAndWeAreJustTestingTheLimitsOfTheSystemHereSuperLongKeyNameThatShouldBeAllowedBecauseWhyNotEvenIfItIsCrazyAndWeAreJustTestingTheLimitsOfTheSystemHereSuperLongKeyNameThatShouldBeAllowedBecauseWhyNotEvenIfItIsCrazyAndWeAreJustTestingTheLimitsOfTheSystemHereSuperLongKeyNameThatShouldBeAllowedBecauseWhyNotEvenIfItIsCrazyAndWeAreJustTestingTheLimitsOfTheSystemHereSuperLongKeyNameThatShouldBeAllowedBecauseWhyNotEvenIfItIsCrazyAndWeAreJustTestingTheLimitsOfTheSystemHereSuperLongKeyNameThatShouldBeAllowedBecauseWhyNotEvenIfItIsCrazyAndWeAreJustTestingTheLimitsOfTheSystemHereSuperLongKeyNameThatShouldBeAllowedBecauseWhyNotEvenIfItIsCrazyAndWeAreJustTestingTheLimitsOfTheSystemHereSuperLongKeyNameThatShouldBeAllowedBecauseWhyNotEvenIfItIsCrazyAndWeAreJustTestingTheLimitsOfTheSystemHereSuperLongKeyNameThatShouldBeAllowedBecauseWhyNotEvenIfItIsCrazyAndWeAreJustTestingTheLimitsOfTheSystemHere", "SomeValueThatIsAlsoQuiteLongButStillShouldBeAllowedAndWeAreJustTestingTheLimitsOfTheSystemHereSomeValueThatIsAlsoQuiteLongButStillShouldBeAllowedAndWeAreJustTestingTheLimitsOfTheSystemHereSomeValueThatIsAlsoQuiteLongButStillShouldBeAllowedAndWeAreJustTestingTheLimitsOfTheSystemHereSomeValueThatIsAlsoQuiteLongButStillShouldBeAllowedAndWeAreJustTestingTheLimitsOfTheSystemHereSomeValueThatIsAlsoQuiteLongButStillShouldBeAllowedAndWeAreJustTestingTheLimitsOfTheSystemHereSomeValueThatIsAlsoQuiteLongButStillShouldBeAllowedAndWeAreJustTestingTheLimitsOfTheSystemHereSomeValueThatIsAlsoQuiteLongButStillShouldBeAllowedAndWeAreJustTestingTheLimitsOfTheSystemHereSomeValueThatIsAlsoQuiteLongButStillShouldBeAllowedAndWeAreJustTestingTheLimitsOfTheSystemHereSomeValueThatIsAlsoQuiteLongButStillShouldBeAllowedAndWeAreJustTestingTheLimitsOfTheSystemHereSomeValueThatIsAlsoQuiteLongButStillShouldBeAllowedAndWeAreJustTestingTheLimitsOfTheSystemHereSomeValueThatIsAlsoQuiteLongButStillShouldBeAllowedAndWeAreJustTestingTheLimitsOfTheSystemHereSomeValueThatIsAlsoQuiteLongButStillShouldBeAllowedAndWeAreJustTestingTheLimitsOfTheSystemHere"
        };
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        AbstractHBaseDriver.writeArgsToStream(args, new DataOutputStream(stream));
//        System.out.println("stream.toString() = " + stream.toString());
        String[] argsRead = AbstractHBaseDriver.readArgsFromStream(new java.io.DataInputStream(
                new java.io.ByteArrayInputStream(stream.toByteArray())));
        assertArrayEquals(args, argsRead);
    }

}