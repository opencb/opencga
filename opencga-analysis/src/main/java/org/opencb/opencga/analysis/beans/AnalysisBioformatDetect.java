package org.opencb.opencga.analysis.beans;

import org.apache.commons.lang.math.NumberUtils;
import org.opencb.opencga.catalog.beans.File;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.regex.Pattern;

/**
 * Created by ralonso on 12/03/15.
 */
public class AnalysisBioformatDetect {

    protected static Pattern variantPattern = Pattern.compile(".*\\.vcf(\\.[\\w]*)*", Pattern.CASE_INSENSITIVE);
    protected static Pattern alignmentPattern = Pattern.compile(".*\\.(sam|bam|cram)(\\.[\\w]*)*", Pattern.CASE_INSENSITIVE);

    public static File.Bioformat detect(String path) {
        Path source = Paths.get(path);
        String mimeType = "";
        try {

            if (variantPattern.matcher(path).matches()) {
                return File.Bioformat.VARIANT;
            } else if (alignmentPattern.matcher(path).matches()) {
                return File.Bioformat.ALIGNMENT;
            }

            mimeType = Files.probeContentType(source);

            if (path.endsWith(".nw")) {
                return File.Bioformat.OTHER_NEWICK;
            }

            if (!mimeType.equalsIgnoreCase("text/plain")
                    || path.endsWith(".redirection")
                    || path.endsWith(".Rout")
                    || path.endsWith("cel_files.txt")
                    || !path.endsWith(".txt")) {
                return File.Bioformat.NONE;
            }

            FileInputStream fstream = new FileInputStream(path);
            BufferedReader br = new BufferedReader(new InputStreamReader(fstream));

            String strLine;
            int numberOfLines = 20;

            int i = 0;
            boolean names = false;
            while ((strLine = br.readLine()) != null) {
                if (strLine.equalsIgnoreCase("")) {
                    continue;
                }
                if (i == numberOfLines) {
                    break;
                }
                if (strLine.startsWith("#")) {
                    if (strLine.startsWith("#NAMES")) {
                        names = true;
                    } else {
                        continue;
                    }
                } else {
                    String[] fields = strLine.split("\t");
                    if (fields.length > 2) {
                        if (names && NumberUtils.isNumber(fields[1])) {
                            return File.Bioformat.DATAMATRIX_EXPRESSION;
                        }
                    } else if (fields.length == 1) {
                        if (fields[0].split(" ").length == 1 && !NumberUtils.isNumber(fields[0])) {
                            return File.Bioformat.IDLIST;
                        }
                    } else if (fields.length == 2) {
                        if (!fields[0].contains(" ") && NumberUtils.isNumber(fields[1])) {
                            return File.Bioformat.IDLIST_RANKED;
                        }
                    }
                }
                i++;
            }
            br.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return File.Bioformat.NONE;
    }

}
