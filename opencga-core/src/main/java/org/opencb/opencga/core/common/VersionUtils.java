package org.opencb.opencga.core.common;

import org.apache.commons.lang3.StringUtils;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

public class VersionUtils {

    public static List<String> order(List<String> versions) {
        return versions.stream().map(Version::new).sorted().map(Version::toString).collect(Collectors.toList());
    }

    public static boolean isMinVersion(String minVersion, String version) {
        int c = new Version(minVersion).compareTo(new Version(version));
//        if (c == 0) {
//            System.out.println(minVersion + "\t=\t" + version);
//        } else if (c < 0) {
//            System.out.println(minVersion + "\t<\t" + version);
//        } else {
//            System.out.println(minVersion + "\t>\t" + version);
//        }
        return c <= 0;
    }

//    public static void main(String[] args) {
//        isMinVersion("5.2.7", "5.2.8");
//        isMinVersion("5.2.7", "5.2.7");
//        isMinVersion("5.2.7.1", "5.2.7.1-alpha");
//        isMinVersion("5.2.7", "5.2.7-SNAPSHOT");
//        isMinVersion("5.2.7-alpha", "5.2.7");
//        isMinVersion("5.2.7-alpha", "5.2.7-beta");
//        isMinVersion("5.2.7", "5.2.6");
//        isMinVersion("5.2.7", "5.2.7.0");
//    }

    public static class Version implements Comparable<Version> {

        private final int major;
        private final int minor;
        private final int patch;
        private final int repatch;
        private final String other;

        public static final Comparator<Version> COMPARATOR = Comparator
                .comparingInt(Version::getMajor)
                .thenComparingInt(Version::getMinor)
                .thenComparingInt(Version::getPatch)
                .thenComparingInt(Version::getRepatch)
                .thenComparing((o1, o2) -> {
                    if (o1.other.equals(o2.other)) {
                        return 0;
                    }
                    if (o1.other.isEmpty()) {
                        return +1;
                    }
                    if (o2.other.isEmpty()) {
                        return -1;
                    }
                    if (o1.other.equals("-SNAPSHOT")) {
                        return -1;
                    }
                    if (o2.other.equals("-SNAPSHOT")) {
                        return +1;
                    }
                    return o1.other.compareTo(o2.other);
                });

        public Version(String version) {
            String[] split = StringUtils.split(version, ".", 4);
            major = Integer.parseInt(split[0]);
            minor = Integer.parseInt(split[1]);
            if (split.length == 4) {
                patch = Integer.parseInt(split[2]);
                String last = split[3];
                String[] split2 = StringUtils.split(last, "-+", 3);
                repatch = Integer.parseInt(split2[0]);
                other = last.substring(split2[0].length());
            } else {
                String last = split[2];
                String[] split2 = StringUtils.split(last, "-+", 2);
                patch = Integer.parseInt(split2[0]);
                repatch = 0;
                other = last.substring(split2[0].length());
            }
        }

        @Override
        public String toString() {
            if (repatch > 0) {
                return major + "." + minor + "." + patch + "." + repatch + other;
            } else {
                return major + "." + minor + "." + patch + other;
            }
        }

        @Override
        public int compareTo(Version o) {
            return COMPARATOR.compare(this, o);
        }

        public int getMajor() {
            return major;
        }

        public int getMinor() {
            return minor;
        }

        public int getPatch() {
            return patch;
        }

        public int getRepatch() {
            return repatch;
        }

        public String getOther() {
            return other;
        }
    }

}
