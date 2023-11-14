package com.github.aayushjn.keyvaluestore.util;

public class StringUtils {
    private StringUtils() {}

    public static boolean hasPrefix(String s, String prefix, boolean ignoreCase) {
        return s.regionMatches(ignoreCase, 0, prefix, 0, prefix.length());
    }
}
