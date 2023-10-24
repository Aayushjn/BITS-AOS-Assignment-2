package com.github.aayushjn.keyvaluestore.util;

public final class ConsoleColor {
    private ConsoleColor() {}

    private static final String FOREGROUND = "\033[38;2;";
    private static final String RESET = "\033[0m";

    public static String withForegroundColor(String text, int red, int green, int blue) {
        return FOREGROUND +
                red +
                ';' +
                green +
                ';' +
                blue +
                'm' +
                text+
                RESET;
    }
}
