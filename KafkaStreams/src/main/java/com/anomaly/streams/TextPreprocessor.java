package com.anomaly.streams;

import java.util.regex.Pattern;

public class TextPreprocessor {

    private static final Pattern URL_PATTERN =
        Pattern.compile("https?://\\S+|www\\.\\S+");

    private static final Pattern EMAIL_PATTERN =
        Pattern.compile("[a-zA-Z0-9._%+\\-]+@[a-zA-Z0-9.\\-]+\\.[a-zA-Z]{2,}");

    private static final Pattern NON_ALPHA_PATTERN =
        Pattern.compile("[^a-z\\s]");

    private static final Pattern WHITESPACE_PATTERN =
        Pattern.compile("\\s+");

    public String clean(String rawText) {
        if (rawText == null || rawText.isEmpty()) return "";

        String text = rawText.toLowerCase();
        text = URL_PATTERN.matcher(text).replaceAll(" ");
        text = EMAIL_PATTERN.matcher(text).replaceAll(" ");
        text = NON_ALPHA_PATTERN.matcher(text).replaceAll(" ");
        text = WHITESPACE_PATTERN.matcher(text).replaceAll(" ").trim();
        return text;
    }
}