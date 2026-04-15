package com.anomaly.streams;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
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

    private static final Set<String> STOPWORDS = new HashSet<>(Arrays.asList(
        "a", "an", "the", "and", "or", "but", "in", "on", "at", "to", "for",
        "of", "with", "by", "from", "as", "is", "was", "are", "were", "be",
        "been", "being", "have", "has", "had", "do", "does", "did", "will",
        "would", "could", "should", "may", "might", "shall", "can", "not",
        "no", "nor", "so", "yet", "both", "either", "neither", "each", "few",
        "more", "most", "other", "some", "such", "than", "too", "very",
        "just", "that", "this", "these", "those", "it", "its", "i", "me",
        "my", "we", "our", "you", "your", "he", "she", "they", "them",
        "his", "her", "their", "what", "which", "who", "whom", "how", "when",
        "where", "why", "all", "any", "if", "then", "there", "here", "up",
        "out", "about", "into", "through", "during", "before", "after",
        "above", "below", "between", "own", "same", "s", "t", "re", "ll",
        "ve", "d", "m"
    ));

    private static final int MIN_TOKEN_LENGTH = 3;

    public String clean(String rawText) {
        if (rawText == null || rawText.isEmpty()) return "";

        String text = rawText.toLowerCase();
        text = URL_PATTERN.matcher(text).replaceAll(" ");
        text = EMAIL_PATTERN.matcher(text).replaceAll(" ");
        text = NON_ALPHA_PATTERN.matcher(text).replaceAll(" ");
        text = WHITESPACE_PATTERN.matcher(text).replaceAll(" ").trim();
        return text;
    }

    public List<String> tokenize(String cleanedText) {
        if (cleanedText == null || cleanedText.isEmpty()) {
            return new ArrayList<>();
        }

        List<String> tokens = new ArrayList<>();
        for (String token : cleanedText.split(" ")) {
            if (token.length() >= MIN_TOKEN_LENGTH && !STOPWORDS.contains(token)) {
                tokens.add(token);
            }
        }
        return tokens;
    }

    public List<String> process(String rawText) {
        return tokenize(clean(rawText));
    }
}