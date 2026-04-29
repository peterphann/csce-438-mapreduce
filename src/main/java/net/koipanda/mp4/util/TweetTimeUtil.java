package net.koipanda.mp4.util;

import java.util.OptionalInt;

public class TweetTimeUtil {

    private TweetTimeUtil() {}

    public static OptionalInt extractHourFromTimeLine(String timeLine) {
        if (timeLine == null) return OptionalInt.empty();
        String line = timeLine.trim();
        if (!line.startsWith("T")) return OptionalInt.empty();

        String[] parts = line.split("\\s+");
        if (parts.length < 3) return OptionalInt.empty();

        String[] timePieces = parts[2].split(":");
        if (timePieces.length < 1) return OptionalInt.empty();

        try {
            return OptionalInt.of(Integer.parseInt(timePieces[0]));
        } catch (NumberFormatException e) {
            return OptionalInt.empty();
        }
    }

    public static String toHourBucketLabel(int hour) {
        String h = String.format("%02d", hour);
        return h + ":00 - " + h + ":59";
    }

}
