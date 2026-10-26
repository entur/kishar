package org.entur.kishar.gtfsrt.mappers;

import com.google.transit.realtime.GtfsRealtime;
import org.entur.avro.realtime.siri.model.TranslatedStringRecord;

import java.time.Instant;
import java.time.LocalDate;
import java.time.Month;
import java.time.Year;
import java.util.List;

public class AvroHelper {
    public static GtfsRealtime.TranslatedString translation(List<TranslatedStringRecord> textStructures) {
        if (textStructures == null) {
            return null;
        }

        if (!textStructures.isEmpty()) {
            GtfsRealtime.TranslatedString.Builder tsBuilder = GtfsRealtime.TranslatedString.newBuilder();
            for (TranslatedStringRecord text : textStructures) {

                String value = text.getValue().toString();
                if (value.isBlank()) {
                    continue;
                }

                value = value.replaceAll("\\s+", " ");

                GtfsRealtime.TranslatedString.Translation.Builder translation = GtfsRealtime.TranslatedString.Translation.newBuilder();
                translation.setText(value);
                if (text.getLanguage() != null) {
                    final String languageTypeStr = text.getLanguage().toString();

                    if (languageTypeStr.startsWith("LANG_TYPE_")) {
                        String language = languageTypeStr
                                .substring("LANG_TYPE_".length())
                                .toLowerCase();

                        translation.setLanguage(language);
                    }
                }

                tsBuilder.addTranslation(translation);
            }
            if (tsBuilder.getTranslationCount() > 0) {
                return tsBuilder.build();
            }
        }
        return null;
    }

    public static Instant getInstant(CharSequence timestamp) {
        Instant instant = parseOffsetDateTime(timestamp);
        return instant != null ? instant : Instant.parse(timestamp);
    }

    /**
     * Fast path for the timestamp format used in SIRI data: yyyy-MM-ddTHH:mm:ss[.S{1,9}](Z|±HH:MM).
     * Instant.parse goes through DateTimeFormatter, which is slow and allocates heavily. Returns null for
     * anything outside the fast path, so that the caller can fall back to Instant.parse.
     */
    private static Instant parseOffsetDateTime(CharSequence s) {
        int length = s.length();
        if (length < 20 || s.charAt(4) != '-' || s.charAt(7) != '-' || s.charAt(10) != 'T'
                || s.charAt(13) != ':' || s.charAt(16) != ':') {
            return null;
        }
        int year = digits(s, 0, 4);
        int month = digits(s, 5, 2);
        int day = digits(s, 8, 2);
        int hour = digits(s, 11, 2);
        int minute = digits(s, 14, 2);
        int second = digits(s, 17, 2);
        if (year < 0 || month < 1 || month > 12 || day < 1 || day > Month.of(month).length(Year.isLeap(year))
                || hour < 0 || hour > 23 || minute < 0 || minute > 59 || second < 0 || second > 59) {
            return null;
        }

        int pos = 19;
        int nanos = 0;
        if (s.charAt(pos) == '.') {
            int start = ++pos;
            while (pos < length && pos - start < 10 && isDigit(s.charAt(pos))) {
                nanos = nanos * 10 + (s.charAt(pos) - '0');
                pos++;
            }
            int fractionDigits = pos - start;
            if (fractionDigits == 0 || fractionDigits > 9) {
                return null;
            }
            for (int i = fractionDigits; i < 9; i++) {
                nanos *= 10;
            }
        }

        int offsetSeconds;
        if (pos == length - 1 && s.charAt(pos) == 'Z') {
            offsetSeconds = 0;
        } else if (pos == length - 6 && (s.charAt(pos) == '+' || s.charAt(pos) == '-') && s.charAt(pos + 3) == ':') {
            int offsetHours = digits(s, pos + 1, 2);
            int offsetMinutes = digits(s, pos + 4, 2);
            if (offsetHours < 0 || offsetHours > 18 || offsetMinutes < 0 || offsetMinutes > 59) {
                return null;
            }
            offsetSeconds = offsetHours * 3600 + offsetMinutes * 60;
            if (s.charAt(pos) == '-') {
                offsetSeconds = -offsetSeconds;
            }
        } else {
            return null;
        }

        long epochDay = LocalDate.of(year, month, day).toEpochDay();
        long epochSecond = epochDay * 86400 + hour * 3600 + minute * 60 + second - offsetSeconds;
        return Instant.ofEpochSecond(epochSecond, nanos);
    }

    private static int digits(CharSequence s, int start, int count) {
        int value = 0;
        for (int i = start; i < start + count; i++) {
            char c = s.charAt(i);
            if (!isDigit(c)) {
                return -1;
            }
            value = value * 10 + (c - '0');
        }
        return value;
    }

    private static boolean isDigit(char c) {
        return c >= '0' && c <= '9';
    }

}
