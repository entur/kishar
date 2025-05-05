package org.entur.kishar.gtfsrt.mappers;

import com.google.transit.realtime.GtfsRealtime;
import org.entur.avro.realtime.siri.model.TranslatedStringRecord;

import java.time.Instant;
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
        return Instant.parse(timestamp);
    }

}
