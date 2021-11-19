package org.entur.kishar.gtfsrt.helpers;

import com.google.transit.realtime.GtfsRealtime;
import uk.org.siri.www.siri.DefaultedTextStructure;

import java.util.List;

public class GtfsRealtimeLibrary {
    public static GtfsRealtime.FeedMessage.Builder createFeedMessageBuilder() {
        long now = System.currentTimeMillis();
        com.google.transit.realtime.GtfsRealtime.FeedHeader.Builder header = GtfsRealtime.FeedHeader.newBuilder();
        header.setTimestamp(now / 1000L);
        header.setIncrementality(GtfsRealtime.FeedHeader.Incrementality.FULL_DATASET);
        header.setGtfsRealtimeVersion("1.0");
        GtfsRealtime.FeedMessage.Builder feedMessageBuilder = GtfsRealtime.FeedMessage.newBuilder();
        feedMessageBuilder.setHeader(header);
        return feedMessageBuilder;
    }

    public static GtfsRealtime.TranslatedString translation(List<uk.org.siri.www.siri.DefaultedTextStructure> textStructures) {
        if (textStructures == null) {
            return null;
        }

        if (!textStructures.isEmpty()) {
            GtfsRealtime.TranslatedString.Builder tsBuilder = GtfsRealtime.TranslatedString.newBuilder();
            for (DefaultedTextStructure text : textStructures) {

                String value = text.getValue();
                if (value == null || value.isBlank()) {
                    continue;
                }

                value = value.replaceAll("\\s+", " ");

                GtfsRealtime.TranslatedString.Translation.Builder translation = GtfsRealtime.TranslatedString.Translation.newBuilder();
                translation.setText(value);
                if (text.getLang() != null) {
                    final String languageTypeStr = text.getLang().toString();

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
}
