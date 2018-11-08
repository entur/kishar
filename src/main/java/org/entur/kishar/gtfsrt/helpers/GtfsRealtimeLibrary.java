package org.entur.kishar.gtfsrt.helpers;

import com.google.transit.realtime.GtfsRealtime;
import uk.org.siri.siri20.DefaultedTextStructure;

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

    public static GtfsRealtime.TranslatedString translation(List<DefaultedTextStructure> textStructures) {
        if (textStructures == null) {
            return null;
        }

        if (textStructures.size() >= 1) {
            DefaultedTextStructure text = textStructures.get(0);
            String value = text.getValue();
            if (value == null) {
                return null;
            }

            value = value.replaceAll("\\s+", " ");

            GtfsRealtime.TranslatedString.Translation.Builder translation = GtfsRealtime.TranslatedString.Translation.newBuilder();
            translation.setText(value);
            if (text.getLang() != null) {
                translation.setLanguage(text.getLang());
            }

            GtfsRealtime.TranslatedString.Builder tsBuilder = GtfsRealtime.TranslatedString.newBuilder();
            tsBuilder.addTranslation(translation);
            return tsBuilder.build();
        }
        return null;
    }
}
