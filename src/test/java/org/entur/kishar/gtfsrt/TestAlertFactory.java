package org.entur.kishar.gtfsrt;

import com.google.transit.realtime.GtfsRealtime;
import org.entur.avro.realtime.siri.converter.jaxb2avro.Jaxb2AvroConverter;
import org.entur.avro.realtime.siri.model.PtSituationElementRecord;
import org.junit.Before;
import org.junit.Test;
import org.junit.jupiter.api.BeforeEach;
import uk.org.siri.siri21.HalfOpenTimestampOutputRangeStructure;
import uk.org.siri.siri21.InfoLinkStructure;
import uk.org.siri.siri21.PtSituationElement;

import java.time.ZonedDateTime;

import static org.entur.kishar.gtfsrt.Helper.createPtSituationElement;
import static org.entur.kishar.gtfsrt.Helper.descriptionValue;
import static org.entur.kishar.gtfsrt.Helper.summaryValue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestAlertFactory {
    AlertFactory alertFactory;

    @Before
    @BeforeEach
    public void init() {
        alertFactory = new AlertFactory();
    }

    @Test
    public void testCreateAlertFromSituation() {

        PtSituationElementRecord ptSituation = createPtSituationElement("RUT");


        GtfsRealtime.Alert.Builder alertBuilder = alertFactory.createAlertFromSituation(ptSituation);
        assertNotNull(alertBuilder);
        GtfsRealtime.Alert alert = alertBuilder.build();
        assertNotNull(alert);

        assertAlert(alert);

    }

    @org.junit.jupiter.api.Test
    public void testOpenEndedValidityPeriodGetsSyntheticEndTime() {
        PtSituationElement siriSituation = Helper.createBasicPtSituationElement("RUT");

        HalfOpenTimestampOutputRangeStructure validityPeriod = new HalfOpenTimestampOutputRangeStructure();
        validityPeriod.setStartTime(ZonedDateTime.now());
        // endTime deliberately left null (open-ended)
        siriSituation.getValidityPeriods().add(validityPeriod);

        PtSituationElementRecord ptSituation = Jaxb2AvroConverter.convert(siriSituation);

        GtfsRealtime.Alert.Builder alertBuilder = alertFactory.createAlertFromSituation(ptSituation);
        GtfsRealtime.Alert alert = alertBuilder.build();

        assertEquals(1, alert.getActivePeriodCount());
        assertTrue(alert.getActivePeriod(0).hasEnd(),
                "Open-ended alert should get a synthetic end time to prevent alerts being valid forever");
    }

    @org.junit.jupiter.api.Test
    public void testNullUriInInfoLinkIsSkipped() {
        PtSituationElement siriSituation = Helper.createBasicPtSituationElement("RUT");

        PtSituationElement.InfoLinks infoLinks = new PtSituationElement.InfoLinks();
        InfoLinkStructure infoLinkWithNullUri = new InfoLinkStructure();
        // uri deliberately left null
        infoLinks.getInfoLinks().add(infoLinkWithNullUri);
        siriSituation.setInfoLinks(infoLinks);

        PtSituationElementRecord ptSituation = Jaxb2AvroConverter.convert(siriSituation);

        GtfsRealtime.Alert.Builder alertBuilder = alertFactory.createAlertFromSituation(ptSituation);
        GtfsRealtime.Alert alert = alertBuilder.build();

        assertFalse(alert.hasUrl(), "Alert with null URI InfoLink should have no URL set");
    }

    @org.junit.jupiter.api.Test
    public void testOnlyFirstValidUrlIsUsed() {
        PtSituationElement siriSituation = Helper.createBasicPtSituationElement("RUT");

        PtSituationElement.InfoLinks infoLinks = new PtSituationElement.InfoLinks();
        InfoLinkStructure link1 = new InfoLinkStructure();
        link1.setUri("https://example.com/first");
        InfoLinkStructure link2 = new InfoLinkStructure();
        link2.setUri("https://example.com/second");
        infoLinks.getInfoLinks().add(link1);
        infoLinks.getInfoLinks().add(link2);
        siriSituation.setInfoLinks(infoLinks);

        PtSituationElementRecord ptSituation = Jaxb2AvroConverter.convert(siriSituation);

        GtfsRealtime.Alert.Builder alertBuilder = alertFactory.createAlertFromSituation(ptSituation);
        GtfsRealtime.Alert alert = alertBuilder.build();

        assertNotNull(alert.getUrl());
        assertEquals(1, alert.getUrl().getTranslationCount(),
                "URL should have exactly one translation entry");
        assertEquals("https://example.com/first", alert.getUrl().getTranslation(0).getText(),
                "Only the first URL should be used");
    }

    static void assertAlert(GtfsRealtime.Alert alert) {
        GtfsRealtime.TranslatedString headerText = alert.getHeaderText();
        assertNotNull(headerText);
        assertEquals(1, headerText.getTranslationCount());
        assertEquals(summaryValue, headerText.getTranslation(0).getText());


        GtfsRealtime.TranslatedString descriptionText = alert.getDescriptionText();
        assertNotNull(descriptionText);
        assertEquals(1, descriptionText.getTranslationCount());
        assertEquals(descriptionValue, descriptionText.getTranslation(0).getText());
    }
}
