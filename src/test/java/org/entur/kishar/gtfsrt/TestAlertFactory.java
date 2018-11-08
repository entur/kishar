package org.entur.kishar.gtfsrt;

import com.google.transit.realtime.GtfsRealtime;
import org.junit.Before;
import org.junit.Test;
import uk.org.siri.siri20.DefaultedTextStructure;
import uk.org.siri.siri20.PtSituationElement;
import uk.org.siri.siri20.SituationNumber;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertNotNull;
import static org.entur.kishar.gtfsrt.Helper.createPtSituationElement;
import static org.entur.kishar.gtfsrt.Helper.descriptionValue;
import static org.entur.kishar.gtfsrt.Helper.summaryValue;

public class TestAlertFactory {
    AlertFactory alertFactory;

    @Before
    public void init() {
        alertFactory = new AlertFactory();
    }

    @Test
    public void testCreateAlertFromSituation() {

        PtSituationElement ptSituation = createPtSituationElement();


        GtfsRealtime.Alert.Builder alertBuilder = alertFactory.createAlertFromSituation(ptSituation);
        assertNotNull(alertBuilder);
        GtfsRealtime.Alert alert = alertBuilder.build();
        assertNotNull(alert);

        assertAlert(alert);

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
