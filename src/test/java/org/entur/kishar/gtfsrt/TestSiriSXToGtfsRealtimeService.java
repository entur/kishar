package org.entur.kishar.gtfsrt;

import com.google.transit.realtime.GtfsRealtime;
import org.junit.Before;
import org.junit.Test;
import org.onebusway.gtfs_realtime.exporter.GtfsRealtimeProviderImpl;
import uk.org.siri.siri20.ServiceDelivery;
import uk.org.siri.siri20.Siri;
import uk.org.siri.siri20.SituationExchangeDeliveryStructure;

import java.io.IOException;
import java.util.List;

import static junit.framework.TestCase.*;
import static org.entur.kishar.gtfsrt.Helper.createPtSituationElement;
import static org.entur.kishar.gtfsrt.TestAlertFactory.assertAlert;

public class TestSiriSXToGtfsRealtimeService {
    SiriToGtfsRealtimeService rtService;

    @Before
    public void init() {
        rtService = new SiriToGtfsRealtimeService(new AlertFactory());
    }

    @Test
    public void testSituationToAlert() throws IOException {

        Siri siri = new Siri();
        ServiceDelivery serviceDelivery = new ServiceDelivery();
        SituationExchangeDeliveryStructure sxDelivery = new SituationExchangeDeliveryStructure();
        sxDelivery.setSituations(new SituationExchangeDeliveryStructure.Situations());
        sxDelivery.getSituations().getPtSituationElements().add(createPtSituationElement());
        serviceDelivery.getSituationExchangeDeliveries().add(sxDelivery);
        siri.setServiceDelivery(serviceDelivery);

        rtService.processDelivery(siri);
        rtService.writeOutput();
        Object alerts = rtService.getAlerts("application/json");
        assertNotNull(alerts);
        assertTrue(alerts instanceof GtfsRealtime.FeedMessage);

        GtfsRealtime.FeedMessage feedMessage = (GtfsRealtime.FeedMessage) alerts;
        List<GtfsRealtime.FeedEntity> entityList = feedMessage.getEntityList();
        assertFalse(entityList.isEmpty());

        GtfsRealtime.FeedMessage byteArrayFeedMessage = GtfsRealtime.FeedMessage.parseFrom((byte[]) rtService.getAlerts(null));
        assertEquals(feedMessage, byteArrayFeedMessage);

        GtfsRealtime.FeedEntity entity = feedMessage.getEntity(0);
        assertNotNull(entity);
        GtfsRealtime.Alert alert = entity.getAlert();
        assertNotNull(alert);

        assertAlert(alert);
    }
}
