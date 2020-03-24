package org.entur.kishar.gtfsrt;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.transit.realtime.GtfsRealtime;
import org.junit.Before;
import org.junit.Test;
import uk.org.siri.siri20.ServiceDelivery;
import uk.org.siri.siri20.Siri;
import uk.org.siri.siri20.SituationExchangeDeliveryStructure;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static junit.framework.TestCase.*;
import static org.entur.kishar.gtfsrt.Helper.createPtSituationElement;
import static org.entur.kishar.gtfsrt.TestAlertFactory.assertAlert;
import static org.mockito.Mockito.when;

public class TestSiriSXToGtfsRealtimeService extends SiriToGtfsRealtimeServiceTest {

    @Test
    public void testSituationToAlert() throws IOException {

        Siri siri = new Siri();
        ServiceDelivery serviceDelivery = new ServiceDelivery();
        SituationExchangeDeliveryStructure sxDelivery = new SituationExchangeDeliveryStructure();
        sxDelivery.setSituations(new SituationExchangeDeliveryStructure.Situations());
        sxDelivery.getSituations().getPtSituationElements().add(createPtSituationElement("RUT"));
        serviceDelivery.getSituationExchangeDeliveries().add(sxDelivery);
        siri.setServiceDelivery(serviceDelivery);

        Map<byte[], byte[]> redisMap = getRedisMap(rtService, siri);

        when(redisService.readGtfsRtMap(RedisService.Type.ALERT)).thenReturn(redisMap);
        rtService.writeOutput();
        Object alerts = rtService.getAlerts("application/json", null);
        assertNotNull(alerts);
        assertTrue(alerts instanceof GtfsRealtime.FeedMessage);

        GtfsRealtime.FeedMessage feedMessage = (GtfsRealtime.FeedMessage) alerts;
        List<GtfsRealtime.FeedEntity> entityList = feedMessage.getEntityList();
        assertFalse(entityList.isEmpty());

        GtfsRealtime.FeedMessage byteArrayFeedMessage = GtfsRealtime.FeedMessage.parseFrom((byte[]) rtService.getAlerts(null, null));
        assertEquals(feedMessage, byteArrayFeedMessage);

        GtfsRealtime.FeedEntity entity = feedMessage.getEntity(0);
        assertNotNull(entity);
        GtfsRealtime.Alert alert = entity.getAlert();
        assertNotNull(alert);

        assertAlert(alert);
    }

    private Map<byte[], byte[]> getRedisMap(SiriToGtfsRealtimeService rtService, Siri siri) {
        Map<byte[], byte[]> gtfsRt = rtService.convertSiriSxToGtfsRt(siri);
        Map<byte[], byte[]> redisMap = Maps.newHashMap();
        for (byte[] key : gtfsRt.keySet()) {
            byte[] data = gtfsRt.get(key);
            byte[] dataInBytes = new byte[data.length + 16];
            System.arraycopy(data, 0, dataInBytes, 16, data.length);
            redisMap.put(key, dataInBytes);
        }
        return redisMap;
    }

    @Test
    public void testSituationToAlertWithDatasourceFiltering() throws IOException {

        Siri siri = new Siri();
        ServiceDelivery serviceDelivery = new ServiceDelivery();
        SituationExchangeDeliveryStructure sxDelivery = new SituationExchangeDeliveryStructure();
        sxDelivery.setSituations(new SituationExchangeDeliveryStructure.Situations());
        sxDelivery.getSituations().getPtSituationElements().add(createPtSituationElement("BNR"));
        serviceDelivery.getSituationExchangeDeliveries().add(sxDelivery);
        siri.setServiceDelivery(serviceDelivery);

        Map<byte[], byte[]> redisMap = getRedisMap(rtService, siri);

        when(redisService.readGtfsRtMap(RedisService.Type.ALERT)).thenReturn(redisMap);
        rtService.writeOutput();
        Object alerts = rtService.getAlerts("application/json", null);
        assertNotNull(alerts);
        assertTrue(alerts instanceof GtfsRealtime.FeedMessage);

        GtfsRealtime.FeedMessage feedMessage = (GtfsRealtime.FeedMessage) alerts;
        List<GtfsRealtime.FeedEntity> entityList = feedMessage.getEntityList();
        assertTrue(entityList.isEmpty());
    }

    @Test
    public void testMappingOfSiriSx() {

        Siri siri = new Siri();
        ServiceDelivery serviceDelivery = new ServiceDelivery();
        SituationExchangeDeliveryStructure sxDelivery = new SituationExchangeDeliveryStructure();
        sxDelivery.setSituations(new SituationExchangeDeliveryStructure.Situations());
        sxDelivery.getSituations().getPtSituationElements().add(createPtSituationElement("RUT"));
        serviceDelivery.getSituationExchangeDeliveries().add(sxDelivery);
        siri.setServiceDelivery(serviceDelivery);

        Map<byte[], byte[]> result = rtService.convertSiriSxToGtfsRt(siri);

        assertFalse(result.isEmpty());
    }
}
