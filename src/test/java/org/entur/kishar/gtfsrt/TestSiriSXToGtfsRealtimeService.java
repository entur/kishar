package org.entur.kishar.gtfsrt;

import com.google.common.collect.Maps;
import com.google.transit.realtime.GtfsRealtime;
import org.entur.avro.realtime.siri.model.ServiceDeliveryRecord;
import org.entur.avro.realtime.siri.model.SiriRecord;
import org.entur.avro.realtime.siri.model.SituationExchangeDeliveryRecord;
import org.entur.kishar.gtfsrt.domain.GtfsRtData;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertFalse;
import static junit.framework.TestCase.assertNotNull;
import static junit.framework.TestCase.assertTrue;
import static org.entur.kishar.gtfsrt.Helper.createPtSituationElement;
import static org.entur.kishar.gtfsrt.TestAlertFactory.assertAlert;
import static org.mockito.Mockito.when;

public class TestSiriSXToGtfsRealtimeService extends SiriToGtfsRealtimeServiceTest {

    @Test
    public void testSituationToAlert() throws IOException {
        SiriRecord siri = createSiriSx("RUT");

        Map<String, byte[]> redisMap = getRedisMap(rtService, siri);

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

    private Map<String, byte[]> getRedisMap(SiriToGtfsRealtimeService rtService, SiriRecord siri) {
        Map<String, GtfsRtData> gtfsRt = rtService.convertSiriToGtfsRt(siri);
        Map<String, byte[]> redisMap = Maps.newHashMap();
        for (String key : gtfsRt.keySet()) {
            byte[] data = gtfsRt.get(key).getData();
            redisMap.put(key, data);
        }
        return redisMap;
    }

    @Test
    public void testSituationToAlertWithDatasourceFiltering() throws IOException {

        String datasource = "BNR";
        SiriRecord siri = createSiriSx(datasource);

        Map<String, byte[]> redisMap = getRedisMap(rtService, siri);

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
        String datasource = "RUT";

        SiriRecord siri = createSiriSx(datasource);

        Map<String, GtfsRtData> result = rtService.convertSiriToGtfsRt(siri);

        assertFalse(result.isEmpty());
    }

    private SiriRecord createSiriSx(String datasource) {
        SituationExchangeDeliveryRecord sxDelivery = SituationExchangeDeliveryRecord.newBuilder()
                .setSituations(List.of(createPtSituationElement(datasource)))
                .build();


        ServiceDeliveryRecord serviceDelivery = ServiceDeliveryRecord.newBuilder()
                .setSituationExchangeDeliveries(List.of(sxDelivery))
                .build();

        return SiriRecord.newBuilder()
                .setServiceDelivery(serviceDelivery)
                .build();
    }
}
