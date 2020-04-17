package org.entur.kishar.gtfsrt;

import com.google.common.collect.Maps;
import com.google.transit.realtime.GtfsRealtime;
import org.junit.Test;
import uk.org.siri.www.siri.ServiceDeliveryType;
import uk.org.siri.www.siri.SiriType;
import uk.org.siri.www.siri.SituationExchangeDeliveryStructure;

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
        SiriType siri = createSiriSx("RUT");

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

    private Map<byte[], byte[]> getRedisMap(SiriToGtfsRealtimeService rtService, SiriType siri) {
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

        String datasource = "BNR";
        SiriType siri = createSiriSx(datasource);

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
        String datasource = "RUT";

        SiriType siri = createSiriSx(datasource);

        Map<byte[], byte[]> result = rtService.convertSiriSxToGtfsRt(siri);

        assertFalse(result.isEmpty());
    }

    private SiriType createSiriSx(String datasource) {
        SituationExchangeDeliveryStructure.SituationsType situations = SituationExchangeDeliveryStructure.SituationsType.newBuilder()
                .addPtSituationElement(createPtSituationElement(datasource))
                .build();

        SituationExchangeDeliveryStructure sxDelivery = SituationExchangeDeliveryStructure.newBuilder()
                .setSituations(situations)
                .build();

        ServiceDeliveryType serviceDelivery = ServiceDeliveryType.newBuilder()
                .addSituationExchangeDelivery(sxDelivery)
                .build();

        return SiriType.newBuilder()
                .setServiceDelivery(serviceDelivery)
                .build();
    }
}
