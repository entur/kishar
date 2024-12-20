package org.entur.kishar.gtfsrt;

import com.google.common.collect.Maps;
import com.google.transit.realtime.GtfsRealtime;
import org.entur.avro.realtime.siri.model.SiriRecord;
import org.entur.kishar.gtfsrt.domain.GtfsRtData;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertFalse;
import static junit.framework.TestCase.assertNotNull;
import static junit.framework.TestCase.assertTrue;
import static org.entur.kishar.gtfsrt.Helper.descriptionValue;
import static org.entur.kishar.gtfsrt.Helper.situationNumberValue;
import static org.entur.kishar.gtfsrt.Helper.summaryValue;
import static org.entur.kishar.gtfsrt.TestAlertFactory.assertAlert;

public class TestSiriSXToGtfsRealtimeService extends SiriToGtfsRealtimeServiceTest {

    @Test
    public void testSituationToAlert() throws IOException {
        SiriRecord siri = createSiriRecord(getXml());

        redisService.writeGtfsRt(rtService.convertSiriToGtfsRt(siri), RedisService.Type.ALERT);

        rtService.registerGtfsRtAlerts(rtService.convertSiriToGtfsRt(siri));

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

    private String getXml() {
        return getXml("TST");
    }
    private String getXml(String codespaceId) {
        return "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>" +
                "<Siri version=\"2.1\" xmlns=\"http://www.siri.org.uk/siri\" xmlns:ns2=\"http://www.ifopt.org.uk/acsb\" xmlns:ns3=\"http://www.ifopt.org.uk/ifopt\" xmlns:ns4=\"http://datex2.eu/schema/2_0RC1/2_0\" xmlns:ns5=\"http://www.opengis.net/gml/3.2\">" +
                "  <ServiceDelivery>" +
                "    <ResponseTimestamp>2023-01-09T13:04:40.855687+01:00</ResponseTimestamp>" +
                "    <ProducerRef>TST</ProducerRef>" +
                "    <MoreData>false</MoreData>" +
                "    <SituationExchangeDelivery version=\"2.1\">" +
                "      <ResponseTimestamp>2023-01-09T13:04:40.855732+01:00</ResponseTimestamp>" +
                "      <Situations>" +
                "        <PtSituationElement>" +
                "          <CreationTime>2022-12-27T09:03:43+01:00</CreationTime>" +
                "          <ParticipantRef>" + codespaceId + "</ParticipantRef>" +
                "          <SituationNumber>" + situationNumberValue + "</SituationNumber>" +
                "          <Version>1</Version>" +
                "          <Source>" +
                "            <SourceType>directReport</SourceType>" +
                "          </Source>" +
                "          <Progress>closed</Progress>" +
                "          <ValidityPeriod>" +
                "            <StartTime>2023-01-13T00:00:00+01:00</StartTime>" +
                "            <EndTime>2223-01-13T06:00:00+01:00</EndTime>" +
                "          </ValidityPeriod>" +
                "          <UndefinedReason/>" +
                "          <Priority>10</Priority>" +
                "          <ReportType>general</ReportType>" +
                "          <Keywords/>" +
                "          <Summary xml:lang=\"NO\">" + summaryValue + "</Summary>" +
                "          <Description xml:lang=\"NO\">" + descriptionValue + "</Description>" +
                "          <Affects/>" +
                "        </PtSituationElement>" +
                "      </Situations>" +
                "    </SituationExchangeDelivery>" +
                "  </ServiceDelivery>" +
                "</Siri>";
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
    public void testSituationToAlertWithDatasourceFiltering() {


        SiriRecord siri = createSiriRecord(getXml("ABC"));

        redisService.writeGtfsRt(rtService.convertSiriToGtfsRt(siri), RedisService.Type.ALERT);

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

        SiriRecord siri = createSiriRecord(getXml());

        Map<String, GtfsRtData> result = rtService.convertSiriToGtfsRt(siri);
        System.out.println(result.values());
        assertFalse(result.isEmpty());
    }

}
