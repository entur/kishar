package org.entur.kishar.gtfsrt;

import org.entur.avro.realtime.siri.converter.jaxb2avro.Jaxb2AvroConverter;
import org.entur.avro.realtime.siri.model.SiriRecord;
import org.entur.kishar.App;
import org.entur.kishar.gtfsrt.helpers.GtfsRealtimeLibrary;
import org.entur.kishar.gtfsrt.helpers.graphql.ServiceJourneyService;
import org.entur.siri21.util.SiriXml;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Configuration;
import uk.org.siri.siri21.Siri;

import java.util.HashMap;
import java.util.List;

import static org.junit.jupiter.api.Assertions.fail;


@Configuration
@SpringBootTest(webEnvironment= SpringBootTest.WebEnvironment.RANDOM_PORT, classes = App.class)
public abstract class SiriToGtfsRealtimeServiceTest {

    @Value("${kishar.settings.vm.close.to.stop.percentage}")
    int NEXT_STOP_PERCENTAGE;

    @Value("${kishar.settings.vm.close.to.stop.distance}")
    int NEXT_STOP_DISTANCE;


    @Value("${kishar.datasource.et.whitelist}")
    List<String> datasourceETWhitelist;
    @Value("${kishar.datasource.vm.whitelist}")
    List<String> datasourceVMWhitelist;
    @Value("${kishar.datasource.sx.whitelist}")
    List<String> datasourceSXWhitelist;
    @Value("${kishar.settings.vm.close.to.stop.percentage}")
    int closeToNextStopPercentage;
    @Value("${kishar.settings.vm.close.to.stop.distance}")
    int closeToNextStopDistance;

    @Autowired
    protected SiriToGtfsRealtimeService rtService;

    @Autowired
    protected ServiceJourneyService serviceJourneyService;

    @Autowired
    protected RedisService redisService;

    @BeforeEach
    public void before() {
//        redisService = new RedisService(false, "localhost", "1234", "pass");
//        rtService = new SiriToGtfsRealtimeService(new AlertFactory(),
//                redisService,
//                serviceJourneyService,
//                datasourceETWhitelist,
//                datasourceVMWhitelist,
//                datasourceSXWhitelist,
//                closeToNextStopPercentage,
//                closeToNextStopDistance);
        rtService.setAlerts(GtfsRealtimeLibrary.createFeedMessageBuilder().build(), new HashMap<>());
        rtService.setVehiclePositions(GtfsRealtimeLibrary.createFeedMessageBuilder().build(), new HashMap<>());
        rtService.setTripUpdates(GtfsRealtimeLibrary.createFeedMessageBuilder().build(), new HashMap<>());

        redisService.resetAllData();
    }

    protected SiriRecord createSiriRecord(String xml) {
        try {
            Siri s = SiriXml.parseXml(xml);
            return Jaxb2AvroConverter.convert(s);
        } catch (Exception e) {
            fail();
        }
        return null;
    }
}
