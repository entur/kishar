package org.entur.kishar.gtfsrt;

import com.google.transit.realtime.GtfsRealtime;
import org.entur.kishar.App;
import org.entur.kishar.gtfsrt.helpers.GtfsRealtimeLibrary;
import org.junit.After;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import uk.org.siri.siri20.Siri;

import java.util.HashMap;
import java.util.List;


@RunWith(SpringJUnit4ClassRunner.class)
@Configuration
@SpringBootTest(webEnvironment= SpringBootTest.WebEnvironment.MOCK, classes = App.class)
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

    protected SiriToGtfsRealtimeService rtService;

    @Mock
    protected RedisService redisService;

    @Before
    public void before() {
        redisService = Mockito.mock(RedisService.class);
        rtService = new SiriToGtfsRealtimeService(new AlertFactory(),
                redisService,
                datasourceETWhitelist,
                datasourceVMWhitelist,
                datasourceSXWhitelist,
                closeToNextStopPercentage,
                closeToNextStopDistance);
    }

    @After
    public void cleanup() {
        //Deletes all received data
        rtService.setAlerts(GtfsRealtimeLibrary.createFeedMessageBuilder().build(), new HashMap<>());
        rtService.setVehiclePositions(GtfsRealtimeLibrary.createFeedMessageBuilder().build(), new HashMap<>());
        rtService.setTripUpdates(GtfsRealtimeLibrary.createFeedMessageBuilder().build(), new HashMap<>());
    }
}
