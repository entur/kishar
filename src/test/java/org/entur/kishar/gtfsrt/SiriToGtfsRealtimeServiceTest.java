package org.entur.kishar.gtfsrt;

import com.google.transit.realtime.GtfsRealtime;
import org.entur.kishar.App;
import org.entur.kishar.gtfsrt.helpers.GtfsRealtimeLibrary;
import org.junit.After;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.HashMap;


@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest(webEnvironment= SpringBootTest.WebEnvironment.MOCK, classes = App.class)
public abstract class SiriToGtfsRealtimeServiceTest {

    @Value("${kishar.settings.vm.close.to.stop.percentage}")
    int NEXT_STOP_PERCENTAGE;

    @Value("${kishar.settings.vm.close.to.stop.distance}")
    int NEXT_STOP_DISTANCE;

    @Autowired
    protected SiriToGtfsRealtimeService rtService;

    @After
    public void cleanup() {
        //Deletes all received data
        rtService.setAlerts(GtfsRealtimeLibrary.createFeedMessageBuilder().build(), new HashMap<>());
        rtService.alertDataById.clear();
        rtService.setVehiclePositions(GtfsRealtimeLibrary.createFeedMessageBuilder().build(), new HashMap<>());
        rtService.dataByVehicle.clear();
        rtService.setTripUpdates(GtfsRealtimeLibrary.createFeedMessageBuilder().build(), new HashMap<>());
        rtService.dataByTimetable.clear();
    }
}
