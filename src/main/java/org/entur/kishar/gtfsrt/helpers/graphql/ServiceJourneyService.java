package org.entur.kishar.gtfsrt.helpers.graphql;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import org.entur.kishar.gtfsrt.helpers.graphql.model.ServiceJourney;
import org.entur.kishar.metrics.PrometheusMetricsService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClientException;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

@Service
public class ServiceJourneyService {

    private static final Logger LOG = LoggerFactory.getLogger(ServiceJourneyService.class.getName());

    @Autowired
    private JourneyPlannerGraphQLClient graphQLClient;

    @Autowired
    private PrometheusMetricsService metricsService;

    @Value("${vehicle.serviceJourney.concurrent.requests:2}")
    private int concurrentRequests;

    @Value("${vehicle.serviceJourney.concurrent.sleeptime:50}")
    private int sleepTime;

    private ExecutorService asyncExecutorService;

    private boolean initialized = false;

    private AtomicInteger concurrentDatedServiceJourneyRequestCounter = new AtomicInteger();

    public ServiceJourneyService() {

        if (concurrentRequests < 1) {
            concurrentRequests = 1;
        }
        asyncExecutorService = Executors.newFixedThreadPool(concurrentRequests);

    }
    private LoadingCache<String, ServiceJourney> datedServiceJourneyCache = CacheBuilder.newBuilder()
            .expireAfterAccess(1, TimeUnit.HOURS)
            .build(new CacheLoader<>() {
                @Override
                public ServiceJourney load(String datedServiceJourneyId) {
                    return lookupDatedServiceJourney(datedServiceJourneyId);
                }
            });

    private AtomicInteger initCounter = new AtomicInteger();

    public ServiceJourney getServiceJourney(String datedServiceJourneyId) {
        try {
        return datedServiceJourneyCache.get(datedServiceJourneyId);
        } catch (ExecutionException e) {
            return null;
        }
    }

    private ServiceJourney lookupDatedServiceJourney(String datedServiceJourneyId) {
        // No need to attempt lookup if id does not match pattern
        ServiceJourney serviceJourney = new ServiceJourney();
        if (datedServiceJourneyId.contains(":DatedServiceJourney:")) {

            String query = "{\"query\":\"{datedServiceJourney(id:\\\"" + datedServiceJourneyId + "\\\"){id operatingDay serviceJourney {id }}}\",\"variables\":null}";

            try {
                Data data = graphQLClient.executeQuery(query);

                if (data != null &&
                        data.datedServiceJourney != null &&
                        data.datedServiceJourney.getServiceJourney() != null) {

                    serviceJourney = data.datedServiceJourney.getServiceJourney();
                    serviceJourney.setDate(data.datedServiceJourney.getOperatingDay());

                    datedServiceJourneyCache.put(datedServiceJourneyId, serviceJourney);
                }

            } catch (WebClientException e) {
                // Ignore - return empty ServiceJourney
            }
        } else {
            //Dummy fallback
            serviceJourney.setId(datedServiceJourneyId);
        }

        return serviceJourney;
    }
}
