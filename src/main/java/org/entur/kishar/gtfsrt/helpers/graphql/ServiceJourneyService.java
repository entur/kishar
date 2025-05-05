package org.entur.kishar.gtfsrt.helpers.graphql;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import org.entur.kishar.gtfsrt.helpers.graphql.model.ServiceJourney;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClientException;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

@Service
public class ServiceJourneyService {

    private static final Logger LOG = LoggerFactory.getLogger(ServiceJourneyService.class.getName());

    @Autowired
    private JourneyPlannerGraphQLClient graphQLClient;

    public ServiceJourneyService() { }

    private final LoadingCache<String, ServiceJourney> datedServiceJourneyCache = CacheBuilder.newBuilder()
            .expireAfterAccess(1, TimeUnit.HOURS)
            .build(new CacheLoader<>() {
                @Override
                public ServiceJourney load(String datedServiceJourneyId) {
                    return lookupDatedServiceJourney(datedServiceJourneyId);
                }
            });

    public ServiceJourney getServiceJourneyFromDatedServiceJourney(String datedServiceJourneyId) {
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
                LOG.info("Failed to fetch DatedServiceJourney from GraphQL", e);
            }
        } else {
            //Dummy fallback
            serviceJourney.setId(datedServiceJourneyId);
        }

        return serviceJourney;
    }
}
