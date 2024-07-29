import com.github.javafaker.Faker;
import io.gatling.javaapi.core.CoreDsl;
import io.gatling.javaapi.core.OpenInjectionStep;
import io.gatling.javaapi.core.ScenarioBuilder;
import io.gatling.javaapi.core.Simulation;
import io.gatling.javaapi.http.HttpDsl;
import io.gatling.javaapi.http.HttpProtocolBuilder;

import java.time.Duration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.stream.Stream;

import static io.gatling.javaapi.core.CoreDsl.*;
import static io.gatling.javaapi.http.HttpDsl.http;

public class KafkaEventPublisher extends Simulation {

    private static final HttpProtocolBuilder httpProtocolBuilder = httpProtocolBuilder();
    private static final Iterator<Map<String, Object>> feeder = buildFeeder();
    private static final ScenarioBuilder scenarioBuilder = buildPostScenario();
    private static final OpenInjectionStep openInjectionsStepBuilder = injectionBuilder();


    public KafkaEventPublisher() {
        setUp(scenarioBuilder
                .injectOpen(openInjectionsStepBuilder)
                .protocols(httpProtocolBuilder)).assertions(global().responseTime()
                .max()
                .lte(10000), global().successfulRequests()
                .percent()
                .gt(90d));
    }

    private static ScenarioBuilder buildPostScenario() {
        return CoreDsl.scenario("Load messages Scenario")
                .feed(feeder)
                .exec(http("POST message")
                        .post("/message")
                        .header("Content-Type", "application/json")
                        .body(StringBody("{ " +
                                "\"key\": \"${key}\" " +
                                "}"))
                );
    }

    private static Iterator<Map<String, Object>> buildFeeder() {
        Faker faker = new Faker();
        return Stream.generate(() -> {
            Map<String, Object> objectMap = new HashMap<>();
            objectMap.put("key", faker.random().hex());
            return objectMap;
        }).iterator();
    }

    private static HttpProtocolBuilder httpProtocolBuilder() {
        return HttpDsl.http.baseUrl("http://localhost:8080")
                .acceptHeader("application/json")
                .maxConnectionsPerHost(10)
                .userAgentHeader("Performance Test");
    }

    private static OpenInjectionStep injectionBuilder() {
        int totalUsers = 10;
        double userRampUpPerInterval = 10;
        double rampUpIntervalInSeconds = 30;

        int rampUptimeSeconds = 300;
        int duration = 30;
        return rampUsersPerSec(userRampUpPerInterval / (rampUpIntervalInSeconds)).to(totalUsers)
                .during(Duration.ofSeconds(rampUptimeSeconds + duration));
    }
}
