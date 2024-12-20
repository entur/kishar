package org.entur.kishar.gtfsrt;

import io.restassured.RestAssured;
import io.restassured.filter.log.RequestLoggingFilter;
import io.restassured.filter.log.ResponseLoggingFilter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static io.restassured.RestAssured.given;

public class TestRestEndpoints extends SiriToGtfsRealtimeServiceTest{

    @BeforeEach
    public void init() {
        RestAssured.port = 1234; //defined in application.properties
        RestAssured.baseURI = "http://localhost";
        RestAssured.filters(new RequestLoggingFilter());
        RestAssured.filters(new ResponseLoggingFilter());
    }

    @Test
    public void testAlerts() {
        given()
                .when()
                .get("/api/alerts")
                .then()
                .statusCode(200);
    }

    @Test
    public void testTripUpdates() {
        given()
                .when()
                .get("/api/trip-updates")
                .then()
                .statusCode(200);
    }

    @Test
    public void testVehiclePositions() {
        given()
                .when()
                .get("/api/vehicle-positions")
                .then()
                .statusCode(200);
    }
}
