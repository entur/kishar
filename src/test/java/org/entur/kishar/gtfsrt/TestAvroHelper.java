package org.entur.kishar.gtfsrt;

import org.apache.avro.util.Utf8;
import org.entur.kishar.gtfsrt.mappers.AvroHelper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class TestAvroHelper {

    @ParameterizedTest
    @ValueSource(strings = {
            "2026-10-26T14:17:36+01:00",
            "2026-10-26T14:17:36.123+01:00",
            "2026-10-26T14:17:36.1+02:00",
            "2026-10-26T14:17:36.123456789-03:30",
            "2026-10-26T13:17:36Z",
            "2026-10-26T13:17:36.5Z",
            "2026-12-31T23:59:59+14:00",
            "2026-01-01T00:00:00-12:00",
            "2024-02-29T12:00:00+01:00",
            "1970-01-01T00:00:00Z",
            "1969-12-31T23:59:59.999Z",
            "2026-10-26T14:17:36+01:00:30",
            "2026-10-26T24:00:00Z",
            "2026-10-26T14:17:36.Z",
    })
    public void testGetInstantMatchesInstantParse(String timestamp) {
        assertEquals(Instant.parse(timestamp), AvroHelper.getInstant(timestamp));
    }

    @Test
    public void testGetInstantMatchesInstantParseForRandomTimestamps() {
        Random random = new Random(42);
        for (int i = 0; i < 10_000; i++) {
            Instant instant = Instant.ofEpochSecond(random.nextLong(0, 4_102_444_800L), random.nextInt(1_000_000_000));
            ZoneOffset offset = ZoneOffset.ofTotalSeconds((random.nextInt(-18 * 4, 18 * 4 + 1)) * 15 * 60);
            int fractionDigits = random.nextInt(10);
            String pattern = "yyyy-MM-dd'T'HH:mm:ss" + (fractionDigits > 0 ? "." + "S".repeat(fractionDigits) : "") + "XXX";
            String timestamp = DateTimeFormatter.ofPattern(pattern).format(instant.atOffset(offset));

            assertEquals(Instant.parse(timestamp), AvroHelper.getInstant(timestamp), timestamp);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "2026-10-26T14:17:36+01:00",
            "2026-10-26T14:17:36.123Z",
    })
    public void testGetInstantAcceptsAvroUtf8(String timestamp) {
        assertEquals(Instant.parse(timestamp), AvroHelper.getInstant(new Utf8(timestamp)));
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "not-a-timestamp",
            "",
            "2026-02-30T10:00:00Z",
            "2026-10-26T25:00:00Z",
            "2026-10-26T14:17:36",
            "2026-10-26T14:17:36+0100",
            "2026-10-26 14:17:36Z",
    })
    public void testGetInstantRejectsInvalidTimestamps(String timestamp) {
        assertThrows(DateTimeParseException.class, () -> Instant.parse(timestamp));
        assertThrows(DateTimeParseException.class, () -> AvroHelper.getInstant(timestamp));
    }
}
