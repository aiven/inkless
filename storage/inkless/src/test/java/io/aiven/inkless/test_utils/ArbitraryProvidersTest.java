// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.test_utils;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.record.Records;
import org.apache.kafka.common.record.SimpleRecord;

import net.jqwik.api.ForAll;
import net.jqwik.api.Property;

import static org.junit.jupiter.api.Assertions.assertNotNull;

public class ArbitraryProvidersTest {

    @Property
    public void dataLayout(@ForAll DataLayout layout) {
        assertNotNull(layout);
    }

    @Property
    public void headers(@ForAll Header header) {
        assertNotNull(header);
    }

    @Property
    public void simpleRecord(@ForAll SimpleRecord record) {
        assertNotNull(record);
    }

    @Property
    public void records(@ForAll Records records) {
        assertNotNull(records);
    }
}
