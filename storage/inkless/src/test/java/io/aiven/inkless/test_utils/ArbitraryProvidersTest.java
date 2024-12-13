/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
