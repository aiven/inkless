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
// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/

package kafka.server.metadata

import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{BeforeEach, Test}
import org.mockito.Mockito._

import java.util
import java.util.function.Supplier

class InklessMetadataViewTest {
  private var metadataCache: KRaftMetadataCache = _
  private var configSupplier: Supplier[util.Map[String, Object]] = _
  private var metadataView: InklessMetadataView = _

  @BeforeEach
  def setup(): Unit = {
    metadataCache = mock(classOf[KRaftMetadataCache])
    configSupplier = mock(classOf[Supplier[util.Map[String, Object]]])
    metadataView = new InklessMetadataView(metadataCache, configSupplier)
  }

  @Test
  def testGetDefaultConfigFiltersNullValues(): Unit = {
    // Setup a map with some null values
    val originalConfig = new util.HashMap[String, Object]()
    originalConfig.put("key1", "value1")
    originalConfig.put("key2", null)
    originalConfig.put("key3", Integer.valueOf(42))
    originalConfig.put("key4", null)

    // Configure the mock to return our test map
    when(configSupplier.get()).thenReturn(originalConfig)

    // Call the method under test
    val filteredConfig = metadataView.getDefaultConfig

    // Verify null values were filtered out
    assertEquals(2, filteredConfig.size)
    assertTrue(filteredConfig.containsKey("key1"))
    assertEquals("value1", filteredConfig.get("key1"))
    assertFalse(filteredConfig.containsKey("key2"))
    assertTrue(filteredConfig.containsKey("key3"))
    assertEquals(Integer.valueOf(42), filteredConfig.get("key3"))
    assertFalse(filteredConfig.containsKey("key4"))
  }

  @Test
  def testGetDefaultConfigWithNoNullValues(): Unit = {
    // Setup a map with no null values
    val originalConfig = new util.HashMap[String, Object]()
    originalConfig.put("key1", "value1")
    originalConfig.put("key2", "value2")

    // Configure the mock to return our test map
    when(configSupplier.get()).thenReturn(originalConfig)

    // Call the method under test
    val filteredConfig = metadataView.getDefaultConfig

    // Verify the filtered map contains all original entries
    assertEquals(2, filteredConfig.size)
    assertEquals("value1", filteredConfig.get("key1"))
    assertEquals("value2", filteredConfig.get("key2"))
  }

  @Test
  def testGetDefaultConfigWithEmptyMap(): Unit = {
    // Setup an empty map
    val originalConfig = new util.HashMap[String, Object]()

    // Configure the mock to return our test map
    when(configSupplier.get()).thenReturn(originalConfig)

    // Call the method under test
    val filteredConfig = metadataView.getDefaultConfig

    // Verify the filtered map is empty
    assertTrue(filteredConfig.isEmpty)
  }
}

