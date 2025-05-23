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
package org.apache.kafka.streams.kstream.internals;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.internals.SerdeGetter;

/**
 * If a component's serdes are Wrapping serdes, then they require a little extra setup
 * to be fully initialized at run time.
 */
public class WrappingNullableUtils {

    @SuppressWarnings({"unchecked", "resource"})
    private static <T> Deserializer<T> prepareDeserializer(final Deserializer<T> specificDeserializer, final ProcessorContext<?, ?> context, final boolean isKey) {
        final Deserializer<T> deserializerToUse;

        if (specificDeserializer == null) {
            deserializerToUse = (Deserializer<T>) (isKey ? context.keySerde().deserializer() : context.valueSerde().deserializer());
        } else {
            deserializerToUse = specificDeserializer;
            initNullableDeserializer(deserializerToUse, new SerdeGetter(context));
        }
        return deserializerToUse;
    }

    @SuppressWarnings({"unchecked", "resource"})
    private static <T> Serializer<T> prepareSerializer(final Serializer<T> specificSerializer, final ProcessorContext<?, ?> context, final boolean isKey) {
        final Serializer<T> serializerToUse;
        if (specificSerializer == null) {
            serializerToUse = (Serializer<T>) (isKey ? context.keySerde().serializer() : context.valueSerde().serializer());
        } else {
            serializerToUse = specificSerializer;
            initNullableSerializer(serializerToUse, new SerdeGetter(context));
        }
        return serializerToUse;
    }

    @SuppressWarnings("unchecked")
    private static <T> Serde<T> prepareSerde(final Serde<T> specificSerde, final SerdeGetter getter, final boolean isKey) {
        final Serde<T> serdeToUse;
        if (specificSerde == null) {
            serdeToUse = (Serde<T>) (isKey ? getter.keySerde() : getter.valueSerde());
        } else {
            serdeToUse = specificSerde;
        }
        if (serdeToUse instanceof WrappingNullableSerde) {
            ((WrappingNullableSerde<?, ?, ?>) serdeToUse).setIfUnset(getter);
        }
        return serdeToUse;
    }

    public static <K> Deserializer<K> prepareKeyDeserializer(final Deserializer<K> specificDeserializer, final ProcessorContext<K, ?> context) {
        return prepareDeserializer(specificDeserializer, context, true);
    }

    public static <V> Deserializer<V> prepareValueDeserializer(final Deserializer<V> specificDeserializer, final ProcessorContext<?, V> context) {
        return prepareDeserializer(specificDeserializer, context, false);
    }

    public static <K> Serializer<K> prepareKeySerializer(final Serializer<K> specificSerializer, final ProcessorContext<?, ?> context) {
        return prepareSerializer(specificSerializer, context, true);
    }

    public static <V> Serializer<V> prepareValueSerializer(final Serializer<V> specificSerializer, final ProcessorContext<?, ?> context) {
        return prepareSerializer(specificSerializer, context, false);
    }

    public static <K> Serde<K> prepareKeySerde(final Serde<K> specificSerde, final SerdeGetter getter) {
        return prepareSerde(specificSerde, getter, true);
    }

    public static <V> Serde<V> prepareValueSerde(final Serde<V> specificSerde, final SerdeGetter getter) {
        return prepareSerde(specificSerde, getter, false);
    }

    public static <T> void initNullableSerializer(final Serializer<T> specificSerializer, final SerdeGetter getter) {
        if (specificSerializer instanceof WrappingNullableSerializer) {
            ((WrappingNullableSerializer<?, ?, ?>) specificSerializer).setIfUnset(getter);
        }
    }

    public static <T> void initNullableDeserializer(final Deserializer<T> specificDeserializer, final SerdeGetter getter) {
        if (specificDeserializer instanceof WrappingNullableDeserializer) {
            ((WrappingNullableDeserializer<?, ?, ?>) specificDeserializer).setIfUnset(getter);
        }
    }

}
