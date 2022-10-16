/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dong;

import com.alibaba.fastjson2.JSONObject;
import com.dong.utils.DimUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import java.util.Objects;

@Slf4j
public class DataStreamJob {
	public static void main(String[] args) throws Exception {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		KafkaSource<JSONObject> source = KafkaSource.<JSONObject>builder()
				.setBootstrapServers("127.0.0.1:9092")
				.setTopics("test")
				.setGroupId("my-group")
				.setStartingOffsets(OffsetsInitializer.earliest())
				.setValueOnlyDeserializer(new KafkaDecoder())
				.build();

		KafkaSink<JSONObject> sink = KafkaSink.<JSONObject>builder()
				.setBootstrapServers("127.0.0.1:9092")
				.setRecordSerializer(KafkaRecordSerializationSchema.builder()
						.setTopic("test1")
						.setValueSerializationSchema(new KafkaEncoder())
						.build()
				)
				.build();
		env.fromSource(source,WatermarkStrategy.noWatermarks(),"xxx")
				.map(o -> {
					Integer shopId = 0;
					try {
						String shopName = o.getString("shopName");
						if (!Objects.isNull(shopName) && !shopName.isEmpty()) {
							JSONObject s = DimUtil.getDimInfo("shop_info", o.getString("shopName"));
							shopId = s.getInteger("shop_id");
						}
					} catch (Exception e) {
						log.warn("xxxx {}",e);
					}
					JSONObject newOne = o.clone();
					newOne.put("shop_id",shopId);
					log.info("new record:{}",newOne);
					return newOne;
				}).sinkTo(sink);

//		DataStream<String> stream = source.filter();
//
//		KafkaSink<String> sink = KafkaSink.<String>builder()
//				.setBootstrapServers("127.0.0.1:9092")
//				.setRecordSerializer(KafkaRecordSerializationSchema.builder()
//						.setTopic("test1")
//						.setValueSerializationSchema(new SimpleStringSchema())
//						.build()
//				)
//				.build();
//
//		stream.sinkTo(sink);

		//empStream.print(); //调度输出
		env.execute("flink kafka to Mysql");

	}
}