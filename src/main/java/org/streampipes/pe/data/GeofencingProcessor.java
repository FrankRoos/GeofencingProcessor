/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.streampipes.pe.data;

import org.apache.streampipes.commons.exceptions.SpRuntimeException;
import org.apache.streampipes.model.DataProcessorType;
import org.apache.streampipes.model.graph.DataProcessorDescription;
import org.apache.streampipes.model.runtime.Event;
import org.apache.streampipes.model.schema.PropertyScope;
import org.apache.streampipes.sdk.builder.ProcessingElementBuilder;
import org.apache.streampipes.sdk.builder.StreamRequirementsBuilder;
import org.apache.streampipes.sdk.helpers.*;
import org.apache.streampipes.sdk.utils.Assets;
import org.apache.streampipes.vocabulary.Geo;
import org.apache.streampipes.wrapper.context.EventProcessorRuntimeContext;
import org.apache.streampipes.wrapper.routing.SpOutputCollector;
import org.apache.streampipes.wrapper.standalone.ProcessorParams;
import org.apache.streampipes.wrapper.standalone.StreamPipesDataProcessor;

public class GeofencingProcessor extends StreamPipesDataProcessor {
  private static final String LATITUDE_CENTER = "latitude-center";
  private static final String LONGITUDE_CENTER = "longitude-center";
  private float centerLatitude;
  private float centerLongitude;
  private int radius;

  @Override
  public DataProcessorDescription declareModel() {
    return ProcessingElementBuilder.create("org.streampipes.pe.date.geofencingprocessor", "Geofencing", "A simple geofencing data org.streampipes.pe.date.geofencingprocessor")
            .category(DataProcessorType.ENRICH)
            .withAssets(Assets.DOCUMENTATION, Assets.ICON)
            .withLocales(Locales.EN)
            .requiredStream(StreamRequirementsBuilder.create()
                    .requiredPropertyWithUnaryMapping(EpRequirements.domainPropertyReq(Geo.lat),
                            Labels.withId(LATITUDE_CENTER), PropertyScope.MEASUREMENT_PROPERTY)
                    .requiredPropertyWithUnaryMapping(EpRequirements.domainPropertyReq(Geo.lng),
                            Labels.from("longitude-field", "Longitude", "The event " +
                                    "property containing the longitude value"), PropertyScope.MEASUREMENT_PROPERTY)
                    .build())
            .requiredIntegerParameter("radius", "Geofence Size", "The size of the circular geofence in meters.", 0, 1000, 1)
            .requiredFloatParameter(Labels.from(LATITUDE_CENTER, "Latitude", "The latitude value"))
            .requiredFloatParameter(Labels.from(LONGITUDE_CENTER, "Longitude", "The longitude value"))
            .outputStrategy(OutputStrategies.keep())
            .build();
  }

  @Override
  public void onInvocation(ProcessorParams parameters, SpOutputCollector spOutputCollector, EventProcessorRuntimeContext runtimeContext) throws SpRuntimeException {
    this.centerLatitude = parameters.extractor().singleValueParameter(LATITUDE_CENTER, Float.class);
    this.centerLongitude = parameters.extractor().singleValueParameter(LONGITUDE_CENTER, Float.class);
    this.radius = parameters.extractor().singleValueParameter("radius", Integer.class);
  }

  @Override
  public void onEvent(Event event, SpOutputCollector collector) throws SpRuntimeException {
    float latitude = event.getFieldBySelector(LATITUDE_CENTER).getAsPrimitive().getAsFloat();
    float longitude = event.getFieldBySelector(LONGITUDE_CENTER).getAsPrimitive().getAsFloat();

    float distance = distFrom(latitude, longitude, centerLatitude, centerLongitude);

    if (distance <= radius) {
      collector.collect(event);
    }
  }

  public static float distFrom(float lat1, float lng1, float lat2, float lng2) {
    double earthRadius = 6371000;
    double dLat = Math.toRadians(lat2-lat1);
    double dLng = Math.toRadians(lng2-lng1);
    double a = Math.sin(dLat/2) * Math.sin(dLat/2) +
            Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2)) *
                    Math.sin(dLng/2) * Math.sin(dLng/2);
    double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1-a));
    return (float) (earthRadius * c);
  }

  @Override
  public void onDetach() throws SpRuntimeException {

  }
}
