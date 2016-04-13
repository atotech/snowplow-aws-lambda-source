/*
 * Copyright (c) 2016 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0, and
 * you may not use this file except in compliance with the Apache License
 * Version 2.0.  You may obtain a copy of the Apache License Version 2.0 at
 * http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the Apache License Version 2.0 is distributed on an "AS
 * IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the Apache License Version 2.0 for the specific language
 * governing permissions and limitations there under.
 */
package com.snowplowanalytics;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.snowplowanalytics.snowplow.tracker.DevicePlatform;
import com.snowplowanalytics.snowplow.tracker.Tracker;
import com.snowplowanalytics.snowplow.tracker.emitter.BatchEmitter;
import com.snowplowanalytics.snowplow.tracker.emitter.Emitter;
import com.snowplowanalytics.snowplow.tracker.emitter.RequestCallback;
import com.snowplowanalytics.snowplow.tracker.events.Unstructured;
import com.snowplowanalytics.snowplow.tracker.http.HttpClientAdapter;
import com.snowplowanalytics.snowplow.tracker.http.OkHttpClientAdapter;
import com.snowplowanalytics.snowplow.tracker.payload.SelfDescribingJson;
import com.snowplowanalytics.snowplow.tracker.payload.TrackerPayload;
import com.squareup.okhttp.OkHttpClient;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class Main {

    public static final String APP_ID = "s3-monitor-lambda";
    public static final String SCHEMA = "iglu:com.banana/example/jsonschema/1-0-0";

    public static final Object isFinishedSending = new Object();

    public void trackEvents(String url, List<SelfDescribingJson> events, String namespace) {
        OkHttpClient client = new OkHttpClient();

        client.setConnectTimeout(5, TimeUnit.SECONDS);
        client.setReadTimeout(5, TimeUnit.SECONDS);
        client.setWriteTimeout(5, TimeUnit.SECONDS);

        HttpClientAdapter adapter = OkHttpClientAdapter.builder()
                .url(url)
                .httpClient(client)
                .build();

        final long expectedSuccesses = events.size();
        Emitter emitter = BatchEmitter.builder()
                .httpClientAdapter(adapter)
                .requestCallback(new RequestCallback() {
                    @Override
                    public void onSuccess(int successCount) {
                        if (successCount==expectedSuccesses)
                        {
                            synchronized (isFinishedSending)
                            {
                                isFinishedSending.notify();
                            }
                        }
                    }

                    @Override
                    public void onFailure(int successCount, List<TrackerPayload> failedEvents) {
                        if (successCount+failedEvents.size()==expectedSuccesses)
                        {
                            synchronized (isFinishedSending) {
                                isFinishedSending.notify();
                            }

                        }
                    }
                })
                .bufferSize(events.size())
                .build();

        Tracker tracker = new Tracker.TrackerBuilder(emitter, namespace, APP_ID)
                .base64(true)
                .platform(DevicePlatform.ServerSideApp)
                .build();

        for (SelfDescribingJson e : events) {
            tracker.track(Unstructured.builder().eventData(e).build());
        }

        try {
            synchronized (isFinishedSending) {  // AWS will kill remaining threads if main exits,
                isFinishedSending.wait();       // so we are turning the async emitter into a sync one
            }                                   // lambda's also have timeouts so if this wait is forever it'll still die
        } catch (InterruptedException e)
        {
            throw new IllegalStateException(e);
        }
    }

    public static SelfDescribingJson toEvent(String schema, Object event) {
        return new SelfDescribingJson(schema, event);
    }

    public static String getRegion(String arn) {
        String region;
        try {
            region = arn.split(":")[3];
        } catch (Exception e){
            throw new IllegalStateException("Couldn't get region from invoked arn" + arn, e);
        }

        return region;
    }

    public void handleRequest(S3Event s3event, Context context) {

        List<SelfDescribingJson> events = s3event.getRecords()
                .stream()
                .map(x -> toEvent(SCHEMA, x))
                .collect(Collectors.toList());

        ObjectMapper m = new ObjectMapper();
        s3event.getRecords().stream().forEach(event -> {
            String eventJson = null;
            try {
                eventJson = m.writeValueAsString(event);
            } catch (JsonProcessingException e) {
                throw new IllegalStateException(e);
            }
            context.getLogger().log(eventJson);
        }); // debug

        String region = getRegion(context.getInvokedFunctionArn());
        String collectorUrl = LambdaUtils.getLambdaDescription(region, context.getFunctionName());

        try {
            new URL(collectorUrl);
        } catch (MalformedURLException e) {
            throw new IllegalStateException("Collector URL in lambda description is invalid - '" + collectorUrl + "'", e);
        }

        trackEvents(collectorUrl, events, APP_ID);
    }
}