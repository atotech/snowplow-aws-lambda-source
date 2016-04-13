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

import com.amazonaws.services.lambda.AWSLambdaClient;
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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class Main {

    public static final String APP_ID = "s3-monitor-lambda";
    public static final String SCHEMA = "iglu:com.amazon.aws.lambda/s3_notification_event/jsonschema/1-0-0";

    public static final Object isFinishedSending = new Object();

    public static HttpClientAdapter getClient(String url) {
        OkHttpClient client = new OkHttpClient();

        client.setConnectTimeout(5, TimeUnit.SECONDS);
        client.setReadTimeout(5, TimeUnit.SECONDS);
        client.setWriteTimeout(5, TimeUnit.SECONDS);

        return OkHttpClientAdapter.builder()
                .url(url)
                .httpClient(client)
                .build();
    }

    public void trackEvents(HttpClientAdapter adapter, List<SelfDescribingJson> events, String namespace) {

        final long expectedSuccesses = events.size();
        final AtomicInteger failureCount = new AtomicInteger();

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
                        failureCount.getAndAdd(failedEvents.size());
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

        if (failureCount.get() > 0)
        {
            throw new RuntimeException("Failed to send " + failureCount + " events to collector!");
        }
    }

    public static SelfDescribingJson toEvent(String schema, Object event) {
        return new SelfDescribingJson(schema, event);
    }

    public static String getRegion(String arn) {

        if (arn==null||arn.trim().isEmpty())
            throw new IllegalArgumentException("Cannot extract region from empty ARN");

        String region;
        try {
            region = arn.split(":")[3];
        } catch (Exception e){
            throw new IllegalArgumentException("Couldn't get region from ARN '" + arn + "'", e);
        }

        return region;
    }

    public void handleRequest(S3Event s3event, Context context) {

        List<SelfDescribingJson> events = s3event.getRecords()
                .stream()
                .map(x -> toEvent(SCHEMA, x))
                .collect(Collectors.toList());

        String region = getRegion(context.getInvokedFunctionArn());
        AWSLambdaClient awsLambdaClient = LambdaUtils.getAwsLambdaClientForRegion(region);
        String collectorUrl = LambdaUtils.getLambdaDescription(awsLambdaClient, context.getFunctionName());

        try {
            new URL(collectorUrl);
        } catch (MalformedURLException e) {
            throw new IllegalStateException("Collector URL in lambda description is invalid - '" + collectorUrl + "'", e);
        }

        trackEvents(getClient(collectorUrl), events, APP_ID);
    }
}