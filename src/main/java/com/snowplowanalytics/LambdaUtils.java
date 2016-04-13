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

import com.amazonaws.regions.Regions;
import com.amazonaws.services.lambda.AWSLambdaClient;
import com.amazonaws.services.lambda.model.GetFunctionConfigurationRequest;
import com.amazonaws.services.lambda.model.GetFunctionConfigurationResult;

public class LambdaUtils {

    public static String getLambdaDescription(String region, String lambdaFunction) {
        GetFunctionConfigurationRequest request = new GetFunctionConfigurationRequest().withFunctionName(lambdaFunction);
        AWSLambdaClient client = new AWSLambdaClient().withRegion(Regions.fromName(region));
        GetFunctionConfigurationResult result = client.getFunctionConfiguration(request);
        return result.getDescription();
    }

}
