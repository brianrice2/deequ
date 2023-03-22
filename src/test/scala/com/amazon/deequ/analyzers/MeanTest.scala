/**
 * Copyright 2023 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"). You may not
 * use this file except in compliance with the License. A copy of the License
 * is located at
 *
 *     http://aws.amazon.com/apache2.0/
 *
 * or in the "license" file accompanying this file. This file is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 *
 */
package com.amazon.deequ.analyzers

import com.amazon.deequ.SparkContextSpec
import com.amazon.deequ.metrics.DoubleMetric
import com.amazon.deequ.utils.FixtureSupport
import org.scalatest.wordspec.AnyWordSpec

class MeanTest extends AnyWordSpec with SparkContextSpec with FixtureSupport {

  "Mean" should {
    "match expected values for a happy path case" in withSparkSession { session =>
      val data = getDfWithNumericValues(session)

      val attributeMean = Mean("att2")
      val state: Option[MeanState] = attributeMean.computeStateFrom(data)
      val metric: DoubleMetric = attributeMean.computeMetricFrom(state)

      assert(metric.value.get == 3.0)
    }

    "not overflow for min/max long values" in withSparkSession { session =>
      val data = getDfWithReallyLargeNumbers(session)

      val attributeMean = Mean("att1")
      val state: Option[MeanState] = attributeMean.computeStateFrom(data)
      val metric: DoubleMetric = attributeMean.computeMetricFrom(state)

      assert(metric.value.get == 0.0)
    }
  }

}
