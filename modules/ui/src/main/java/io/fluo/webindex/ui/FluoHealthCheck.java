package io.fluo.webindex.ui;

import com.codahale.metrics.health.HealthCheck;

public class FluoHealthCheck extends HealthCheck {

  @Override
  protected Result check() throws Exception {
    return Result.healthy();
  }
}
