package io.fluo.commoncrawl.web;

import com.codahale.metrics.health.HealthCheck;

public class FluoHealthCheck extends HealthCheck {

  @Override
  protected Result check() throws Exception {
    return Result.healthy();
  }
}
