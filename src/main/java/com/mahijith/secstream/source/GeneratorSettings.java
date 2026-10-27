package com.mahijith.secstream.source;

/**
 * @param count events to generate; the source is finite
 * @param ratePerSecond target rate, or 0 for as fast as the pipeline accepts
 * @param firewallFraction share of firewall events (the rest are sign-ins)
 * @param failureFraction share of failed sign-ins / denied connections
 * @param duplicateFraction share of events that re-send an earlier event verbatim
 * @param malformedFraction share of deliberately invalid payloads
 * @param seed makes the generated stream reproducible
 */
public record GeneratorSettings(
    long count,
    int ratePerSecond,
    double firewallFraction,
    double failureFraction,
    double duplicateFraction,
    double malformedFraction,
    long seed) {

  public GeneratorSettings {
    if (count < 0 || ratePerSecond < 0) {
      throw new IllegalArgumentException("count and ratePerSecond must be >= 0");
    }
    for (double f :
        new double[] {firewallFraction, failureFraction, duplicateFraction, malformedFraction}) {
      if (!(f >= 0.0 && f <= 1.0)) {
        throw new IllegalArgumentException("fractions must be within [0, 1]");
      }
    }
    if (duplicateFraction + malformedFraction > 1.0) {
      throw new IllegalArgumentException("duplicateFraction + malformedFraction must be <= 1");
    }
  }
}
