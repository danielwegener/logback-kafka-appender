# Change Log
All notable changes to this project will be documented in this file.

## [0.2.1] - Unreleased
### Changed

## [0.2.0] - 2021-03-11
### Changed
- Forked <https://github.com/danielwegener/logback-kafka-appender> repository due to non-maintenance, and it will be available with groupId `com.github.rahulsinghai`
- Added support for Kerberized Kafka cluster (#103)
- Fixed typos in logback warning messages emitted by `com.github.rahulsinghai.logback.kafka.KafkaAppenderConfig` (#28)
- Switched default delivery strategy to `com.github.rahulsinghai.logback.kafka.delivery.AsynchronousDeliveryStrategy` as it is the [more sensible default](https://github.com/rahulsinghai/logback-kafka-appender/pull/32).
- Make artifact osgi compatible (#40)
- Upgrade to logback 1.2.3. Removed `KafkaMessageEncoder` infrastructure in favor of `ch.qos.logback.core.encoder.Encoder` (#51).
- Upgrade to kafka 2.1.1 (which supports protocol negotiation for older broker versions). Add use the actual log event timestamp to the kafka record.
- Target partition can now be configured with a fixed number
- Deprecating `BlockingDeliveryStrategy` in favor of `AsynchronousDeliveryStrategy`
- `RoundRobinKeyingStrategy` has been renamed to `NoKeyKeyingStrategy.`
- Semantic checking of `kafkaProducerValues` has been removed (in order to support multiple versions of kafka-clients).
- Minimum Java version is 1.8

## [0.1.0] - 2016-02-07
### Changed
- Class `com.github.danielwegener.logback.kafka.KafkaAppenderBase` has been inlined into the `KafkaAppender`.
- Class `com.github.danielwegener.logback.kafka.encoding.PatternLayoutKafkaMessageEncoder` has been renamed to `com.github.danielwegener.logback.kafka.encoding.LayoutKafkaMessageEncoder` (#9). To ease the migration, there is still a deprecated class alias `PatternLayoutKafkaMessageEncoder`. You should change your logback.xml to `LayoutKafkaMessageEncoder` as soon as possible!
- `KafkaAppender`, `KeyingStrategy` and `LayoutKafkaMessageEncoder` are now generic and can now be used with alternative logback implementations like logback-access (#16)!

## [0.0.5] - 2015-12-23
### Changed
- Upgrade to kafka 0.9.0. This includes that __Java 6 is no longer supported__.
- Using of deprecated kafka configuration is reported as logback warning. 

## [0.0.4] - 2015-11-28
### Changed
- Missing config keys having a default value are reported as info rather as warning (from @soniro)
- Made producer lazy initialized to avoid warnings on logger startup (from @aerskine)

## [0.0.3] - 2015-08-20
### Changed
- Upgrade to kafka 0.8.2.1
### Added
- Documentation for custom serializers

## [0.0.2] - 2015-03-12
### Changed
- Fix deadlocks in AsyncProducer
- Mockito is now a test only dependency
- Handling of `BufferExhaustedExceptions` making `block.on.buffer.full=false` usable
- Suppress log events from `org.apache.kafka.*.` namespace

### Added
- Documentation for delivery strategies

## [0.0.1] - 2015-02-23
- initial release

[Unreleased]: https://github.com/danielwegener/logback-kafka-appender/compare/logback-kafka-appender-0.1.0...HEAD
[0.1.0]: https://github.com/danielwegener/logback-kafka-appender/compare/logback-kafka-appender-0.0.5...logback-kafka-appender-0.1.0
[0.0.5]: https://github.com/danielwegener/logback-kafka-appender/compare/logback-kafka-appender-0.0.4...logback-kafka-appender-0.0.5
[0.0.4]: https://github.com/danielwegener/logback-kafka-appender/compare/logback-kafka-appender-0.0.3...logback-kafka-appender-0.0.4
[0.0.3]: https://github.com/danielwegener/logback-kafka-appender/compare/logback-kafka-appender-0.0.2...logback-kafka-appender-0.0.3
[0.0.2]: https://github.com/danielwegener/logback-kafka-appender/compare/logback-kafka-appender-0.0.1...logback-kafka-appender-0.0.2
[0.0.1]: https://github.com/danielwegener/logback-kafka-appender/compare/465947...logback-kafka-appender-0.0.1
