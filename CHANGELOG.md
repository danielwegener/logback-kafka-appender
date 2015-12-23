# Change Log
All notable changes to this project will be documented in this file.

## [Unreleased]
###Changed
- Upgrade to kafka 0.9.0. This includes that __Java 6 is no longer supported__.

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

[Unreleased]: https://github.com/danielwegener/logback-kafka-appender/compare/logback-kafka-appender-0.0.4...HEAD
[0.0.4]: https://github.com/danielwegener/logback-kafka-appender/compare/logback-kafka-appender-0.0.3...logback-kafka-appender-0.0.4
[0.0.3]: https://github.com/danielwegener/logback-kafka-appender/compare/logback-kafka-appender-0.0.2...logback-kafka-appender-0.0.3
[0.0.2]: https://github.com/danielwegener/logback-kafka-appender/compare/logback-kafka-appender-0.0.1...logback-kafka-appender-0.0.2
[0.0.1]: https://github.com/danielwegener/logback-kafka-appender/compare/465947...logback-kafka-appender-0.0.1