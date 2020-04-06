# Changelog

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- SetHeadInfo on BlockStream to allow using GetHeadInfo() before stream actually starts
- WithName option on NewForkable for better logging
- License changed to Apache 2.0

### Changed
- BREAKING CHANGE: `JoiningSourceRateLimit` now takes a `time.Duration` as a second argument, instead of an `int` amount of milliseconds.


## [v1.7.0]

### Changed
- `forkable.NewWithLIB(...)` replaced by `forkable.New(forkable.WithExclusiveLIB(...))`
- `Forkable.SetFilters()` replaced by the `forkable.WithFilters()` _Option_, to bassed to `New()`.
- `Forkable.EnsureBlockFlows()` replaced by the `forkable.EnsureBlockFlows()` _Option_.
- `Forkable.EnsureAllBlocksTriggerLongestChain()` replaced by the `forkable.EnsureAllBlocksTriggerLongestChain()` _Option_.

### Added
- `forkable.WithInclusiveLIB()` as an _Option_ to `forkable.New()`
