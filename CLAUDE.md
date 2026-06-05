# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Flink Java Process Table Function (PTF) for geofencing (`GeoLocatorDynamicMapsPTF`).
Given an item ID, a location ID, and x/y coordinates, the function determines which area(s) of the location the item falls into.
Floorplan maps are defined dynamically: the PTF consumes a `named_area_maps` table (one row per named area) and keeps the per-location map in PTF state, so maps can be added or changed at runtime without redeploying.
DXF floorplan files are converted offline into `named_area_maps` rows (SQL or JSON) by the `Dxf2NamedAreaMapsCLI` utility — they are no longer packaged in the JAR.

## Build & Test

This is a Maven project targeting Java 17. The build produces an uber-jar (via maven-shade-plugin) for deployment to Confluent Cloud Flink.

```bash
mvn clean package          # build uber-jar (includes shade)
mvn test                   # run all tests
mvn -Dtest=ClassName test  # run a single test class
```

## Architecture

- **Flink Table API**: `flink-table-api-java` 2.1.0 (provided scope — supplied by CC Flink runtime)
- **PTF**: `GeoLocatorDynamicMapsPTF` takes two `PARTITIONED BY location_id` input tables — `named_area_maps` (map definitions, kept in state) and `items` (points to locate) — and emits one row per matching area
- **DXF → maps**: `Dxf2NamedAreaMapsCLI` parses a DXF *file* (location ID = filename without `.dxf`) via `LocationLoader.loadMap(InputStream, name)` and prints `named_area_maps` rows as SQL `INSERT`s or JSON. DXF files are not bundled in the JAR.
- **Uber-jar**: all non-provided dependencies are shaded into the final JAR via maven-shade-plugin

## Key Constraints

- Dependencies already provided by the Flink runtime (`flink-table-api-java`, `log4j-api`, `commons-lang3`) must remain `provided` scope to avoid version clashes.
- The shade plugin includes all non-provided dependencies; any new dependency added with default scope will be bundled into the uber-jar.

## Confluent Cloud Flink PTF

This project targets **Confluent Cloud (CC) Flink**, not OSS Apache Flink. The PTF API in CC Flink differs from OSS Flink and has additional limitations. Always consult the CC-specific documentation below — do not use OSS-only PTF features or APIs.

Reference documentation:
- [Process Table Functions in CC Flink](https://docs.confluent.io/cloud/current/flink/concepts/process-table-functions.html)
- [Create a Process Table Function in CC Flink](https://docs.confluent.io/cloud/current/flink/how-to-guides/create-ptf.html)
- [Differences between OSS Flink PTF and CC Flink PTF](https://docs.confluent.io/cloud/current/flink/concepts/process-table-functions.html#differences-from-apache-flink)
- [Current limitations of CC Flink PTF](https://docs.confluent.io/cloud/current/flink/concepts/process-table-functions.html#process-table-function-limitations)