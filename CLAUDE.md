# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Flink Java User Defined Table Function (UDTF) for geofencing. 
Given an entity ID (EPC), a location ID, and x/y coordinates, the function determines which area(s) of the location the entity falls into. 
Floorplan maps are DXF files loaded from JAR resources at startup (`open()`) and cached in memory.

## Build & Test

This is a Maven project targeting Java 17. The build produces an uber-jar (via maven-shade-plugin) for deployment to Confluent Cloud Flink.

```bash
mvn clean package          # build uber-jar (includes shade)
mvn test                   # run all tests
mvn -Dtest=ClassName test  # run a single test class
```

## Architecture

- **Flink Table API**: `flink-table-api-java` 2.1.0 (provided scope — supplied by CC Flink runtime)
- **DXF maps**: stored in `src/main/resources/maps/`, filename = location ID (e.g. `ES_0279.dxf`)
- **Uber-jar**: all non-provided dependencies are shaded into the final JAR via maven-shade-plugin
- **Input data format**: JSON batches with `placeId`, `floorNumber`, and per-EPC `x`/`y` coordinates (see `example_batch_multifloor.json`)

## Key Constraints

- Dependencies already provided by the Flink runtime (`flink-table-api-java`, `log4j-api`, `commons-lang3`) must remain `provided` scope to avoid version clashes.
- The shade plugin includes all non-provided dependencies; any new dependency added with default scope will be bundled into the uber-jar.