# f1-data-fetcher-databricks

🏁 F1 Telemetry Ingestion with OpenF1, Databricks & MongoDB Atlas

This project provides a complete pipeline to ingest and enrich Formula 1 telemetry data using the OpenF1 API, process it within Databricks, and store structured, queryable data into MongoDB Atlas.

It’s ideal for:

    🧪 Historical analysis of sessions (e.g., Free Practice, Qualifying, Races)

    📊 Building real-time dashboards or analytics tools

    🛠️ Powering data science workflows with clean F1 telemetry

    🧠 Enhancing ML models with position, gaps, tyre data, and more

The ingestion includes:

    Car telemetry (speed, gear, throttle, brake, DRS)

    Lap metadata (stint, tyre compound, tyre age, starting speed)

    Positional and gap context at each time slice

    Driver & session-level metadata

All data is structured and inserted into a cloud-hosted MongoDB Atlas collection, where it can be queried, analyzed, or exported for downstream applications.
