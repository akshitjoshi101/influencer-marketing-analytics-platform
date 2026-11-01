# Microsoft Fabric Pipeline Definition

This folder contains a sanitized, version-controlled **export** of `Influencer_Data_Pipeline` from Microsoft Fabric. It is not an independently deployable pipeline without reconnecting environment-specific references.

## Workflow

1. **Run Data Cleaning and Gold** (`TridentNotebook`) invokes the `Influencer_Data_Cleaning` notebook, which performs the project's cleaning and analytics transformations.
2. **Pipeline Failure** (`Fail`) executes only when the notebook activity fails, using the error code `DATA_PIPELINE_FAILED`.

The exported activity has a 12-hour timeout, zero automatic retries, and a retry interval setting of 30 seconds (unused while retry is zero).

## Before reuse

Replace `REPLACE_WITH_WORKSPACE_ID` and `REPLACE_WITH_NOTEBOOK_ID` with the identifiers of your own Fabric environment, and verify notebook/Lakehouse bindings and permissions. Do not commit credentials or access tokens. A separate screenshot in `docs/screenshots/` documents a successful execution; this JSON describes the configuration, not the run history.

This export does **not** prove that the pipeline is on a recurring schedule or automatically connected to the local Kafka streaming test.
