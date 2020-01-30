# schemaregistry
CloudEvents Schema Registry

This is a simple JSON-API and Web interface for storing, browsing, and retrieving schemas for CloudEvents.

This is meant to run on Knative or Google Cloud Run and has a dependency on Google Cloud Datastore.

# Build

```
ko publish
```

# Required environment variables

* GCP_PROJECT - the Google Cloud Project ID that the datastore DB lives in

Also, your Google Application Credentials need to be published correctly (env var, service account, etc)
