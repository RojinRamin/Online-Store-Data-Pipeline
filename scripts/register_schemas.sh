#!/bin/sh

echo "Waiting for Schema Registry..."

until curl -s http://schema-registry:8081/subjects >/dev/null
do
  sleep 5
done

echo "Schema Registry is ready"

for file in /schemas/*.avsc
do
    SUBJECT=$(basename "$file" .avsc)

    echo "Registering $SUBJECT"

    curl -X POST \
      http://schema-registry:8081/subjects/${SUBJECT}-value/versions \
      -H "Content-Type: application/vnd.schemaregistry.v1+json" \
      -d "{\"schema\": $(jq -Rs . < "$file")}"
done

echo "All schemas registered"