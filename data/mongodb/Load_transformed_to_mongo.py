def load_records_to_mongodb(records, db):

    inserted_count = 0

    for record in records:

        try:

            event_type = record["event_type"]

            # Dynamic collection selection
            collection = db[event_type]

            # Create unique index
            collection.create_index(
                [
                    ("timestamp", 1),
                    ("session_id", 1)
                ],
                unique=True
            )

            collection.insert_one(record)

            inserted_count += 1

        except Exception as e:

            print(
                f"[WARNING] Failed to insert record | "
                f"event_type={record.get('event_type')} | "
                f"error={str(e)}"
            )

    return inserted_count