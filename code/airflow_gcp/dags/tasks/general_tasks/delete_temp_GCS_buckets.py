def delete_temp_GCS_buckets(bucket_names):
    from google.cloud import storage
    from google.api_core.exceptions import NotFound

    client = storage.Client()

    for bucket_name in bucket_names:
        try:
            bucket = client.bucket(bucket_name)

            # Delete all objects in the bucket first
            blobs = client.list_blobs(bucket)
            for blob in blobs:
                blob.delete()

            # Delete the bucket
            bucket.delete()
            print(f"Deleted bucket: {bucket_name}")

        except NotFound:
            print(f"Bucket {bucket_name} not found.")
        except Exception as e:
            print(f"Error deleting bucket {bucket_name}: {e}")

    return