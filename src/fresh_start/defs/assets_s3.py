import dagster as dg
from dagster_aws.s3 import S3Resource
import json


@dg.asset(kinds={"s3"})
def filtered_data_to_s3(s3: S3Resource) -> None:
    # Call get_client() to retrieve the S3 client used to interact with S3
    s3_client = s3.get_client()
    # Get the S3 file from the source bucket
    response = s3_client.get_object(
        Bucket=dg.EnvVar("S3_SOURCE_BUCKET_NAME").get_value(),
        Key=dg.EnvVar("S3_SOURCE_FILE_PATH").get_value()
    )

    # Read the data from S3 object and ensure it's in utf-8 format
    data = response["Body"].read().decode("utf-8")
    # Parse JSON data
    json_data = json.loads(data)
    # Filter the data based on some criteria and return none if no match is found
    filtered_data = next((item for item in json_data if item.get("personid") =="U00026561"), None)

    # If filtered data is not None, convert it back to JSON
    if filtered_data is not None:
        record = json.dumps(filtered_data, indent=4)
    # If no match is found
    else:
        record = json.dumps({"error": "Record not found"}, indent=4)

    # Write the data to S3
    s3_client.put_object(
        Bucket=dg.EnvVar("S3_Target_BUCKET_NAME").get_value(),
        Key=dg.EnvVar("S3_TARGET_FILE_PATH").get_value(),
        Body=record
    )