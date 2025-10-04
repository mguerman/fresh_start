import dagster as dg
from dagster_aws.s3 import S3Resource

@dg.definitions
def resources() -> dg.Definitions:
    return dg.Definitions(resources={
        "s3": S3Resource(
            region_name=dg.EnvVar("S3_REGION"),
            aws_access_key_id=dg.EnvVar("S3_ACCESS_KEY_ID"),
            aws_secret_access_key=dg.EnvVar("S3_SECRET_ACCESS_KEY")
    )

    })
