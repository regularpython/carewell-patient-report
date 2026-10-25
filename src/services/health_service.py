
import boto3
import pandas as pd
import logging

from io import BytesIO, StringIO
from botocore.exceptions import ClientError, NoCredentialsError


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("hospital_reports.log")
    ]
)

logger = logging.getLogger(__name__)


class HospitalReports:

    def __init__(self, bucket_name, file_key, output_key):
        self.bucket_name = bucket_name
        self.file_key = file_key
        self.output_key = output_key

        # Create S3 client
        self.s3_client = boto3.client("s3")
        self.df = pd.DataFrame()

        logger.info("HospitalReports ETL initialized")
        logger.info("Input file: s3://%s/%s", bucket_name, file_key)
        logger.info("Output file: s3://%s/%s", bucket_name, output_key)

    def read(self):
        try:
            logger.info("Starting READ operation")

            response = self.s3_client.get_object(
                Bucket=self.bucket_name,
                Key=self.file_key
            )

            self.df = pd.read_csv(
                BytesIO(response["Body"].read())
            )

            logger.info("Data successfully read from S3")
            logger.info("Number of records read: %d", len(self.df))
            logger.info("Number of columns: %d", len(self.df.columns))

        except NoCredentialsError:
            logger.exception("AWS credentials not found")
            raise

        except ClientError:
            logger.exception(
                "Failed to read file from S3: %s",
                self.file_key
            )
            raise

        except pd.errors.EmptyDataError:
            logger.exception("The CSV file is empty")
            raise

        except pd.errors.ParserError:
            logger.exception("Failed to parse CSV file")
            raise

        except Exception:
            logger.exception("Unexpected error during READ operation")
            raise

    def transform(self):
        try:
            logger.info("Starting TRANSFORM operation")

            if self.df.empty:
                raise ValueError(
                    "No data available for transformation"
                )

            logger.info("Validating required columns")

            required_columns = [
                "patient_id", "name", "age", "gender", "city",
                "department", "diagnosis", "admission_date",
                "discharge_date", "treatment_cost",
                "insurance_covered", "insurance_provider",
                "blood_group"
            ]

            missing_columns = [
                col for col in required_columns
                if col not in self.df.columns
            ]

            if missing_columns:
                raise ValueError(
                    f"Missing required columns: {missing_columns}"
                )

            logger.info("Required column validation successful")

            # Select required columns
            self.df = self.df[required_columns]

            logger.info("Selected required columns")
            logger.info("Grouping data by city")

            # Calculate total treatment cost by city
            self.df = self.df.groupby(
                "city"
            )["treatment_cost"].sum()

            logger.info("Data transformation completed successfully")
            logger.info("Number of city groups: %d", len(self.df))

        except ValueError:
            logger.exception("Transformation validation failed")
            raise

        except Exception:
            logger.exception("Unexpected error during TRANSFORM operation")
            raise

    def load(self):
        try:
            logger.info("Starting LOAD operation")

            if self.df.empty:
                raise ValueError(
                    "No data available to save"
                )

            # Convert DataFrame to CSV in memory
            csv_buffer = StringIO()
            self.df.to_csv(csv_buffer)

            logger.info("Data converted to CSV format")

            # Upload to S3
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=self.output_key,
                Body=csv_buffer.getvalue()
            )

            logger.info(
                "Data successfully saved to s3://%s/%s",
                self.bucket_name,
                self.output_key
            )

            logger.info("Number of output records: %d", len(self.df))

        except NoCredentialsError:
            logger.exception("AWS credentials not found during LOAD")
            raise

        except ClientError:
            logger.exception(
                "Failed to upload data to S3: %s",
                self.output_key
            )
            raise

        except Exception:
            logger.exception("Unexpected error during LOAD operation")
            raise


if __name__ == "__main__":

    bucket_name = "regular-animation-test"
    file_key = "landing_zone/healthcare_dataset_1000.csv"
    output_key = "derived_zone/class_processed_healthcare_dataset_1000.csv"

    try:
        logger.info("Hospital Reports ETL pipeline started")

        obj = HospitalReports(
            bucket_name,
            file_key,
            output_key
        )

        # READ
        obj.read()
        logger.info("Sample input data:\n%s", obj.df.head(3))

        # TRANSFORM
        obj.transform()
        logger.info("Transformed data:\n%s", obj.df)

        # LOAD
        obj.load()

        logger.info("Hospital Reports ETL pipeline completed successfully")

    except Exception:
        logger.exception("Hospital Reports ETL pipeline failed")

    finally:
        logger.info("Hospital Reports ETL pipeline execution finished")
