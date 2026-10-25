import boto3
import pandas as pd
from io import BytesIO
import matplotlib.pyplot as plt
from io import StringIO

# S3 Configuration
bucket_name = "regular-animation-test"
file_key = "landing_zone/healthcare_dataset_1000.csv"
output_key = "derived_zone/processed_healthcare_dataset_1000.csv"


# Create S3 client
s3_client = boto3.client("s3")

# Data
response = s3_client.get_object(
    Bucket=bucket_name,
    Key=file_key
)

df = pd.read_csv(BytesIO(response["Body"].read()))
print(df.shape)

# Transformation
required_colums = ["patient_id"	,"name","age", "gender",	"city",	"department",	"diagnosis",	"admission_date",
                   "discharge_date",	"treatment_cost",	"insurance_covered",
                   "insurance_provider", "blood_group"]
df = df[required_colums]

df = df.groupby("city")["treatment_cost"].sum()

# Convert DataFrame to CSV in memory
csv_buffer = StringIO()
df.to_csv(csv_buffer)


# Load
# Upload CSV to S3
s3_client.put_object(
    Bucket=bucket_name,
    Key=output_key,
    Body=csv_buffer.getvalue()
)

print("DataFrame successfully saved to S3!")


# df.to_csv("City_treatment.csv")
# print(df)



# city_counts.plot.bar(figsize=(6.4, 3.4),
#                      color="#0E8A8C")
# plt.title("Treatment Cost per city")
# plt.ylabel("Treatment Cost")
# plt.show()


