import boto3
import boto3.session
import datetime
from datetime import timezone
from dateutil.tz import tzutc

current_datetime = datetime.datetime.now(timezone.utc)
time = 30 # Time in minutes, Files less than this time will be considered to copy

SOURCE_BUCKET="tmx-sb-copy"
DESTINATION_BUCKET="tmx-sb-dest"

def file_transfer(file):
	s3_resource = boto3.resource('s3')

	copy_source = {
	        'Bucket': SOURCE_BUCKET,
	        'Key': file
	}
	s3_resource.meta.client.copy(copy_source, DESTINATION_BUCKET, file)

s3 = boto3.client('s3')

objs = s3.list_objects_v2(Bucket=SOURCE_BUCKET)['Contents']

for obj in objs:
	if not obj["Key"].endswith("/"):
		time_diff = (current_datetime - obj["LastModified"]).total_seconds()/60
		if time_diff < time:
			file_transfer(obj["Key"])
			print(obj["Key"] + " copied successfully.")
		else:
			print(obj["Key"] + " is not latest file, which is uploaded/updated " + str(time) + " minutes ago.")
