import boto3
import boto3.session
import datetime
from datetime import timezone
from dateutil.tz import tzutc
import subprocess

import azcopy

current_datetime = datetime.datetime.now(timezone.utc)
time = 30

SOURCE_BUCKET="tmx-sb-source"
CERT_PATH="tmx-sb-source/tmx-config-files/csamx_cert_out.pem"

def file_transfer(file):
	s3_resource = boto3.resource('s3')

	copy_source = {
	        'Bucket': SOURCE_BUCKET,
	        'Key': file
	}
	#s3_resource.meta.client.copy(copy_source, DESTINATION_BUCKET, file)
	subprocess.call(["azcopy", "login", "--service-principal", "--certificate-path={0}".format(CERT_PATH),
                     "--application-id=420fce6f-04b5-42e2-aa24-65e948201ac3",
                     "--tenant-id=015637ad-dbce-4b9c-a607-525b6b1cff39"])

    subprocess.call(["azcopy", "copy", copy_source,
                     "https://csamxstorage.blob.core.windows.net/mxblob-uat/test1"])

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