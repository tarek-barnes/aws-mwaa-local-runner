import logging
import requests

def download_public_file_from_s3(s3_public_url, destination_file_path):
	response = requests.get(s3_public_url)
	if response.status_code == 200:
		with open(destination_file_path, 'wb') as f:
			f.write(response.content)
		logging.info(f"""
			File successfully downloaded from public S3 bucket.
			S3 Bucket: {s3_public_url}
			Destination: {destination_file_path}
			""")
	else:
		logging.error(f"""
			Download from public S3 bucket failed.
			S3 Bucket: {s3_public_url}
			Status Code: {response.status_code}
			""")
		raise Exception(f"""
			Failed to download file from public S3 bucket.
			Status Code: {response.status_code}
			""")
