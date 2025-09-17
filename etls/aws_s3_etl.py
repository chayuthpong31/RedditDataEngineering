from utils.constants import AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY
import s3fs

def connect_to_s3():
    try:
        s3 = s3fs.S3FileSystem(anon=False,
                                key=AWS_ACCESS_KEY_ID,
                                secret=AWS_SECRET_ACCESS_KEY)
        print("Connected to S3")
        return s3
    except Exception as e:
        print(f"Failed to connect to S3: {e}")

def create_bucket_if_not_exists(s3:s3fs.S3FileSystem, bucket: str):
    try:
        if not s3.exists(bucket):
            s3.mkdir(bucket)
            print(f"Bucket '{bucket}' created.")
        else:
            print(f"Bucket '{bucket}' already exists.")
    except Exception as e:
        print(f"Error checking/creating bucket: {e}")

def upload_to_s3(s3:s3fs.S3FileSystem, file_path:str, bucket:str, s3_file_name:str):
    try:
        s3.put(file_path, bucket + '/raw/' + s3_file_name)
        print(f"File '{file_path}' uploaded to S3.")
    except Exception as e:
        print('The file was not found.')
    