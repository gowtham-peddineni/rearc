import boto3
import urllib.request
import urllib.error
import re
import os

SOURCE_URL = "https://download.bls.gov/pub/time.series/pr/"
BUCKET_NAME = "bls-time-series-gp" 
USER_AGENT = "bls-time-series/gp1 (gowthampix96@gmail.com)"
S3_PREFIX = "pr/" 

s3 = boto3.client('s3')

def get_remote_files():

    print(f"Attempting to scrape: {SOURCE_URL}")
    req = urllib.request.Request(SOURCE_URL, headers={'User-Agent': USER_AGENT})
    
    try:
        with urllib.request.urlopen(req) as response:
            html = response.read().decode('utf-8')
    except urllib.error.HTTPError as e:
        print(f"Error accessing BLS website: {e.code} - {e.reason}")
        raise

    files = {}
    
    pattern = re.compile(r'href=["\'](.*?)["\']', re.IGNORECASE)
    
    for match in pattern.finditer(html):
        raw_link = match.group(1)
        
        filename = raw_link.split('/')[-1]

        if not filename or filename in ['Name', 'Last modified', 'Size', 'Description', 'Parent Directory']:
            continue
        if raw_link == '../' or raw_link == './':
            continue
        if filename.startswith('?'): 
            continue

        if raw_link.startswith('http'):
            file_url = raw_link
        else:
            if raw_link.startswith('/'):
                domain = '/'.join(SOURCE_URL.split('/')[:3]) 
                file_url = domain + raw_link
            else:
                file_url = SOURCE_URL + filename

        try:
            head_req = urllib.request.Request(file_url, headers={'User-Agent': USER_AGENT}, method='HEAD')
            with urllib.request.urlopen(head_req) as head_res:

                source_etag = head_res.headers.get('ETag')
                last_mod_str = head_res.headers.get('Last-Modified')

                if source_etag:
                    source_etag = source_etag.replace('"', '') 
                
                unique_id = source_etag if source_etag else last_mod_str
                
                files[filename] = {
                    'size': int(head_res.headers.get('Content-Length', 0)),
                    'unique_id': unique_id, 
                    'url': file_url
                }
        except Exception as e:
            print(f"Failed to HEAD {filename}: {e}")
    # print(f"Here are bls files {files}")            
    return files

def get_s3_files():

    s3_files = {}
    paginator = s3.get_paginator('list_objects_v2')
    
    for page in paginator.paginate(Bucket=BUCKET_NAME, Prefix=S3_PREFIX):
        if 'Contents' in page:
            for obj in page['Contents']:
                key = obj['Key']
                filename = key.replace(S3_PREFIX, "")
                s3_files[filename] = {'key': key}
    # print(f"Here are s3_files{s3_files}")  
    return s3_files


def lambda_handler(event, context):
    print("Starting Sync Process...")

    remote_files = get_remote_files()
    print(f"Found {len(remote_files)} files at source.")

    s3_files = get_s3_files()
    print(f"Found {len(s3_files)} files in S3.")

    for filename, remote_meta in remote_files.items():
        s3_key = S3_PREFIX + filename
        should_upload = False
        
        if filename not in s3_files:
            print(f"New file detected: {filename}")
            should_upload = True
        else:
            try:
                s3_obj = s3.head_object(Bucket=BUCKET_NAME, Key=s3_key)
                stored_id = s3_obj.get('Metadata', {}).get('source-unique-id')
                
                if stored_id != remote_meta['unique_id']:
                    print(f"Update detected for {filename} (Source: {remote_meta['unique_id']} vs S3: {stored_id})")
                    should_upload = True
            except Exception as e:
                print(f"Error checking S3 metadata for {filename}: {e}")
                should_upload = True
        
        if should_upload:
            print(f"Uploading {filename}...")
            try:
                with urllib.request.urlopen(urllib.request.Request(remote_meta['url'], headers={'User-Agent': USER_AGENT})) as response:
                    s3.upload_fileobj(
                        response, 
                        BUCKET_NAME, 
                        s3_key,
                        ExtraArgs={
                            'Metadata': {
                                'source-unique-id': str(remote_meta['unique_id']) 
                            }
                        }
                    )
            except Exception as e:
                 print(f"Failed to upload {filename}: {e}")

    for filename in s3_files:
        if filename not in remote_files:
            print(f"File deleted at source. Removing from S3: {filename}")
            s3.delete_object(Bucket=BUCKET_NAME, Key=S3_PREFIX + filename)

    print("Sync Complete.")