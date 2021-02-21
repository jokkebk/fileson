#!/usr/bin/env python3
import boto3

from crypt import AESFile

import argparse, time

parser = argparse.ArgumentParser(description='AWS S3 upload/download with on-the-fly encryption')
parser.add_argument('mode', type=str, choices=['upload','download'], help='Mode')
parser.add_argument('bucket', type=str, help='S3 bucket')
parser.add_argument('input', type=str, help='Input file or S3 object name')
parser.add_argument('output', type=str, help='Output file or S3 object name')
parser.add_argument('password', type=str, help='Password')
parser.add_argument('salt', type=str, help='Salt')
parser.add_argument('-i', '--iterations', type=int, default=100000,
        help='PBKDF2 iterations (default 100000)')
args = parser.parse_args()

s3 = boto3.client('s3')

key = AESFile.key(args.password, args.salt, args.iterations)

if args.mode == 'upload':
    fp = AESFile(args.input, 'rb', key)
    s3.upload_fileobj(fp, args.bucket, args.output)
else:
    fp = AESFile(args.output, 'wb', key)
    s3.download_fileobj(args.bucket, args.input, fp)

fp.close()
