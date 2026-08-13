#!/usr/bin/env python3
"""
Download every object from an S3 bucket to a local directory,
preserving the bucket's folder structure.

Usage:
    python download_bucket.py my-bucket-name
    python download_bucket.py my-bucket-name -o ./downloads
    python download_bucket.py my-bucket-name -p some/prefix/ -o ./downloads
    python download_bucket.py my-bucket-name --profile myprofile --region us-east-1

Credentials are resolved the standard boto3 way: environment variables
(AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY), a shared ~/.aws/credentials
profile, or an attached IAM role. Nothing is hard-coded here.
"""

import argparse
import os
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed


def load_dotenv(path=None):
    """Load KEY=VALUE lines from a .env file next to this script into os.environ.
    Existing environment variables are NOT overwritten. No dependency required."""
    if path is None:
        path = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
    if not os.path.exists(path):
        return
    with open(path, encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, value = line.partition("=")
            key = key.strip()
            value = value.strip().strip('"').strip("'")
            os.environ.setdefault(key, value)


load_dotenv()

try:
    import boto3
    from botocore.exceptions import ClientError, NoCredentialsError
except ImportError:
    sys.exit("boto3 is not installed. Run:  pip install boto3")


def parse_args():
    p = argparse.ArgumentParser(
        description="Mirror an S3 bucket to a local folder, preserving structure."
    )
    p.add_argument("bucket", help="Name of the S3 bucket")
    p.add_argument(
        "-o", "--output", default=None,
        help="Local destination directory (default: ./<bucket>)",
    )
    p.add_argument(
        "-p", "--prefix", default="",
        help="Only download objects under this key prefix (e.g. 'images/2024/')",
    )
    p.add_argument("--profile", default=None, help="AWS shared-credentials profile name")
    p.add_argument("--region", default=None, help="AWS region of the bucket")
    p.add_argument(
        "--workers", type=int, default=8,
        help="Number of parallel download threads (default: 8)",
    )
    p.add_argument(
        "--overwrite", action="store_true",
        help="Re-download files even if a same-size local copy already exists",
    )
    return p.parse_args()


def iter_objects(s3, bucket, prefix):
    """Yield every object (dict) under prefix, handling pagination."""
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            yield obj


def download_one(s3, bucket, obj, dest_root, overwrite):
    key = obj["Key"]

    # Keys ending in "/" are "folder" placeholders — just make the dir.
    local_path = os.path.join(dest_root, key)
    if key.endswith("/"):
        os.makedirs(local_path, exist_ok=True)
        return key, "dir"

    os.makedirs(os.path.dirname(local_path) or ".", exist_ok=True)

    # Skip if an identically-sized copy already exists.
    if not overwrite and os.path.exists(local_path):
        if os.path.getsize(local_path) == obj["Size"]:
            return key, "skip"

    s3.download_file(bucket, key, local_path)
    return key, "ok"


def main():
    args = parse_args()
    dest_root =  os.path.join(".", f"downloads/{args.bucket if args.bucket else ''}" )

    session = boto3.Session(profile_name=args.profile, region_name=args.region)
    s3 = session.client("s3")

    try:
        objects = list(iter_objects(s3, args.bucket, args.prefix))
    except NoCredentialsError:
        sys.exit("No AWS credentials found. Configure them via env vars, "
                 "~/.aws/credentials, or --profile.")
    except ClientError as e:
        sys.exit(f"Failed to list bucket '{args.bucket}': {e}")

    if not objects:
        print(f"No objects found in s3://{args.bucket}/{args.prefix}")
        return

    total = len(objects)
    print(f"Found {total} objects in s3://{args.bucket}/{args.prefix or ''}")
    print(f"Downloading into: {os.path.abspath(dest_root)}\n")

    counts = {"ok": 0, "skip": 0, "dir": 0, "error": 0}

    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = {
            pool.submit(download_one, s3, args.bucket, obj, dest_root, args.overwrite): obj
            for obj in objects
        }
        for i, fut in enumerate(as_completed(futures), 1):
            key = futures[fut]["Key"]
            try:
                _, status = fut.result()
                counts[status] += 1
                tag = {"ok": "↓", "skip": "=", "dir": "+"}[status]
            except Exception as e:  # noqa: BLE001
                counts["error"] += 1
                tag = "✗"
                print(f"[{i}/{total}] {tag} {key}  ERROR: {e}", file=sys.stderr)
                continue
            print(f"[{i}/{total}] {tag} {key}")

    print(
        f"\nDone. downloaded={counts['ok']} skipped={counts['skip']} "
        f"dirs={counts['dir']} errors={counts['error']}"
    )
    if counts["error"]:
        sys.exit(1)


if __name__ == "__main__":
    main()
