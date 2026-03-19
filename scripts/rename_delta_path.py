#!/usr/bin/env python3
"""
Rename (move) a Delta table path by copying all objects from source prefix to destination prefix
and optionally deleting the source objects. Designed for S3/MinIO (`s3://` or `s3a://`) and local
filesystem paths. The user edits parameters below to set `SOURCE_PATH` and `DEST_PATH`.

Usage: edit the variables below and run in the repo virtualenv:
  .venv/bin/python scripts/rename_delta_path.py

Notes:
- For S3/MinIO, configure `AWS_ENDPOINT_URL`, `AWS_ACCESS_KEY_ID`, and `AWS_SECRET_ACCESS_KEY`
  by editing this file or by using environment variables (preferred).
- The script defaults to `DRY_RUN=True` to avoid accidental deletes; change to False to apply.
"""

from __future__ import annotations
import os
import sys
import shutil
from urllib.parse import urlparse
from typing import Tuple

# --- User-editable parameters -------------------------------------------------
# Source and destination paths. Examples:
#  - S3: s3a://delta-lake/bronze/
#  - Local: /tmp/delta/bronze/
SOURCE_PATH = "s3a://delta-lake/bronze/"
DEST_PATH = "s3a://delta-lake/bronze_uae_ean_hotelbeds/"

# If True: perform a dry-run (list and show what would be copied/deleted).
DRY_RUN = True

# If True: actually copy objects (not just list) but DO NOT delete the source.
# This is the 'copy-only, keep source' safe option you requested.
# To perform copy-only, set COPY_ONLY = True and keep DRY_RUN=True to skip deletes.
COPY_ONLY = True

# If using MinIO or a custom S3 endpoint, set endpoint URL and credentials here or
# use the environment variables AWS_ENDPOINT_URL, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY.
# Default to MinIO values used by the notebooks (cell 1) when env vars are not set.
# These match the MinIO setup in notebooks/new_cluster_analysis.ipynb (endpoint=http://localhost:9000)
AWS_ENDPOINT_URL = os.environ.get("AWS_ENDPOINT_URL", "http://localhost:9000")
AWS_ACCESS_KEY_ID = os.environ.get("AWS_ACCESS_KEY_ID", "minioadmin")
AWS_SECRET_ACCESS_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY", "minioadmin")
AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")
# -----------------------------------------------------------------------------


def is_s3_path(path: str) -> bool:
    return path.startswith("s3://") or path.startswith("s3a://")


def parse_s3_path(path: str) -> Tuple[str, str]:
    # Returns (bucket, key_prefix)
    p = path
    if p.startswith("s3a://"):
        p = p[6:]
    elif p.startswith("s3://"):
        p = p[5:]
    # strip leading/trailing slashes
    p = p.lstrip("/")
    parts = p.split("/", 1)
    bucket = parts[0]
    prefix = ""
    if len(parts) == 2:
        prefix = parts[1]
    if prefix and not prefix.endswith("/"):
        prefix += "/"
    return bucket, prefix


def ensure_boto3_available():
    try:
        import boto3  # noqa: F401
    except Exception as e:
        print("boto3 is required for S3 operations. Install with: pip install boto3")
        raise


def s3_copy_prefix(
    source: str,
    dest: str,
    dry_run: bool = True,
    endpoint_url: str = None,
    aws_access_key_id: str = None,
    aws_secret_access_key: str = None,
    aws_region: str = None,
) -> Tuple[int, int]:
    """Copy all objects under source prefix to dest prefix. Returns (copied_count, errored_count)."""
    ensure_boto3_available()
    import boto3
    from botocore.exceptions import ClientError

    bucket_src, prefix_src = parse_s3_path(source)
    bucket_dst, prefix_dst = parse_s3_path(dest)

    session_kwargs = {}
    client_kwargs = {}
    if aws_region:
        session_kwargs["region_name"] = aws_region
    if aws_access_key_id and aws_secret_access_key:
        session_kwargs["aws_access_key_id"] = aws_access_key_id
        session_kwargs["aws_secret_access_key"] = aws_secret_access_key
    if endpoint_url:
        client_kwargs["endpoint_url"] = endpoint_url

    session = boto3.Session(**session_kwargs)
    s3 = session.client("s3", **client_kwargs)

    paginator = s3.get_paginator("list_objects_v2")
    page_iter = paginator.paginate(Bucket=bucket_src, Prefix=prefix_src)

    copied = 0
    errors = 0

    for page in page_iter:
        for obj in page.get("Contents", []):
            key = obj["Key"]
            rel_key = key[len(prefix_src) :] if key.startswith(prefix_src) else key
            new_key = prefix_dst + rel_key
            print(f"COPY s3://{bucket_src}/{key} -> s3://{bucket_dst}/{new_key}")
            if not dry_run:
                try:
                    copy_source = {"Bucket": bucket_src, "Key": key}
                    s3.copy_object(
                        Bucket=bucket_dst, Key=new_key, CopySource=copy_source
                    )
                    copied += 1
                except ClientError as ce:
                    print(f"ERROR copying {key}: {ce}")
                    errors += 1
    return copied, errors


def s3_delete_prefix(
    source: str,
    dry_run: bool = True,
    endpoint_url: str = None,
    aws_access_key_id: str = None,
    aws_secret_access_key: str = None,
    aws_region: str = None,
) -> Tuple[int, int]:
    """Delete all objects under source prefix. Returns (deleted_count, errored_count)."""
    ensure_boto3_available()
    import boto3
    from botocore.exceptions import ClientError

    bucket_src, prefix_src = parse_s3_path(source)

    session_kwargs = {}
    client_kwargs = {}
    if aws_region:
        session_kwargs["region_name"] = aws_region
    if aws_access_key_id and aws_secret_access_key:
        session_kwargs["aws_access_key_id"] = aws_access_key_id
        session_kwargs["aws_secret_access_key"] = aws_secret_access_key
    if endpoint_url:
        client_kwargs["endpoint_url"] = endpoint_url

    session = boto3.Session(**session_kwargs)
    s3 = session.client("s3", **client_kwargs)

    paginator = s3.get_paginator("list_objects_v2")
    page_iter = paginator.paginate(Bucket=bucket_src, Prefix=prefix_src)

    deleted = 0
    errors = 0

    # collect up to 1000 keys per delete_objects call
    batch = []
    for page in page_iter:
        for obj in page.get("Contents", []):
            key = obj["Key"]
            print(f"DELETE s3://{bucket_src}/{key}")
            if not dry_run:
                batch.append({"Key": key})
                if len(batch) >= 1000:
                    try:
                        resp = s3.delete_objects(
                            Bucket=bucket_src, Delete={"Objects": batch}
                        )
                        deleted += len(batch)
                        batch = []
                    except ClientError as ce:
                        print(f"ERROR deleting batch: {ce}")
                        errors += 1
    if batch and not dry_run:
        try:
            resp = s3.delete_objects(Bucket=bucket_src, Delete={"Objects": batch})
            deleted += len(batch)
        except ClientError as ce:
            print(f"ERROR deleting final batch: {ce}")
            errors += 1

    return deleted, errors


def local_move(source: str, dest: str, dry_run: bool = True) -> Tuple[int, int]:
    # Source and dest are local filesystem paths; if dest exists, raise or use copytree
    if not source.endswith(os.path.sep):
        source = source + os.path.sep
    if not dest.endswith(os.path.sep):
        dest = dest + os.path.sep
    print(f"LOCAL MOVE: {source} -> {dest}")
    if dry_run:
        # list files
        count = 0
        for root, dirs, files in os.walk(source):
            for f in files:
                srcf = os.path.join(root, f)
                rel = os.path.relpath(srcf, source)
                dstf = os.path.join(dest, rel)
                print(f"MOVE {srcf} -> {dstf}")
                count += 1
        return count, 0
    # perform actual move: copytree then remove source
    try:
        if os.path.exists(dest):
            print(f"Destination {dest} exists; copying contents into it")
            # copy contents
            for item in os.listdir(source):
                s_item = os.path.join(source, item)
                d_item = os.path.join(dest, item)
                if os.path.isdir(s_item):
                    shutil.copytree(s_item, d_item, dirs_exist_ok=True)
                else:
                    shutil.copy2(s_item, d_item)
            # remove source folder
            shutil.rmtree(source)
        else:
            shutil.move(source, dest)
        return 0, 0
    except Exception as e:
        print(f"Error moving local files: {e}")
        return 0, 1


def main():
    src = SOURCE_PATH
    dst = DEST_PATH
    dry = DRY_RUN

    print("Rename Delta path script")
    print(f"SOURCE_PATH={src}")
    print(f"DEST_PATH={dst}")
    print(f"DRY_RUN={dry}")

    if is_s3_path(src) and is_s3_path(dst):
        # S3 path copy
        print("Detected S3 paths. Using boto3 to copy objects.")
        endpoint = AWS_ENDPOINT_URL or None
        ak = AWS_ACCESS_KEY_ID or None
        sk = AWS_SECRET_ACCESS_KEY or None
        region = AWS_REGION or None

        # Handle copy-only mode: perform copies but keep source intact.
        if COPY_ONLY:
            print("COPY_ONLY=True — performing object copies and skipping deletes")
            copied, copy_err = s3_copy_prefix(
                src,
                dst,
                dry_run=False,
                endpoint_url=endpoint,
                aws_access_key_id=ak,
                aws_secret_access_key=sk,
                aws_region=region,
            )
            print(f"Copy completed: copied={copied}, errors={copy_err}")
            print("Source objects left intact (no deletes performed).")
        else:
            copied, copy_err = s3_copy_prefix(
                src,
                dst,
                dry_run=dry,
                endpoint_url=endpoint,
                aws_access_key_id=ak,
                aws_secret_access_key=sk,
                aws_region=region,
            )
            print(f"Copy completed: copied={copied}, errors={copy_err}")
            if not dry:
                deleted, del_err = s3_delete_prefix(
                    src,
                    dry_run=dry,
                    endpoint_url=endpoint,
                    aws_access_key_id=ak,
                    aws_secret_access_key=sk,
                    aws_region=region,
                )
                print(f"Delete completed: deleted={deleted}, errors={del_err}")
            else:
                print(
                    "Dry-run mode: no deletes performed. Run with DRY_RUN=False to delete source objects."
                )
    elif not is_s3_path(src) and not is_s3_path(dst):
        # Local filesystem move
        moved, err = local_move(src, dst, dry_run=dry)
        print(f"Local move listed/moved: {moved}, errors: {err}")
    else:
        print(
            "Source and destination must both be S3 paths or both local paths. Aborting."
        )
        sys.exit(2)


if __name__ == "__main__":
    main()
