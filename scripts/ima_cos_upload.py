#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ima 知识库文件上传辅助脚本（腾讯云 COS，使用官方 cos-python-sdk-v5）。

用法:
  python ima_cos_upload.py --credential <cos_credential.json> --file <path> --content-type text/html

说明:
  - 入参 cos_credential 为 ima create_media 返回的 cos_credential 对象（JSON 文件）。
  - 仅做 HTTP PUT 上传文件字节到 COS；create_media / add_knowledge 由调用方（Agent/MCP）完成。
  - 依赖 qcloud_cos（装在托管 venv 中）。
"""
import argparse
import json
import sys

from qcloud_cos import CosConfig, CosS3Client


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--credential", required=True, help="cos_credential JSON 文件路径")
    ap.add_argument("--file", required=True, help="待上传的本地文件路径")
    ap.add_argument("--content-type", default="text/html")
    args = ap.parse_args()

    with open(args.credential, "r", encoding="utf-8") as f:
        cred = json.load(f)

    with open(args.file, "rb") as f:
        body = f.read()

    config = CosConfig(
        Region=cred["region"],
        SecretId=cred["secret_id"],
        SecretKey=cred["secret_key"],
        Token=cred["token"],
        Scheme="https",
    )
    client = CosS3Client(config)

    try:
        resp = client.put_object(
            Bucket=cred["bucket_name"],
            Body=body,
            Key=cred["cos_key"],
            ContentType=args.content_type,
        )
    except Exception as e:  # noqa: BLE001
        print(json.dumps({"ok": False, "stage": "put", "error": str(e)}, ensure_ascii=False))
        sys.exit(2)

    etag = resp.get("ETag")
    print(json.dumps({
        "ok": True,
        "status": 200,
        "bucket": cred["bucket_name"],
        "key": cred["cos_key"],
        "etag": etag,
        "bytes": len(body),
    }, ensure_ascii=False))
    sys.exit(0)


if __name__ == "__main__":
    main()
