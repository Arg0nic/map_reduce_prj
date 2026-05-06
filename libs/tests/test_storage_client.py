from io import BytesIO

import pytest

import libs.storage_client.client as storage_client


class FakePaginator:
    def __init__(self, pages):
        self.pages = pages
        self.calls = []

    def paginate(self, Bucket, Prefix):
        self.calls.append((Bucket, Prefix))
        return self.pages


class FakeS3Client:
    def __init__(self):
        self.uploads = []
        self.put_objects = []
        self.downloads = []
        self.presigned = []
        self.paginator = FakePaginator([])

    def upload_file(self, local_path, bucket, key):
        self.uploads.append((local_path, bucket, key))

    def put_object(self, **kwargs):
        self.put_objects.append(kwargs)

    def get_object(self, Bucket, Key):
        return {"Body": BytesIO(b"object-bytes")}

    def download_file(self, bucket, key, local_path):
        self.downloads.append((bucket, key, local_path))

    def generate_presigned_url(self, operation, Params, ExpiresIn):
        self.presigned.append((operation, Params, ExpiresIn))
        return "https://example.test/presigned"

    def get_paginator(self, name):
        assert name == "list_objects_v2"
        return self.paginator


def test_get_s3_client_creates_and_caches_configured_client(monkeypatch: pytest.MonkeyPatch) -> None:
    created = []
    monkeypatch.setattr(storage_client, "_client", None)

    def fake_boto3_client(service_name, **kwargs):
        client = object()
        created.append((service_name, kwargs, client))
        return client

    monkeypatch.setattr(storage_client.boto3, "client", fake_boto3_client)

    first = storage_client.get_s3_client()
    second = storage_client.get_s3_client()

    assert first is second
    assert len(created) == 1
    service_name, kwargs, _ = created[0]
    assert service_name == "s3"
    assert kwargs["endpoint_url"] == storage_client.settings.MINIO_ENDPOINT
    assert kwargs["aws_access_key_id"] == storage_client.settings.MINIO_ACCESS_KEY
    assert kwargs["aws_secret_access_key"] == storage_client.settings.MINIO_SECRET_KEY
    assert kwargs["use_ssl"] == storage_client.settings.MINIO_SECURE


def test_upload_file_creates_parent_directory_and_uploads(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    fake_s3 = FakeS3Client()
    monkeypatch.setattr(storage_client, "get_s3_client", lambda: fake_s3)
    local_path = tmp_path / "nested" / "file.txt"

    storage_client.upload_file(str(local_path), bucket="bucket-1", key="key-1")

    assert local_path.parent.is_dir()
    assert fake_s3.uploads == [(str(local_path), "bucket-1", "key-1")]


def test_upload_bytes_puts_object_with_content_metadata(monkeypatch: pytest.MonkeyPatch) -> None:
    fake_s3 = FakeS3Client()
    monkeypatch.setattr(storage_client, "get_s3_client", lambda: fake_s3)

    storage_client.upload_bytes(b"hello", bucket="bucket-1", key="key-1", content_type="text/plain")

    assert fake_s3.put_objects == [
        {
            "Bucket": "bucket-1",
            "Key": "key-1",
            "Body": b"hello",
            "ContentLength": 5,
            "ContentType": "text/plain",
        },
    ]


def test_read_object_bytes_reads_response_body(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(storage_client, "get_s3_client", lambda: FakeS3Client())

    assert storage_client.read_object_bytes("bucket-1", "key-1") == b"object-bytes"


def test_download_file_creates_parent_directory_and_downloads(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    fake_s3 = FakeS3Client()
    monkeypatch.setattr(storage_client, "get_s3_client", lambda: fake_s3)
    local_path = tmp_path / "nested" / "file.txt"

    storage_client.download_file("bucket-1", "key-1", str(local_path))

    assert local_path.parent.is_dir()
    assert fake_s3.downloads == [("bucket-1", "key-1", str(local_path))]


def test_generate_presigned_url_delegates_to_client(monkeypatch: pytest.MonkeyPatch) -> None:
    fake_s3 = FakeS3Client()
    monkeypatch.setattr(storage_client, "get_s3_client", lambda: fake_s3)

    url = storage_client.generate_presigned_url("bucket-1", "key-1", expires_in=60)

    assert url == "https://example.test/presigned"
    assert fake_s3.presigned == [
        ("get_object", {"Bucket": "bucket-1", "Key": "key-1"}, 60),
    ]


def test_list_objects_collects_keys_from_all_pages(monkeypatch: pytest.MonkeyPatch) -> None:
    fake_s3 = FakeS3Client()
    fake_s3.paginator = FakePaginator(
        [
            {"Contents": [{"Key": "prefix/a"}, {"Key": "prefix/b"}]},
            {},
            {"Contents": [{"Key": "prefix/c"}]},
        ]
    )
    monkeypatch.setattr(storage_client, "get_s3_client", lambda: fake_s3)

    keys = storage_client.list_objects("bucket-1", "prefix/")

    assert keys == ["prefix/a", "prefix/b", "prefix/c"]
    assert fake_s3.paginator.calls == [("bucket-1", "prefix/")]
