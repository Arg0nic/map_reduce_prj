import hashlib
import os
import time
import uuid
from typing import BinaryIO, Callable, Iterator, Protocol

from api_gateway.publisher import RabbitJobEventPublisher
from libs.job_repository import AbstractJobRepository, LocalJsonJobRepository
from libs.models import ChunkInfo, Job, JobStatus, JobUploadedEvent
from libs.storage_client.client import upload_bytes
from libs.storage_client.config import settings
from libs.storage_client.paths import chunks_prefix


DEFAULT_BUCKET = settings.DEFAULT_BUCKET or "mapreduce-data"
DEFAULT_CHUNK_SIZE = 8 * 1024 * 1024


class JobEventPublisher(Protocol):
    def publish_job_uploaded(self, event: JobUploadedEvent) -> None:
        pass


def _get_safe_filename(filename: str) -> str:
    # Keep only the filename, so a client cannot pass a path like "../../file.txt".
    return os.path.basename(filename) or "input.txt"


def _get_chunks_prefix(job_id: str) -> str:
    return chunks_prefix(job_id)


class ChunkUploader:
    """Splits an input file into line-safe chunks and uploads them to object storage."""

    def __init__(
        self,
        bucket: str = DEFAULT_BUCKET,
        max_chunk_size: int = DEFAULT_CHUNK_SIZE,
        upload_func: Callable[..., None] = upload_bytes,
    ):
        self.bucket = bucket
        self.max_chunk_size = max_chunk_size
        self.upload_func = upload_func

    def iter_text_chunks(
        self,
        file_obj: BinaryIO,
        max_chunk_size: int | None = None,
    ) -> Iterator[bytes]:
        # NOTE: If one input line is larger than max_chunk_size, it becomes one oversized chunk.
        active_chunk_size = max_chunk_size or self.max_chunk_size
        chunk = bytearray()

        for line in file_obj:
            # Yield only before adding the next line, so chunks do not split lines.
            if chunk and len(chunk) + len(line) > active_chunk_size:
                yield bytes(chunk)
                chunk.clear()

            chunk.extend(line)

        if chunk:
            # Send the remaining bytes as the last chunk.
            yield bytes(chunk)

    def upload_chunks(
        self,
        job_id: str,
        file_obj: BinaryIO,
        bucket: str | None = None,
        max_chunk_size: int | None = None,
    ) -> dict:
        active_bucket = bucket or self.bucket
        chunks = []
        total_bytes = 0

        for part_index, chunk in enumerate(
            self.iter_text_chunks(file_obj, max_chunk_size=max_chunk_size)
        ):
            # This key ties every uploaded chunk to the parent job_id.
            key = f"{_get_chunks_prefix(job_id)}part_{part_index:05d}.txt"
            size_bytes = len(chunk)

            self.upload_func(chunk, bucket=active_bucket, key=key, content_type="text/plain")

            chunk_info = ChunkInfo(
                part_index=part_index,
                key=key,
                size_bytes=size_bytes,
                sha256=hashlib.sha256(chunk).hexdigest(),
            )
            chunks.append(chunk_info.model_dump(mode="json"))

            total_bytes += size_bytes

        if not chunks:
            raise ValueError("Input file is empty.")

        # Manifest tells Planner which S3 objects belong to this job.
        return {
            "chunk_count": len(chunks),
            "total_bytes": total_bytes,
            "chunks": chunks,
        }



class JobService:
    """Coordinates chunk upload and job metadata creation."""

    def __init__(
        self,
        repository: AbstractJobRepository | None = None,
        uploader: ChunkUploader | None = None,
        event_publisher: JobEventPublisher | None = None,
        bucket: str = DEFAULT_BUCKET,
    ):
        self.repository = repository or LocalJsonJobRepository()
        self.uploader = uploader or ChunkUploader(bucket=bucket)
        self.event_publisher = event_publisher or RabbitJobEventPublisher()
        self.bucket = bucket

    def create_from_chunks_manifest(
        self,
        job_id: str,
        original_filename: str,
        bucket: str,
        manifest: dict,
    ) -> dict:
        # This job is the top-level record for one uploaded input file.
        job = Job(
            job_id=job_id,
            status=JobStatus.UPLOADED,
            original_filename=_get_safe_filename(original_filename),
            storage="minio",
            bucket=bucket,
            chunk_count=manifest["chunk_count"],
            total_bytes=manifest["total_bytes"],
            chunks=manifest["chunks"],
            submitted_at=time.time(),
            completed_at=None,
            planner_status="pending",
            planner_message="Chunks uploaded. Planner integration is not wired yet.",
        )

        return self.repository.save(job.model_dump(mode="json"))

    def create_from_upload(
        self,
        file_obj: BinaryIO,
        original_filename: str | None,
        bucket: str | None = None,
    ) -> dict:
        active_bucket = bucket or self.bucket

        # One job_id groups the input file, its chunks, worker tasks, and final result.
        job_id = str(uuid.uuid4())
        manifest = self.uploader.upload_chunks(
            job_id=job_id,
            file_obj=file_obj,
            bucket=active_bucket,
        )

        job = self.create_from_chunks_manifest(
            job_id=job_id,
            original_filename=original_filename or "input.txt",
            bucket=active_bucket,
            manifest=manifest,
        )

        event = JobUploadedEvent(
            job_id=job_id,
            bucket=active_bucket,
            chunks_prefix=_get_chunks_prefix(job_id),
            created_at=job["submitted_at"],
        )
        self.event_publisher.publish_job_uploaded(event)

        return job
    
    def get_job(self, job_id: str) -> dict | None:
        return self.repository.load(job_id)


# _TODO:
# 1) integration with DB
# 2) update storage_client
