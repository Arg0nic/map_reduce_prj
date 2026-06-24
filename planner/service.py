import logging
import time

from libs.job_repository import AbstractJobRepository
from libs.logging_config import format_log_fields
from libs.models import JobStatus, JobUploadedEvent, TaskCompletedEvent, TaskType
from libs.task_repository import AbstractTaskRepository
from planner.finalizer import finalize_job
from planner.task_planner import create_map_tasks_for_job, create_reduce_tasks_for_job


TASK_STATUS_COMPLETED = "completed"
logger = logging.getLogger(__name__)


class PlannerService:
    '''
    Coordinates the lifecycle of active MapReduce jobs.

    Task progress is derived from the task repository so planner can continue
    phase transitions after a restart.
    '''

    def __init__(
        self,
        task_repository: AbstractTaskRepository | None = None,
        job_repository: AbstractJobRepository | None = None,
    ):
        '''
        Creates a planner service.

        Task repository is the source of truth for planner-visible progress.
        '''
        self.task_repository = task_repository
        self.job_repository = job_repository

    def _require_task_repository(self) -> AbstractTaskRepository:
        if self.task_repository is None:
            raise RuntimeError("Task repository is required to coordinate planner state.")
        return self.task_repository

    def list_tasks_for_job(self, job_id: str) -> list[dict]:
        return self._require_task_repository().list_tasks_for_job(job_id)

    def task_type_value(self, task: dict) -> str | None:
        task_type = task.get("type") or task.get("task_type")
        if isinstance(task_type, TaskType):
            return task_type.value
        return task_type

    def tasks_of_type(self, tasks: list[dict], task_type: TaskType) -> list[dict]:
        return [task for task in tasks if self.task_type_value(task) == task_type.value]

    def find_task(self, tasks: list[dict], task_id: str, task_type: TaskType) -> dict | None:
        for task in tasks:
            if task.get("task_id") == task_id and self.task_type_value(task) == task_type.value:
                return task
        return None

    def completed_count(self, tasks: list[dict]) -> int:
        return sum(1 for task in tasks if task.get("status") == TASK_STATUS_COMPLETED)

    def record_tasks_published(self, tasks) -> None:
        self._require_task_repository().record_tasks_published(tasks)

    def record_task_completed(self, event: TaskCompletedEvent) -> None:
        self._require_task_repository().mark_task_completed(event)

    def record_task_started(self, task: dict, worker_id: str, started_at: float) -> None:
        self._require_task_repository().mark_task_started(task, worker_id=worker_id, started_at=started_at)

    def record_task_failed(self, task: dict, message: str, event_type: str = "failed") -> None:
        self._require_task_repository().mark_task_failed(task, message=message, event_type=event_type)

    def mark_job_processing(self, job_id: str, planner_status: str, message: str) -> None:
        if self.job_repository is not None:
            self.job_repository.update(
                job_id,
                {
                    "status": JobStatus.PROCESSING.value,
                    "planner_status": planner_status,
                    "planner_message": message,
                },
            )

    def mark_job_failed(self, job_id: str, message: str, completed_at: float | None = None) -> None:
        if self.job_repository is not None:
            self.job_repository.update(
                job_id,
                {
                    "status": JobStatus.FAILED.value,
                    "completed_at": completed_at if completed_at is not None else time.time(),
                    "planner_status": "failed",
                    "planner_message": message,
                },
            )

    def is_job_failed(self, job_id: str) -> bool:
        if self.job_repository is None:
            return False

        job = self.job_repository.load(job_id)
        if job is None:
            return False

        return job.get("status") == JobStatus.FAILED.value

    def is_job_done(self, job_id: str) -> bool:
        if self.job_repository is None:
            return False

        job = self.job_repository.load(job_id)
        if job is None:
            return False

        return job.get("status") == JobStatus.DONE.value

    def is_job_finished(self, job_id: str) -> bool:
        return self.is_job_done(job_id) or self.is_job_failed(job_id)

    def handle_job_uploaded(self, ch, event: JobUploadedEvent) -> None:
        '''
        Plans map tasks for a newly uploaded job.

        A new uploaded job starts with one map task per uploaded chunk.
        '''
        existing_tasks = self.list_tasks_for_job(event.job_id)
        if existing_tasks:
            logger.info("job already has planned tasks, ack and skip %s", format_log_fields(job_id=event.job_id))
            return

        tasks = create_map_tasks_for_job(ch, event)
        self.record_tasks_published(tasks)
        self.mark_job_processing(
            event.job_id,
            planner_status="map_running",
            message=f"Planner published {len(tasks)} map tasks.",
        )
        logger.info("planned map tasks %s", format_log_fields(job_id=event.job_id, task_count=len(tasks)))

    def start_reduce_phase(self, ch, job_id: str, bucket: str) -> None:
        '''
        Creates reduce tasks for a job whose map phase is complete.

        Reduce tasks are created only after every map task has uploaded its
        shuffle output, so reduce workers can read complete partition data.
        '''
        existing_tasks = self.list_tasks_for_job(job_id)
        if self.tasks_of_type(existing_tasks, TaskType.REDUCE):
            logger.info("reduce phase already planned, ack and skip %s", format_log_fields(job_id=job_id))
            return

        tasks = create_reduce_tasks_for_job(ch, job_id, bucket)
        self.record_tasks_published(tasks)
        self.mark_job_processing(
            job_id,
            planner_status="reduce_running",
            message=f"Planner published {len(tasks)} reduce tasks.",
        )
        logger.info("planned reduce tasks %s", format_log_fields(job_id=job_id, task_count=len(tasks)))

    def handle_worker_heartbeat(self, heartbeat: dict) -> None:
        '''
        Records the task currently reported by a live worker heartbeat.
        '''
        current_task = heartbeat.get("current_task")
        if not isinstance(current_task, dict):
            return

        worker_id = heartbeat.get("worker_id")
        started_at = current_task.get("started_at")
        if not worker_id or started_at is None:
            return

        task = {
            "job_id": current_task.get("job_id"),
            "task_id": current_task.get("task_id"),
            "type": current_task.get("type") or current_task.get("task_type"),
            "part_num": current_task.get("part_num"),
        }
        self.record_task_started(task, worker_id=worker_id, started_at=started_at)
        logger.info(
            "heartbeat reports running task %s",
            format_log_fields(
                job_id=task.get("job_id"),
                task_id=task.get("task_id"),
                task_type=task.get("type"),
                worker_id=worker_id,
                part_num=task.get("part_num"),
            ),
        )

    def handle_map_completed(self, ch, event: TaskCompletedEvent) -> None:
        '''
        Records a completed map task and starts reduce when all maps are done.

        Completion events may be delivered more than once, so current progress
        is checked in the task repository before changing phase.
        '''
        tasks = self.list_tasks_for_job(event.job_id)
        task = self.find_task(tasks, event.task_id, TaskType.MAP)

        if task is None:
            logger.warning(
                "unknown map task completion, ack and skip %s",
                format_log_fields(job_id=event.job_id, task_id=event.task_id),
            )
            return

        if task.get("status") == TASK_STATUS_COMPLETED:
            logger.info(
                "duplicate map completion %s",
                format_log_fields(job_id=event.job_id, task_id=event.task_id),
            )
        else:
            self.record_task_completed(event)
            tasks = self.list_tasks_for_job(event.job_id)

        map_tasks = self.tasks_of_type(tasks, TaskType.MAP)
        reduce_tasks = self.tasks_of_type(tasks, TaskType.REDUCE)
        completed_maps = self.completed_count(map_tasks)
        logger.info(
            "map task completed %s",
            format_log_fields(
                job_id=event.job_id,
                task_id=event.task_id,
                completed_maps=completed_maps,
                total_maps=len(map_tasks),
            ),
        )

        if map_tasks and completed_maps == len(map_tasks) and not reduce_tasks:
            logger.info("all map tasks completed, starting reduce phase %s", format_log_fields(job_id=event.job_id))
            self.start_reduce_phase(ch, event.job_id, event.bucket)

    def handle_reduce_completed(self, event: TaskCompletedEvent) -> None:
        '''
        Records a completed reduce task and finalizes the job when all reduces are done.

        The last reduce completion is the point where planner can build the
        final result object and mark the job as done for the API.
        '''
        tasks = self.list_tasks_for_job(event.job_id)
        task = self.find_task(tasks, event.task_id, TaskType.REDUCE)

        if task is None:
            logger.warning(
                "unknown reduce task completion, ack and skip %s",
                format_log_fields(job_id=event.job_id, task_id=event.task_id),
            )
            return

        if task.get("status") == TASK_STATUS_COMPLETED:
            logger.info(
                "duplicate reduce completion %s",
                format_log_fields(job_id=event.job_id, task_id=event.task_id),
            )
        else:
            self.record_task_completed(event)
            tasks = self.list_tasks_for_job(event.job_id)

        reduce_tasks = self.tasks_of_type(tasks, TaskType.REDUCE)
        completed_reduces = self.completed_count(reduce_tasks)
        logger.info(
            "reduce task completed %s",
            format_log_fields(
                job_id=event.job_id,
                task_id=event.task_id,
                completed_reduces=completed_reduces,
                total_reduces=len(reduce_tasks),
            ),
        )

        if reduce_tasks and completed_reduces == len(reduce_tasks) and not self.is_job_done(event.job_id):
            final_result_key = finalize_job(event.job_id, event.bucket)
            logger.info(
                "all reduce tasks completed, finalized job %s",
                format_log_fields(job_id=event.job_id, result_key=final_result_key),
            )

    def handle_task_completed(self, ch, event: TaskCompletedEvent) -> None:
        '''
        Routes a generic worker completion event to its phase-specific handler.

        Map and reduce completion events share one queue; task_type chooses
        which phase-specific handler should process the event.
        '''
        if self.is_job_finished(event.job_id):
            logger.info("completion for finished job, ack and skip %s", format_log_fields(job_id=event.job_id))
            return

        if event.task_type == TaskType.MAP:
            self.handle_map_completed(ch, event)
        elif event.task_type == TaskType.REDUCE:
            self.handle_reduce_completed(event)
        else:
            logger.warning(
                "unknown completed task type, ack and skip %s",
                format_log_fields(job_id=event.job_id, task_id=event.task_id, task_type=event.task_type),
            )

    def handle_task_dead(self, task: dict) -> None:
        '''
        Records a task that exhausted worker retries and reached the dead queue.
        '''
        job_id = task.get("job_id")
        task_id = task.get("task_id")
        task_type = task.get("type")
        if not job_id:
            raise ValueError("Dead task message is missing job_id.")
        if not task_id:
            raise ValueError("Dead task message is missing task_id.")
        if not task_type:
            raise ValueError("Dead task message is missing type.")

        message = f"Task {task_id} reached dead queue after worker retries."
        self.record_task_failed(task, message, event_type="dead_lettered")
        self.mark_job_failed(job_id, message)

        logger.warning(
            "marked job failed because task reached dead queue %s",
            format_log_fields(job_id=job_id, task_id=task_id, task_type=task_type),
        )

    def fail_timed_out_tasks(self, timeout_seconds: float, now: float | None = None) -> int:
        '''
        Fails running tasks that have exceeded the allowed execution time.
        '''
        if self.task_repository is None:
            return 0

        current_time = now if now is not None else time.time()
        cutoff_timestamp = current_time - timeout_seconds
        timed_out_tasks = self.task_repository.list_timed_out_running_tasks(cutoff_timestamp)

        for task in timed_out_tasks:
            job_id = task.get("job_id")
            task_id = task.get("task_id")
            if not job_id or not task_id:
                continue

            message = f"Task {task_id} timed out after {timeout_seconds:g} seconds."
            self.record_task_failed(task, message, event_type="timed_out")
            self.mark_job_failed(job_id, message, completed_at=current_time)

            logger.warning(
                "marked job failed because task timed out %s",
                format_log_fields(job_id=job_id, task_id=task_id, timeout_seconds=timeout_seconds),
            )

        return len(timed_out_tasks)
