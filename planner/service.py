import time

from libs.job_repository import AbstractJobRepository
from libs.models import JobStatus, JobUploadedEvent, TaskCompletedEvent, TaskType
from libs.task_repository import AbstractTaskRepository
from planner.finalizer import finalize_job
from planner.state import JobPlanState
from planner.task_planner import create_map_tasks_for_job, create_reduce_tasks_for_job


class PlannerService:
    '''
    Coordinates the lifecycle of active MapReduce jobs.

    The service owns in-memory job progress, starts reduce after all map tasks
    complete, and finalizes the job after all reduce tasks complete.
    '''

    def __init__(
        self,
        job_states: dict[str, JobPlanState] | None = None,
        task_repository: AbstractTaskRepository | None = None,
        job_repository: AbstractJobRepository | None = None,
    ):
        '''
        Creates a planner service.

        State is injectable so tests and future persistent stores can control
        the planner's view of active jobs.
        '''
        self.job_states = job_states if job_states is not None else {}
        self.task_repository = task_repository
        self.job_repository = job_repository

    def record_tasks_published(self, tasks) -> None:
        if self.task_repository is not None:
            self.task_repository.record_tasks_published(tasks)

    def record_task_completed(self, event: TaskCompletedEvent) -> None:
        if self.task_repository is not None:
            self.task_repository.mark_task_completed(event)

    def record_task_started(self, task: dict, worker_id: str, started_at: float) -> None:
        if self.task_repository is not None:
            self.task_repository.mark_task_started(task, worker_id=worker_id, started_at=started_at)

    def record_task_failed(self, task: dict, message: str, event_type: str = "failed") -> None:
        if self.task_repository is not None:
            self.task_repository.mark_task_failed(task, message=message, event_type=event_type)

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

    def handle_job_uploaded(self, ch, event: JobUploadedEvent) -> None:
        '''
        Plans map tasks for a newly uploaded job.

        A new uploaded job starts with one map task per uploaded chunk.
        '''
        tasks = create_map_tasks_for_job(ch, event)
        self.record_tasks_published(tasks)
        print(f"[Planner] planned {len(tasks)} map tasks for job {event.job_id}")
        self.job_states[event.job_id] = JobPlanState(
            bucket=event.bucket,
            map_task_ids={task.task_id for task in tasks},
        )

    def start_reduce_phase(self, ch, job_id: str, state: JobPlanState) -> None:
        '''
        Creates reduce tasks for a job whose map phase is complete.

        Reduce tasks are created only after every map task has uploaded its
        shuffle output, so reduce workers can read complete partition data.
        '''
        tasks = create_reduce_tasks_for_job(ch, job_id, state.bucket)
        self.record_tasks_published(tasks)
        state.reduce_task_ids = {task.task_id for task in tasks}
        state.reduce_started = True
        print(f"[Planner] planned {len(tasks)} reduce tasks for job {job_id}")

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
        print(f"[Planner] heartbeat reports task {task['task_id']} running on worker {worker_id}")

    def handle_map_completed(self, ch, event: TaskCompletedEvent) -> None:
        '''
        Records a completed map task and starts reduce when all maps are done.

        Completion events may be delivered more than once, so task ids are
        recorded in sets and duplicates do not advance the phase twice.
        '''
        state = self.job_states.get(event.job_id)
        if state is None:
            print(f"[Planner] completion for unknown job {event.job_id}, ack and skip")
            return

        if event.task_id not in state.map_task_ids:
            print(f"[Planner] unknown map task {event.task_id} for job {event.job_id}, ack and skip")
            return

        if event.task_id in state.completed_map_task_ids:
            print(f"[Planner] duplicate map completion {event.task_id} for job {event.job_id}")
        else:
            self.record_task_completed(event)
            state.completed_map_task_ids.add(event.task_id)
            print(
                f"[Planner] map completed for job {event.job_id}: "
                f"{len(state.completed_map_task_ids)}/{len(state.map_task_ids)}"
            )

        if len(state.completed_map_task_ids) == len(state.map_task_ids) and not state.reduce_started:
            print(f"[Planner] all map tasks completed for job {event.job_id}. Starting reduce phase.")
            self.start_reduce_phase(ch, event.job_id, state)

    def handle_reduce_completed(self, event: TaskCompletedEvent) -> None:
        '''
        Records a completed reduce task and finalizes the job when all reduces are done.

        The last reduce completion is the point where planner can build the
        final result object and mark the job as done for the API.
        '''
        state = self.job_states.get(event.job_id)
        if state is None:
            print(f"[Planner] completion for unknown job {event.job_id}, ack and skip")
            return

        if event.task_id not in state.reduce_task_ids:
            print(f"[Planner] unknown reduce task {event.task_id} for job {event.job_id}, ack and skip")
            return

        if event.task_id in state.completed_reduce_task_ids:
            print(f"[Planner] duplicate reduce completion {event.task_id} for job {event.job_id}")
        else:
            self.record_task_completed(event)
            state.completed_reduce_task_ids.add(event.task_id)
            print(
                f"[Planner] reduce completed for job {event.job_id}: "
                f"{len(state.completed_reduce_task_ids)}/{len(state.reduce_task_ids)}"
            )

        if len(state.completed_reduce_task_ids) == len(state.reduce_task_ids) and not state.done:
            final_result_key = finalize_job(event.job_id, state.bucket)
            state.done = True
            print(f"[Planner] all reduce tasks completed for job {event.job_id}. Result: {final_result_key}")

    def handle_task_completed(self, ch, event: TaskCompletedEvent) -> None:
        '''
        Routes a generic worker completion event to its phase-specific handler.

        Map and reduce completion events share one queue; task_type chooses
        which phase-specific handler should process the event.
        '''
        state = self.job_states.get(event.job_id)
        if state is not None and state.done:
            print(f"[Planner] completion for already finished job {event.job_id}, ack and skip")
            return

        if self.is_job_failed(event.job_id):
            print(f"[Planner] completion for failed job {event.job_id}, ack and skip")
            return

        if event.task_type == TaskType.MAP:
            self.handle_map_completed(ch, event)
        elif event.task_type == TaskType.REDUCE:
            self.handle_reduce_completed(event)
        else:
            print(f"[Planner] unknown completed task type {event.task_type}, ack and skip")

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

        state = self.job_states.get(job_id)
        if state is not None:
            state.done = True

        print(f"[Planner] marked job {job_id} failed because task {task_id} reached dead queue")

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

            state = self.job_states.get(job_id)
            if state is not None:
                state.done = True

            print(f"[Planner] marked job {job_id} failed because task {task_id} timed out")

        return len(timed_out_tasks)
