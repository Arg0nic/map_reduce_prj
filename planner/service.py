from libs.models import JobUploadedEvent, TaskCompletedEvent, TaskType
from planner.finalizer import finalize_job
from planner.state import JobPlanState
from planner.task_planner import create_map_tasks_for_job, create_reduce_tasks_for_job


class PlannerService:
    '''
    Coordinates the lifecycle of active MapReduce jobs.

    The service owns in-memory job progress, starts reduce after all map tasks
    complete, and finalizes the job after all reduce tasks complete.
    '''

    def __init__(self, job_states: dict[str, JobPlanState] | None = None):
        '''
        Creates a planner service.

        State is injectable so tests and future persistent stores can control
        the planner's view of active jobs.
        '''
        self.job_states = job_states if job_states is not None else {}

    def handle_job_uploaded(self, ch, event: JobUploadedEvent) -> None:
        '''
        Plans map tasks for a newly uploaded job.

        A new uploaded job starts with one map task per uploaded chunk.
        '''
        tasks = create_map_tasks_for_job(ch, event)
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
        state.reduce_task_ids = {task.task_id for task in tasks}
        state.reduce_started = True
        print(f"[Planner] planned {len(tasks)} reduce tasks for job {job_id}")

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
        if event.task_type == TaskType.MAP:
            self.handle_map_completed(ch, event)
        elif event.task_type == TaskType.REDUCE:
            self.handle_reduce_completed(event)
        else:
            print(f"[Planner] unknown completed task type {event.task_type}, ack and skip")
