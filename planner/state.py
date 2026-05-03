from dataclasses import dataclass, field


@dataclass
class JobPlanState:
    '''
    Stores the planner's in-memory progress record for one active job.

    It is intentionally small until job progress moves to a real database.
    '''
    bucket: str
    map_task_ids: set[str]
    completed_map_task_ids: set[str] = field(default_factory=set)
    reduce_task_ids: set[str] = field(default_factory=set)
    completed_reduce_task_ids: set[str] = field(default_factory=set)
    reduce_started: bool = False
    done: bool = False
