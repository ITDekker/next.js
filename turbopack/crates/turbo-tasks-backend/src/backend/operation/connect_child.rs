use serde::{Deserialize, Serialize};
use turbo_tasks::TaskId;

use super::{ExecuteContext, Operation};
use crate::data::{CachedDataItem, CachedDataItemKey};

#[derive(Serialize, Deserialize, Clone, Default)]
pub enum ConnectChildOperation {
    ScheduleTask {
        task_id: TaskId,
    },
    #[default]
    Done,
    // TODO Add aggregated edge
}

impl ConnectChildOperation {
    pub fn run(parent_task: TaskId, child_task: TaskId, ctx: ExecuteContext<'_>) {
        let mut parent_task = ctx.task(parent_task);
        if parent_task.add(CachedDataItem::Child {
            task: child_task,
            value: (),
        }) {
            // TODO add aggregated edge
            // TODO check for active
            ConnectChildOperation::ScheduleTask {
                task_id: child_task,
            }
            .execute(&ctx);
        }
    }
}

impl Operation for ConnectChildOperation {
    fn execute(mut self, ctx: &ExecuteContext<'_>) {
        loop {
            ctx.operation_suspend_point(&self);
            match self {
                ConnectChildOperation::ScheduleTask { task_id } => {
                    let mut should_schedule;
                    {
                        let mut task = ctx.task(task_id);
                        should_schedule = !task.has_key(&CachedDataItemKey::Output {});
                        if should_schedule {
                            should_schedule = task.add(CachedDataItem::new_scheduled(task_id));
                        }
                    }
                    if should_schedule {
                        ctx.schedule(task_id);
                    }

                    self = ConnectChildOperation::Done;
                    continue;
                }
                ConnectChildOperation::Done => {
                    return;
                }
            }
        }
    }
}
