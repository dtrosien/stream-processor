pub mod parallelize;

use crate::action_plan::ActionPlan;
use crate::optimizer::parallelize::SplitByInputPartitions;
use std::sync::Arc;

pub struct Optimizer;

impl Optimizer {
    pub fn optimize(plan: Arc<dyn ActionPlan>) -> Arc<dyn ActionPlan> {
        // use here a list of rules when new rules are implemented
        let rule = SplitByInputPartitions;
        plan
    }

    pub fn split_by_partitions(plan: Arc<dyn ActionPlan>) -> Vec<Arc<dyn ActionPlan>> {
        // use here a list of rules when new rules are implemented
        SplitByInputPartitions::split(plan)
    }
}

trait OptimizerRule {
    fn optimize(&self, plan: Arc<dyn ActionPlan>) -> Arc<dyn ActionPlan>;
}
