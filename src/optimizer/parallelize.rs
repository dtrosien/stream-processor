use crate::action_plan::deserialize::Deserialize;
use crate::action_plan::scan::Scan;
use crate::action_plan::transform::Transform;
use crate::action_plan::write::Write;
use crate::action_plan::ActionPlan;
use crate::optimizer::OptimizerRule;
use log::info;
use std::sync::Arc;

pub struct SplitByInputPartitions;

// impl OptimizerRule for ParallelizeByInputPartitions {
//     fn optimize(&self, plan: Arc<dyn ActionPlan>) -> Arc<dyn ActionPlan> {
//         Self::parallelize(plan)
//     }
// }

impl SplitByInputPartitions {
    pub fn split(plan: Arc<dyn ActionPlan>) -> Vec<Arc<dyn ActionPlan>> {
        let parts = plan.get_partitions();

        let mut plans: Vec<Arc<dyn ActionPlan>> = Vec::new();

        for part in parts {
            plans.push(Self::duplicate_plan(plan.clone(), part))
        }

        plans
    }

    fn duplicate_plan(plan: Arc<dyn ActionPlan>, partition: String) -> Arc<dyn ActionPlan> {
        if let Some(scan) = plan.as_any().downcast_ref::<Scan>() {
            let ds = scan.data_source.recreate_partitioned(partition); // todo recreate datasource
            Scan::new(ds)
        } else if let Some(deserialize) = plan.as_any().downcast_ref::<Deserialize>() {
            let input = Self::duplicate_plan(deserialize.child().unwrap(), partition);
            Deserialize::new(input, deserialize.decoder.clone())
        } else if let Some(transform) = plan.as_any().downcast_ref::<Transform>() {
            let input = Self::duplicate_plan(transform.child().unwrap(), partition);
            Transform::new(
                input,
                transform.mapper.clone(),
                transform.encoder.clone(),
                transform.transformations.clone(),
            )
        } else if let Some(write) = plan.as_any().downcast_ref::<Write>() {
            let input = Self::duplicate_plan(write.child().unwrap(), partition);
            Write::new(input, write.data_sinks.clone())
        } else {
            panic!("ParallelizeByInputPartitions error")
        }
    }
}
