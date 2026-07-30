//! MeasurePushOperator — port of `zql/src/query/measure-push-operator.ts`.
//!
//! Wraps a pipeline to measure push timing. Passes through fetch unchanged.

use std::cell::RefCell;
use std::rc::Rc;
use std::time::Instant;

use crate::ivm::change::Change;
use crate::ivm::operator::{FetchRequest, Input, InputBase, Output, OutputHandle, Shared};
use crate::ivm::schema::SourceSchema;
use crate::ivm::stream::NodeStream;

/// Metrics delegate — receives timing measurements.
pub trait MetricsDelegate {
    fn add_metric(&self, name: &str, ms: f64, query_id: &str);
}

/// A no-op metrics delegate for when timing is not needed.
pub struct NullMetricsDelegate;
impl MetricsDelegate for NullMetricsDelegate {
    fn add_metric(&self, _name: &str, _ms: f64, _query_id: &str) {}
}

/// MeasurePushOperator — wraps an Input to measure push latency.
/// Port of TS `MeasurePushOperator` (measure-push-operator.ts:21).
pub struct MeasurePushOperator {
    input: Shared<dyn Input>,
    query_id: String,
    metrics: Rc<dyn MetricsDelegate>,
    metric_name: String,
    output: Rc<RefCell<Option<OutputHandle>>>,
    schema: SourceSchema,
}

impl MeasurePushOperator {
    pub fn new(
        input: Shared<dyn Input>,
        query_id: String,
        metrics: Rc<dyn MetricsDelegate>,
        metric_name: String,
    ) -> Shared<MeasurePushOperator> {
        let schema = input.borrow().get_schema();
        let mpo = Rc::new(RefCell::new(MeasurePushOperator {
            input: input.clone(),
            query_id,
            metrics,
            metric_name,
            output: Rc::new(RefCell::new(None)),
            schema,
        }));

        let mpo_clone = mpo.clone();
        input
            .borrow()
            .set_output(Rc::new(RefCell::new(MeasureOutput { mpo: mpo_clone })));

        mpo
    }
}

impl InputBase for MeasurePushOperator {
    fn get_schema(&self) -> SourceSchema {
        self.schema.clone()
    }

    fn destroy(&mut self) {
        self.input.borrow_mut().destroy();
    }
}

impl Input for MeasurePushOperator {
    fn set_output(&self, output: OutputHandle) {
        *self.output.borrow_mut() = Some(output);
    }

    fn fetch(&self, req: &FetchRequest) -> NodeStream {
        self.input.borrow().fetch(req)
    }
}

impl Output for MeasurePushOperator {
    fn push(&mut self, _change: Change, _pusher: &dyn InputBase) {
        // Pushes arrive via MeasureOutput adapter
    }
}

struct MeasureOutput {
    mpo: Shared<MeasurePushOperator>,
}

impl Output for MeasureOutput {
    fn push(&mut self, change: Change, pusher: &dyn InputBase) {
        let mpo = self.mpo.borrow();
        let start = Instant::now();

        let output = mpo.output.borrow().clone();
        if let Some(output) = output {
            output.borrow_mut().push(change, pusher);
        }

        let elapsed_ms = start.elapsed().as_secs_f64() * 1000.0;
        mpo.metrics
            .add_metric(&mpo.metric_name, elapsed_ms, &mpo.query_id);
    }
}
