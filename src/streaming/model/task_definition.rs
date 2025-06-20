use serde::{Deserialize, Serialize};
use crate::streaming::model::operator_definition::{OperatorDefinition, OperatorSpec};

#[derive(Clone, Serialize, Deserialize)]
pub struct TaskDefinition {
    pub task_id: String,
    pub operator: OperatorDefinition,
}

impl TaskDefinition {

    pub fn exchange_outputs(&self) -> Vec<String> {
        Self::get_exchange_outputs(&self.operator.spec)
    }

    pub fn exchange_inputs(&self) -> Vec<String> {
        Self::get_exchange_inputs(&self.operator.spec)
    }

    pub fn used_state_ids(&self) -> Vec<String> {
        Self::get_used_state_ids(&self.operator)
    }

    // TODO should these methods really be here? Seems like they should be members of an operator trait
    fn get_exchange_outputs(operator: &OperatorSpec) -> Vec<String> {
        match operator {
            OperatorSpec::Identity(_) => vec![],
            OperatorSpec::Source(_) => vec![],
            OperatorSpec::CountStar(_) => vec![],
            OperatorSpec::CountByKey(_) => vec![],
            OperatorSpec::RemoteExchangeInput(_) => vec![],
            OperatorSpec::RemoteExchangeOutput(output) => vec![output.get_stream_id().to_string()],
            OperatorSpec::Nested(nested) => nested.get_operators().iter()
                .flat_map(|op| Self::get_exchange_outputs(&op.spec))
                .collect(),
        }
    }

    fn get_exchange_inputs(operator: &OperatorSpec) -> Vec<String> {
        match operator {
            OperatorSpec::Identity(_) => vec![],
            OperatorSpec::Source(_) => vec![],
            OperatorSpec::CountStar(_) => vec![],
            OperatorSpec::CountByKey(_) => vec![],
            OperatorSpec::RemoteExchangeOutput(_) => vec![],
            OperatorSpec::RemoteExchangeInput(input) => input.get_stream_ids().iter().cloned().collect(),
            OperatorSpec::Nested(nested) => nested.get_operators().iter()
                .flat_map(|op| Self::get_exchange_inputs(&op.spec))
                .collect(),
        }
    }

    fn get_used_state_ids(operator: &OperatorDefinition) -> Vec<String> {
        match &operator.spec {
            OperatorSpec::Identity(_) => vec![],
            OperatorSpec::Source(_) => vec![],
            OperatorSpec::CountStar(_) => vec![operator.state_id.clone()],
            OperatorSpec::CountByKey(_) => vec![operator.state_id.clone()],
            OperatorSpec::RemoteExchangeInput(_) => vec![],
            OperatorSpec::RemoteExchangeOutput(_) => vec![],
            OperatorSpec::Nested(nested) => nested.get_operators().iter()
                .flat_map(|op| Self::get_used_state_ids(&op))
                .collect(),
        }
    }
}
