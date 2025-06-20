use pyo3::exceptions::PyValueError;
use pyo3::{PyErr, PyResult};
use datafusion::error::DataFusionError;

pub trait DataFusionErrorToPython {
    fn to_python_error(&self) -> PyErr;
}

impl DataFusionErrorToPython for DataFusionError {
    fn to_python_error(&self) -> PyErr {
        PyValueError::new_err(format!("DataFusion error: {}", self))
    }
}

fn to_python_error(e: &DataFusionError) -> PyErr {
    e.to_python_error()
}

fn into_python_error(e: DataFusionError) -> PyErr {
    e.to_python_error()
}

pub trait DataFusionResultToPython<T> {
    fn to_python_result(self) -> PyResult<T>;
}

impl<T> DataFusionResultToPython<T> for Result<T, DataFusionError> {
    fn to_python_result(self) -> PyResult<T> {
        self.map_err(into_python_error)
    }
}
