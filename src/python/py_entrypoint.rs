use futures_util::{StreamExt, TryStreamExt};
use pyo3::{pyfunction, PyResult, Python};
use std::sync::Arc;
use datafusion_python::utils::wait_for_future;
use pyo3::prelude::*;
use crate::python::compute_runtime::ComputeRuntime;
use crate::python::python_resources::PythonResources;
use crate::streaming::run::run;

#[pyfunction]
pub fn entrypoint(
    py: Python,
    processor_class: PyObject,
    ray_get: PyObject,
) -> PyResult<()> {
    let python_resources = Arc::new(PythonResources::new(processor_class, ray_get)?);
    let compute_runtime = Arc::new(ComputeRuntime::new(python_resources.clone()));
    wait_for_future(py, run(compute_runtime))
}
