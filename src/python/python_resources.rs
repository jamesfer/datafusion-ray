use pyo3::{Bound, Py, PyAny, PyObject, PyResult, Python};

// Stores references to python objects that are needed to interact with Ray
pub struct PythonResources {
    processor_class: Py<PyAny>,
    ray_get: Py<PyAny>,
}

impl PythonResources {
    pub fn new(processor_class: PyObject, ray_get: PyObject) -> PyResult<Self> {
        Python::with_gil(|py| {
            Ok(Self {
                processor_class: processor_class.bind(py).clone().unbind(),
                ray_get: ray_get.bind(py).clone().unbind(),
            })
        })
    }

    pub async fn with_python<F, R>(&self, f: F) -> PyResult<R>
    where
        F: FnOnce(&Bound<PyAny>, &Bound<PyAny>) -> PyResult<R> + Send + 'static,
        R: Send + 'static,
    {
        let processor_class = Python::with_gil(|py| self.processor_class.clone_ref(py));
        let ray_get = Python::with_gil(|py| self.ray_get.clone_ref(py));

        tokio::task::spawn_blocking(move || {
            Python::with_gil(|py| {
                let pc_bound = processor_class.bind(py);
                let rg_bound = ray_get.bind(py);
                f(&pc_bound, &rg_bound)
            })
        }).await.unwrap()
    }
}
