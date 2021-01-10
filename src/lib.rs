use chrono::{DateTime, TimeZone, Utc};
use pyo3::prelude::*;
use pyo3::{PyErr, PyResult};
use raystack::{
    ClientSeed, NewClientSeedError, NewSkySparkClientError, ParseRefError, Ref,
};
use thiserror::Error;
use tokio::runtime::Runtime;
use url::Url;

#[pyclass]
struct SkySparkClient {
    client: raystack::SkySparkClient,
}

#[pymodule]
fn pyraystack(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<SkySparkClient>()?;

    Ok(())
}

#[pymethods]
impl SkySparkClient {
    #[new]
    pub fn new(
        project_api_url: &str,
        username: &str,
        password: &str,
        timeout_in_seconds: u64,
    ) -> PyResult<SkySparkClient> {
        let rt = new_runtime()?;

        let seed = ClientSeed::new(timeout_in_seconds)
            .map_err(|err| PrError::ClientSeed(err))?;
        let url = Url::parse(project_api_url)
            .map_err(|err| PrError::UrlParse(err))?;

        let client_fut = new_skyspark_client(url, username, password, seed);
        let client = rt.block_on(client_fut)?;

        Ok(Self { client })
    }

    pub fn his_write_num(
        &mut self,
        id: String,
        time_zone_name: &str,
        data: Vec<(&str, f64)>,
        unit: Option<&str>,
    ) -> PyResult<()> {
        let id = Ref::new(id).map_err(|err| PrError::RefParse(err))?;
        let utc = Utc;

        let data: Result<Vec<(DateTime<_>, f64)>, PrError> = data
            .into_iter()
            .map(|(dt_str, num)| {
                let dt = utc.datetime_from_str(dt_str, "%Y-%m-%dT%T%.f")
                    .map_err(|err| PrError::DateTimeParse(err));

                dt.map(|dt| (dt, num))
            })
            .collect();

        let data = data?;

        let write_fut = (&mut self.client).utc_his_write_num(&id, time_zone_name, &data, unit);

        let rt = new_runtime()?;
        let _grid = rt
            .block_on(write_fut)
            .map_err(|err| PrError::Raystack(err))?;

        Ok(())
    }
}

fn new_runtime() -> Result<Runtime, PrError> {
    Runtime::new().map_err(|err| PrError::NewAsyncRuntime(err))
}

async fn new_skyspark_client(
    url: Url,
    username: &str,
    password: &str,
    seed: ClientSeed,
) -> Result<raystack::SkySparkClient, PrError> {
    raystack::SkySparkClient::new(url, username, password, seed)
        .await
        .map_err(|err| PrError::NewClient(err))
}

#[derive(Debug, Error)]
enum PrError {
    #[error("New async runtime error: {0}")]
    NewAsyncRuntime(#[from] std::io::Error),
    #[error("Client seed error: {0}")]
    ClientSeed(#[from] NewClientSeedError),
    #[error("DateTime parse error: {0}")]
    DateTimeParse(chrono::ParseError),
    #[error("New client error: {0}")]
    NewClient(#[from] NewSkySparkClientError),
    #[error("Raystack error :{0}")]
    Raystack(raystack::Error),
    #[error("Ref parse error: {0}")]
    RefParse(ParseRefError),
    #[error("Url parse error: {0}")]
    UrlParse(#[from] url::ParseError),
}

impl std::convert::From<PrError> for PyErr {
    fn from(err: PrError) -> PyErr {
        let msg = format!("{}", err);
        pyo3::exceptions::PyException::new_err(msg)
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
