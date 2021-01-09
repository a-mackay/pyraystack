use pyo3::prelude::*;
use pyo3::{PyErr, PyResult};
use raystack::{ClientSeed, NewClientSeedError, NewSkySparkClientError, ParseRefError, Ref};
use thiserror::Error;
use tokio::runtime::Runtime;
use url::Url;

#[pyclass]
struct SkySparkClient {
    client: raystack::SkySparkClient,
    rt: Runtime,
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
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .map_err(|err| PrError::NewAsyncRuntime(err))?;

        let seed = ClientSeed::new(timeout_in_seconds)
            .map_err(|err| PrError::ClientSeed(err))?;
        let url = Url::parse(project_api_url)
            .map_err(|err| PrError::UrlParse(err))?;

        let client_fut = new_skyspark_client(url, username, password, seed);
        let client = rt.block_on(client_fut)?;

        Ok(Self { client, rt })
    }

    pub fn his_write_num(&mut self, id: String) -> PyResult<()> {
        let id = Ref::new(id).map_err(|err| PrError::RefParse(err))?;
        let data = vec![];
        let unit = None;
        let write_fut = (&mut self.client).his_write_num(&id, &data, unit);
        let _grid = self.rt.block_on(write_fut).map_err(|err| PrError::Raystack(err))?;
        Ok(())
    }
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
        todo!();
        // match err.0 {
        //     Error::Io(e) => IOError::py_err(e.to_string()),
        //     Error::ParseError(e) => ParseError::py_err(e.to_string()),
        //     Error::WidthError(e) => ParseError::py_err(e.to_string()),
        // }
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
