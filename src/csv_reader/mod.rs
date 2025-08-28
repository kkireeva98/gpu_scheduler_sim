use std::error::Error;
use std::fmt::Formatter;
use std::str::FromStr;
use serde::{de, Deserialize, Deserializer};

use crate::types::*;

#[cfg(test)]
mod tests;

pub fn process_csv<T, R, F>(csv_reader: R, mut callback: F ) -> Result<(), Box<dyn Error>>
where
    F: FnMut(usize, T) -> Result<(), Box<dyn Error>>,
    R: std::io::Read,
    T: for<'de> Deserialize<'de>,
{
    let mut rdr = csv::Reader::from_reader(csv_reader);

    rdr.deserialize().enumerate().try_for_each( |(i, result)| {
        let record = result?;

        callback( i, record )
    })
}


// Handle special case for parsing GPU Specs for Pod Specs, ei V100M16|V100M32
pub fn parse_multi_spec<'de, D>( deserializer : D ) -> Result< MODELS, D::Error> where
    D: Deserializer<'de> {

    struct MultiSpecVisitor;

    impl<'de> de::Visitor<'de> for MultiSpecVisitor {
        type Value = MODELS;

        fn expecting(&self, formatter: &mut Formatter) -> std::fmt::Result {
            formatter.write_str("valid gpu_spec string")
        }

        fn visit_str<E>(self, v: &str) -> Result<Self::Value, E> where E: de::Error {
            if v.is_empty() { return Ok(Vec::new()); }

            v.split("|")
                .map(|str| {
                    GpuSpec::try_from(str).map_err(|_| E::custom(format!("invalid gpu_spec: {}", str)))
                }).collect()
        }
    }

    deserializer.deserialize_any(MultiSpecVisitor)
}