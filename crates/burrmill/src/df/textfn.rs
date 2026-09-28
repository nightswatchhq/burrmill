//! A host's own text functions: strings in, one string out, NULL in any argument gives NULL, and an
//! error refuses the statement. nuthatch's `nuthatch_*` conversions are these.

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, AsArray, StringBuilder};
use arrow::compute::cast;
use arrow::datatypes::DataType;
use datafusion_common::{Result, exec_err};
use datafusion_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, TypeSignature,
    Volatility,
};

/// What a text function computes from its arguments, or why it refuses them.
pub type TextFunction = Arc<dyn Fn(&[&str]) -> std::result::Result<String, String> + Send + Sync>;

pub(crate) struct TextFn {
    name: String,
    sig: Signature,
    f: TextFunction,
}

impl std::fmt::Debug for TextFn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "TextFn({})", self.name)
    }
}

impl PartialEq for TextFn {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name && Arc::ptr_eq(&self.f, &other.f)
    }
}
impl Eq for TextFn {}
impl std::hash::Hash for TextFn {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.name.hash(state);
    }
}

impl TextFn {
    pub(crate) fn udf(name: &str, arity: usize, f: TextFunction) -> Arc<ScalarUDF> {
        let text = [DataType::Utf8, DataType::Utf8View, DataType::LargeUtf8];
        let sig = Signature::one_of(
            vec![TypeSignature::Uniform(arity, text.to_vec())],
            Volatility::Immutable,
        );
        Arc::new(ScalarUDF::from(Self { name: name.to_string(), sig, f }))
    }
}

impl ScalarUDFImpl for TextFn {
    fn name(&self) -> &str {
        &self.name
    }
    fn signature(&self) -> &Signature {
        &self.sig
    }
    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let rows = args.number_rows;
        let cols = args
            .args
            .iter()
            .map(|a| cast(&a.to_array(rows)?, &DataType::Utf8).map_err(Into::into))
            .collect::<Result<Vec<ArrayRef>>>()?;
        let cols: Vec<_> = cols.iter().map(|c| c.as_string::<i32>()).collect();
        let mut out = StringBuilder::with_capacity(rows, rows * 16);
        let mut values = Vec::with_capacity(cols.len());
        for i in 0..rows {
            if cols.iter().any(|c| c.is_null(i)) {
                out.append_null();
                continue;
            }
            values.clear();
            values.extend(cols.iter().map(|c| c.value(i)));
            match (self.f)(&values) {
                Ok(s) => out.append_value(s),
                Err(e) => return exec_err!("{}: {e}", self.name),
            }
        }
        Ok(ColumnarValue::Array(Arc::new(out.finish())))
    }
}
