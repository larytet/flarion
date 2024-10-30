use datafusion::error::Result;
use datafusion::prelude::*;
use datafusion::scalar::ScalarValue;
use datafusion::physical_plan::functions::make_scalar_function;
use datafusion::physical_plan::udf::ScalarUDF;
use std::sync::Arc;

// The function greatest finds the maximum scalar value in a list. 
// If the list is empty, it returns ScalarValue::Null. 
// If values are present, it iterates through them, keeping track 
// of the largest, which it returns wrapped in a Result.
pub fn greatest(values: Vec<ScalarValue>) -> Result<ScalarValue> {
    let mut max_value = None;

    for value in values {
        if let Some(current_max) = max_value {
            max_value = Some(current_max.max(value));
        } else {
            max_value = Some(value);
        }
    }

    Ok(max_value.unwrap_or(ScalarValue::Null))
}

pub fn run_query() -> Result<()> {
    let greatest_fn = make_scalar_function(|args: &[ScalarValue]| {
        greatest(args.to_vec())
    });

    let greatest_udf = Arc::new(ScalarUDF::new(
        "greatest",
        vec![DataType::Int64],
        Arc::new(DataType::Int64),
        greatest_fn,
    ));

    let mut ctx = ExecutionContext::new();
    ctx.register_udf(greatest_udf);

    let data = vec![
        vec![ScalarValue::Int64(Some(10))],
        vec![ScalarValue::Int64(Some(20))],
        vec![ScalarValue::Int64(Some(5))],
    ];
    let schema = Arc::new(Schema::new(vec![Field::new("value", DataType::Int64, false)]));

    let df = ctx.read_from_batch(RecordBatch::try_new(schema.clone(), data)?)?;
    ctx.register_table("test_data", df);

    let sql = "SELECT greatest(value) AS max_value FROM test_data";
    let df = ctx.sql(sql)?;

    let results = df.collect().await?;
    for batch in results {
        println!("{:?}", batch);
    }

    Ok(())
}