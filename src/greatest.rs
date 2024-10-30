use datafusion::error::Result;
use datafusion::scalar::ScalarValue;

// Your greatest function implementation
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

// Example run_query function to demonstrate usage
pub fn run_query() -> Result<()> {
    // Example values to test the greatest function
    let values = vec![
        ScalarValue::Int32(Some(1)),
        ScalarValue::Int32(Some(5)),
        ScalarValue::Int32(Some(3)),
    ];

    let max_value = greatest(values)?;
    println!("The greatest value is: {:?}", max_value);

    Ok(())
}
