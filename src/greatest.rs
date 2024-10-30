use datafusion::error::Result;
use datafusion::scalar::ScalarValue;

pub fn greatest(values: Vec<ScalarValue>) -> Result<ScalarValue> {
    let mut max_value: Option<ScalarValue> = None;

    for value in values {
        match (&max_value, value) {
            (Some(current_max), new_value) => {
                // Compare the values based on their types
                max_value = Some(match (current_max, new_value) {
                    (ScalarValue::Int32(Some(max)), ScalarValue::Int32(Some(new))) => {
                        if new > *max {
                            ScalarValue::Int32(Some(new))
                        } else {
                            current_max.clone()
                        }
                    }
                    (ScalarValue::Float32(Some(max)), ScalarValue::Float32(Some(new))) => {
                        if new > *max {
                            ScalarValue::Float32(Some(new))
                        } else {
                            current_max.clone()
                        }
                    }
                    // Handle other scalar types as needed
                    _ => current_max.clone(),
                });
            }
            (None, new_value) => {
                max_value = Some(new_value);
            }
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
