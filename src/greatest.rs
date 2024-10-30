use datafusion::error::Result;
use datafusion::scalar::ScalarValue;

/// Find the greatest value in a vector of ScalarValues.
pub fn greatest(values: Vec<ScalarValue>) -> Result<ScalarValue> {
    let mut max_value: Option<ScalarValue> = None;

    for value in values {
        max_value = update_max_value(max_value, value);
    }

    Ok(max_value.unwrap_or(ScalarValue::Null))
}

/// Update the current maximum value based on the new value.
fn update_max_value(max_value: Option<ScalarValue>, new_value: ScalarValue) -> Option<ScalarValue> {
    match max_value {
        Some(current_max) => {
            // Compare the current max with the new value and return the greater one
            Some(compare_values(current_max, new_value))
        }
        None => Some(new_value), // If no max value, set it to the new value
    }
}

/// Return the greater of two values.
fn compare_values(current_max: ScalarValue, new_value: ScalarValue) -> ScalarValue {
    match (current_max.clone(), new_value) {
        (ScalarValue::Int32(Some(max)), ScalarValue::Int32(Some(new))) => {
            if new > max {
                ScalarValue::Int32(Some(new))
            } else {
                // Return the current_max since it's not changed
                current_max 
            }
        }
        (ScalarValue::Float32(Some(max)), ScalarValue::Float32(Some(new))) => {
            if new > max {
                ScalarValue::Float32(Some(new))
            } else {
                // Return the current_max since it's not changed
                current_max 
            }
        }
        // unsupported type: return current max (None)
        _ => current_max, 
    }
}

/// Run the query to demonstrate the greatest function.
pub fn run_query() -> Result<()> {
    let values = vec![
        ScalarValue::Int32(Some(1)),
        ScalarValue::Int32(Some(5)),
        ScalarValue::Int32(Some(3)),
    ];

    let max_value = greatest(values)?;
    println!("The greatest value is: {:?}", max_value);

    Ok(())
}
