use datafusion::error::Result;
use datafusion::scalar::ScalarValue;


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
