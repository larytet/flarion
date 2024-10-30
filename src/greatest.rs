use std::cmp;
// use std::borrow::Cow;
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

/// Return the greater of two values, type cast if possible to match the Spark's behavior
/// Support for Int32, Float32, Int64, Float64, String, Boolean
fn compare_values(current_max: ScalarValue, new_value: ScalarValue) -> ScalarValue {
    match (current_max.clone(), new_value) {
        (ScalarValue::Int32(Some(max)), ScalarValue::Int32(Some(new))) => {
            ScalarValue::Int32(Some(cmp::max(max, new)))
        }
        (ScalarValue::Int32(Some(max)), ScalarValue::Int64(Some(new))) => {
            ScalarValue::Int64(Some(i64::max(max as i64, new as i64)))
        }
        (ScalarValue::Int32(Some(max)), ScalarValue::Float32(Some(new))) => {
            ScalarValue::Float32(Some(f32::max(max as f32, new as f32)))
        }
        (ScalarValue::Int32(Some(max)), ScalarValue::Float64(Some(new))) => {
            ScalarValue::Float64(Some(f64::max(max as f64, new as f64)))
        }

        (ScalarValue::Int64(Some(max)), ScalarValue::Int32(Some(new))) => {
            ScalarValue::Int64(Some(i64::max(max as i64, new as i64)))
        }
         (ScalarValue::Int64(Some(max)), ScalarValue::Int64(Some(new))) => {
            ScalarValue::Int64(Some(i64::max(max as i64, new as i64)))
        }
        (ScalarValue::Int64(Some(max)), ScalarValue::Float32(Some(new))) => {
            ScalarValue::Float64(Some(f64::max(max as f64, new as f64)))
        }
        (ScalarValue::Int64(Some(max)), ScalarValue::Float64(Some(new))) => {
            ScalarValue::Float64(Some(f64::max(max as f64, new)))
        }

        (ScalarValue::Float32(Some(max)), ScalarValue::Int32(Some(new))) => {
            ScalarValue::Float32(Some(f32::max(max as f32, new as f32)))
        }
        (ScalarValue::Float32(Some(max)), ScalarValue::Int64(Some(new))) => {
            ScalarValue::Float64(Some(f64::max(max as f64, new as f64)))
        }
        (ScalarValue::Float32(Some(max)), ScalarValue::Float32(Some(new))) => {
            ScalarValue::Float32(Some(f32::max(max as f32, new as f32)))
        }
        (ScalarValue::Float32(Some(max)), ScalarValue::Float64(Some(new))) => {
            ScalarValue::Float64(Some(f64::max(max as f64, new as f64)))
        }

        (ScalarValue::Float64(Some(max)), ScalarValue::Int32(Some(new))) => {
            ScalarValue::Float64(Some(f64::max(max as f64, new as f64)))
        }
        (ScalarValue::Float64(Some(max)), ScalarValue::Int64(Some(new))) => {
            ScalarValue::Float64(Some(f64::max(max as f64, new as f64)))
        }
        (ScalarValue::Float64(Some(max)), ScalarValue::Float32(Some(new))) => {
            ScalarValue::Float64(Some(f64::max(max as f64, new as f64)))
        }
        (ScalarValue::Float64(Some(max)), ScalarValue::Float64(Some(new))) => {
            ScalarValue::Float64(Some(f64::max(max as f64, new as f64)))
        }


        (ScalarValue::Boolean(Some(max)), ScalarValue::Boolean(Some(new))) => {
            // "true" > "false"
            if new && !max {
                ScalarValue::Boolean(Some(true))
            } else {
                current_max
            }
        }
        (ScalarValue::Utf8(Some(ref max)), ScalarValue::Utf8(Some(ref new))) => {
            // string comparison, retutn a cloned string
            if new > max {
                ScalarValue::Utf8(Some(new.clone()))
            } else {
                current_max
            }
        }
        // unsupported, return the current (None?)
        _ => current_max,
    }
}

/// Demo
pub fn run_query() -> Result<()> {
    let values = vec![
        ScalarValue::Int32(Some(1)),
        ScalarValue::Int32(Some(2)),
        ScalarValue::Float32(Some(3.2)),
        ScalarValue::Int32(Some(3)),
        ScalarValue::Int64(Some(4)),
    ];

    let max_value = greatest(values)?;
    println!("The greatest value is: {:?}", max_value);

    Ok(())
}



#[cfg(test)]
mod tests {
    use super::*; 

    #[test]
    fn test_compare_int32() {
        let max = ScalarValue::Int32(Some(10));
        let new = ScalarValue::Int32(Some(5));
        let result = compare_values(max.clone(), new);
        assert_eq!(result, ScalarValue::Int32(Some(10)));

        let new = ScalarValue::Int32(Some(15));
        let result = compare_values(max.clone(), new);
        assert_eq!(result, ScalarValue::Int32(Some(15)));
    }

    #[test]
    fn test_compare_int64() {
        let max = ScalarValue::Int64(Some(20));
        let new = ScalarValue::Int32(Some(25));
        let result = compare_values(max.clone(), new);
        assert_eq!(result, ScalarValue::Int64(Some(25)));

        let new = ScalarValue::Int64(Some(15));
        let result = compare_values(max.clone(), new);
        assert_eq!(result, ScalarValue::Int64(Some(20)));
    }

    #[test]
    fn test_compare_float32() {
        let max = ScalarValue::Float32(Some(10.0));
        let new = ScalarValue::Float32(Some(5.0));
        let result = compare_values(max.clone(), new);
        assert_eq!(result, ScalarValue::Float32(Some(10.0)));

        let new = ScalarValue::Float32(Some(15.0));
        let result = compare_values(max.clone(), new);
        assert_eq!(result, ScalarValue::Float32(Some(15.0)));
    }

    #[test]
    fn test_compare_boolean() {
        let max = ScalarValue::Boolean(Some(false));
        let new = ScalarValue::Boolean(Some(true));
        let result = compare_values(max.clone(), new);
        assert_eq!(result, ScalarValue::Boolean(Some(true)));

        let new = ScalarValue::Boolean(Some(false));
        let result = compare_values(max.clone(), new);
        assert_eq!(result, ScalarValue::Boolean(Some(false)));
    }

    #[test]
    fn test_compare_utf8() {
        let max = ScalarValue::Utf8(Some("apple".to_string()));
        let new = ScalarValue::Utf8(Some("banana".to_string()));
        let result = compare_values(max.clone(), new);
        assert_eq!(result, ScalarValue::Utf8(Some("banana".to_string())));

        let new = ScalarValue::Utf8(Some("apple".to_string()));
        let result = compare_values(max.clone(), new);
        assert_eq!(result, ScalarValue::Utf8(Some("apple".to_string())));
    }

    #[test]
    fn test_compare_unsupported() {
        let max = ScalarValue::Int32(Some(10));
        let new = ScalarValue::Boolean(Some(true)); 
        let result = compare_values(max.clone(), new);
        assert_eq!(result, max); 
    }
}
