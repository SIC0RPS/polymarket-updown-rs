//! Various utilities

use std::cmp::Ordering;

/// Used for finding max/min values
pub fn partial_cmp(a: f64, b: f64) -> Ordering {
    a.partial_cmp(&b).unwrap_or(Ordering::Equal)
}

/// Formats an `f64` for printing and tries to give it a fixed width
/// Minimum width is 8
pub fn format_num(num: f64, target_width: usize) -> String {
    if num.is_nan() || num.is_infinite() {
        return format!("{:1$}", num, target_width);
    }
    let sign = if num >= 0.0 { ' ' } else { '-' };
    // Account for non-digit characters that are added to every number
    let pad_width = target_width.max(8) - 7;
    let num_string = format!("{:.1$e}", num.abs(), pad_width);
    let mut num_parts = num_string.split('e');
    let num_digits = num_parts.next().unwrap();
    let num_exponent = num_parts.next().unwrap().parse::<i32>().unwrap();

    format!("{}{}e{:+03}", sign, num_digits, num_exponent)
}

/// Returns the range of the values in the iterator
///
/// Always returns a positive value
/// Returns `None` if the iterator is empty or contains a `NAN` value
pub fn range<I: IntoIterator<Item = f64>>(iter: I) -> Option<f64> {
    let mut iter = iter.into_iter();
    let first = iter.next();
    let (mut min, mut max) = match first {
        Some(x) => (x, x),
        None => return None,
    };

    for x in iter {
        if x.is_nan() {
            return None;
        }

        if let Ordering::Less = partial_cmp(x, min) {
            min = x;
        }

        if let Ordering::Greater = partial_cmp(x, max) {
            max = x;
        }
    }

    Some(max - min)
}
