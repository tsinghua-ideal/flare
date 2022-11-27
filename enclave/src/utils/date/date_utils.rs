
/// Takes a year as a u16 and returns whether it is a leap year.
pub fn is_leap_year(year: u16) -> bool {
    year % 4 == 0 && (year % 100 != 0 || year % 400 == 0)
}

/// Produces the integer representing the last date in the month in year.
pub fn get_last_date_in_month(month: u8, year: u16) -> u8 {
    match month {
        2 => {
            if is_leap_year(year) {
                29
            } else {
                28
            }
        }
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        _ => 30,
    }
}