use super::date_utils; 
use super::month_tuple::MonthTuple;

use std::cmp::Ordering;
use std::fmt;
use std::str::FromStr;
use std::string::String;

use deepsize::DeepSizeOf;
use regex::Regex;
use serde_derive::{Deserialize, Serialize};

const DAYS_IN_A_COMMON_YEAR: u32 = 365;
const DAYS_IN_A_LEAP_YEAR: u32 = 366;

pub type Date = DateTuple;

/// Holds a specific date by year, month, and day.
///
/// Handles values from 01 Jan 0000 to 31 Dec 9999.

#[derive(Default, PartialEq, Eq, Debug, Copy, Clone, Hash, Serialize, Deserialize, DeepSizeOf)]
pub struct DateTuple {
    y: u16,
    m: u8,
    d: u8,
}

impl DateTuple {
    /// Takes a year, month, and day and converts them into a DateTuple.
    ///
    /// Will not overlap - the date entered must be valid without further calculation.
    pub fn new(y: u16, m: u8, d: u8) -> Result<DateTuple, String> {
        if y > 9999 {
            return Err(format!(
                "Invalid year in DateTuple {:?}: year must be <= 9999.",
                DateTuple { y, m, d }
            ));
        }
        if (1..=12).contains(&m) {
            if d == 0 || d > date_utils::get_last_date_in_month(m, y) {
                return Err(format!(
                    "Invalid date in DateTuple: {:?}",
                    DateTuple { y, m, d }
                ));
            }
            Ok(DateTuple { y, m, d })
        } else {
            Err(format!(
                "Invalid month in DateTuple: {:?}\nMonth must be between 1 and 12; Note that months are ONE-BASED since version 2.0.0.",
                DateTuple { y, m, d }
            ))
        }
    }

    /// Returns the minimum date handled - 1st January 0000.
    pub fn min_value() -> DateTuple {
        DateTuple::new(0, 1, 1).unwrap()
    }

    /// Returns the maximum date handled - 31st December 9999.
    pub fn max_value() -> DateTuple {
        DateTuple::new(9999, 12, 31).unwrap()
    }

    pub fn get_year(self) -> u16 {
        self.y
    }

    pub fn get_month(self) -> u8 {
        self.m
    }

    pub fn get_date(self) -> u8 {
        self.d
    }

    /// Gets a DateTuple representing the date immediately following
    /// the current one. Will not go past Dec 9999.
    pub fn next_date(self) -> DateTuple {
        if self.y == 9999 && self.m == 12 && self.d == 31 {
            return self;
        }
        if self.d == date_utils::get_last_date_in_month(self.m, self.y) {
            if self.m == 12 {
                DateTuple {
                    y: self.y + 1,
                    m: 1,
                    d: 1,
                }
            } else {
                DateTuple {
                    y: self.y,
                    m: self.m + 1,
                    d: 1,
                }
            }
        } else {
            DateTuple {
                y: self.y,
                m: self.m,
                d: self.d + 1,
            }
        }
    }

    /// Gets a DateTuple representing the date immediately preceding
    /// the current one. Will not go past 1 Jan 0000.
    pub fn previous_date(self) -> DateTuple {
        if self.y == 0 && self.m == 1 && self.d == 1 {
            return self;
        }
        if self.d == 1 {
            if self.m == 1 {
                DateTuple {
                    y: self.y - 1,
                    m: 12,
                    d: date_utils::get_last_date_in_month(12, self.y - 1),
                }
            } else {
                DateTuple {
                    y: self.y,
                    m: self.m - 1,
                    d: date_utils::get_last_date_in_month(self.m - 1, self.y),
                }
            }
        } else {
            DateTuple {
                y: self.y,
                m: self.m,
                d: self.d - 1,
            }
        }
    }

    /// Adds a number of days to a DateTuple.
    pub fn add_days(&mut self, days: u32) {
        for _ in 0..days {
            *self = self.next_date();
        }
    }

    /// Subtracts a number of days from a DateTuple.
    pub fn subtract_days(&mut self, days: u32) {
        for _ in 0..days {
            *self = self.previous_date();
        }
    }

    /// Adds a number of months to a DateTuple.
    ///
    /// If the day of month is beyond the last date in the resulting month, the day of
    /// month will be set to the last day of that month.
    pub fn add_months(&mut self, months: u32) {
        let mut new_month = MonthTuple::from(*self);
        new_month.add_months(months);
        let last_date_in_month =
            date_utils::get_last_date_in_month(new_month.get_month(), new_month.get_year());
        if self.d > last_date_in_month {
            self.d = last_date_in_month;
        }
        self.y = new_month.get_year();
        self.m = new_month.get_month();
    }

    /// Subtracts a number of months from a DateTuple.
    ///
    /// If the day of month is beyond the last date in the resulting month, the day of
    /// month will be set to the last day of that month.
    pub fn subtract_months(&mut self, months: u32) {
        let mut new_month = MonthTuple::from(*self);
        new_month.subtract_months(months);
        let last_date_in_month =
            date_utils::get_last_date_in_month(new_month.get_month(), new_month.get_year());
        if self.d > last_date_in_month {
            self.d = last_date_in_month;
        }
        self.y = new_month.get_year();
        self.m = new_month.get_month();
    }

    /// Adds a number of years to a DateTuple.
    ///
    /// If the date is set to Feb 29 and the resulting year is not a leap year,
    /// it will be changed to Feb 28.
    pub fn add_years(&mut self, years: u16) {
        let mut new_years = self.y + years;
        if new_years > 9999 {
            new_years = 9999;
        }
        if self.m == 2 && self.d == 29 && !date_utils::is_leap_year(new_years) {
            self.d = 28
        }
        self.y = new_years;
    }

    /// Subtracts a number of years from a DateTuple.
    ///
    /// If the date is set to Feb 29 and the resulting year is not a leap year,
    /// it will be changed to Feb 28.
    pub fn subtract_years(&mut self, years: u16) {
        let mut new_years = i32::from(self.y) - i32::from(years);
        if new_years < 0 {
            new_years = 0;
        }
        let new_years = new_years as u16;
        if self.m == 2 && self.d == 29 && !date_utils::is_leap_year(new_years) {
            self.d = 28
        }
        self.y = new_years;
    }

    /// Produces a readable date.
    ///
    /// ## Examples
    /// * 2 Oct 2018
    /// * 13 Jan 2019
    pub fn to_readable_string(self) -> String {
        let month = MonthTuple::from(self);
        format!("{} {}", self.d, month.to_readable_string())
    }

    /// Gets the total number of days in the tuple,
    /// with the first being `DateTuple::min_value()`.
    pub fn to_days(self) -> u32 {
        let mut total_days = 0u32;
        for y in 0..self.y {
            total_days += if date_utils::is_leap_year(y) {
                DAYS_IN_A_LEAP_YEAR
            } else {
                DAYS_IN_A_COMMON_YEAR
            }
        }
        for m in 1..self.m {
            total_days += u32::from(date_utils::get_last_date_in_month(m, self.y));
        }
        total_days + u32::from(self.d)
    }

    /// Calculates years, months, and days from a total number of
    /// days, with the first being `DateTuple::min_value()`.
    pub fn from_days(mut total_days: u32) -> Result<DateTuple, String> {
        let mut years = 0u16;
        let mut months = 1u8;
        while total_days
            > if date_utils::is_leap_year(years) {
                DAYS_IN_A_LEAP_YEAR
            } else {
                DAYS_IN_A_COMMON_YEAR
            }
        {
            total_days -= if date_utils::is_leap_year(years) {
                DAYS_IN_A_LEAP_YEAR
            } else {
                DAYS_IN_A_COMMON_YEAR
            };
            years += 1;
        }
        while total_days > u32::from(date_utils::get_last_date_in_month(months, years)) {
            total_days -= u32::from(date_utils::get_last_date_in_month(months, years));
            months += 1;
        }
        DateTuple::new(years, months, total_days as u8)
    }
}

impl fmt::Display for DateTuple {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:04}-{:02}-{:02}", self.y, self.m, self.d)
    }
}

impl FromStr for DateTuple {
    type Err = String;

    /// Expects a string formatted like 2018-11-02.
    ///
    /// Also accepts the legacy crate format of 20181102.
    fn from_str(s: &str) -> Result<DateTuple, Self::Err> {
        lazy_static! {
            static ref VALID_FORMAT: Regex = Regex::new(r"^\d{4}-\d{2}-\d{2}$").unwrap();
            static ref LEGACY_FORMAT: Regex = Regex::new(r"^\d{8}$").unwrap();
        }

        if VALID_FORMAT.is_match(s) {
            match DateTuple::new(
                u16::from_str(&s[0..4]).unwrap(),
                u8::from_str(&s[5..7]).unwrap(),
                u8::from_str(&s[8..10]).unwrap(),
            ) {
                Ok(d) => Ok(d),
                Err(e) => Err(format!("Invalid date passed to from_str: {}", e)),
            }
        } else if LEGACY_FORMAT.is_match(s) {
            let (s1, s2) = s.split_at(4);
            let (s2, s3) = s2.split_at(2);
            match DateTuple::new(
                u16::from_str(s1).unwrap(),
                u8::from_str(s2).unwrap(),
                u8::from_str(s3).unwrap(),
            ) {
                Ok(d) => Ok(d),
                Err(e) => Err(format!("Invalid date passed to from_str: {}", e)),
            }
        } else {
            Err(format!("Invalid str formatting of DateTuple: {}\nExpects a string formatted like 2018-11-02.", s))
        }
    }
}

impl PartialOrd for DateTuple {
    fn partial_cmp(&self, other: &DateTuple) -> Option<Ordering> {
        if self.y == other.y {
            if self.m == other.m {
                self.d.partial_cmp(&other.d)
            } else {
                self.m.partial_cmp(&other.m)
            }
        } else {
            self.y.partial_cmp(&other.y)
        }
    }
}

#[cfg_attr(tarpaulin, skip)]
impl Ord for DateTuple {
    fn cmp(&self, other: &DateTuple) -> Ordering {
        if self.y == other.y {
            if self.m == other.m {
                self.d.cmp(&other.d)
            } else {
                self.m.cmp(&other.m)
            }
        } else {
            self.y.cmp(&other.y)
        }
    }
}