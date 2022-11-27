use super::date_tuple::DateTuple;

use std::cmp::Ordering;
use std::convert::From;
use std::fmt;
use std::str::FromStr;
use std::string::String;

use regex::Regex;
use serde_derive::{Deserialize, Serialize};

const MONTH_STRINGS: [&str; 12] = [
    "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
];

pub type Month = MonthTuple;

/// A container for a month of a specific year.
///
/// **NOTE:** MonthTuple's `m` field is one-based (one represents January) as of version 2.0.0.
///
/// Only handles values between Jan 0000 and Dec 9999 (inclusive).
#[derive(Default, PartialEq, Eq, Debug, Copy, Clone, Hash, Deserialize, Serialize)]
pub struct MonthTuple {
    y: u16,
    m: u8,
}

impl MonthTuple {
    /// Produces a new MonthTuple.
    ///
    /// Only accepts a valid month value (`1 <= m <= 12`).
    ///
    /// Only accepts a valid year value (`0 <= y <= 9999`).
    pub fn new(y: u16, m: u8) -> Result<MonthTuple, String> {
        if (1..=12).contains(&m) {
            if y <= 9999 {
                Ok(MonthTuple { y, m })
            } else {
                Err(format!(
                    "Invalid year in MonthTuple: {:?}\nYear must be <= 9999.",
                    MonthTuple { y, m }
                ))
            }
        } else {
            Err(format!(
                "Invalid month in MonthTuple: {:?}\nMonth must be between 1 and 12; Note that months are ONE-BASED since version 2.0.0.",
                MonthTuple { y, m }
            ))
        }
    }

    pub fn get_year(self) -> u16 {
        self.y
    }

    /// Retrieves the month component of the tuple.
    ///
    /// Note this month is **ONE-BASED** (one represents January).
    pub fn get_month(self) -> u8 {
        self.m
    }

    /// Gets a MonthTuple representing the month immediately following
    /// the current one. Will not go past Dec 9999.
    pub fn next_month(self) -> MonthTuple {
        if self.y == 9999 && self.m == 12 {
            return self;
        }
        if self.m == 12 {
            MonthTuple {
                y: self.y + 1,
                m: 1,
            }
        } else {
            MonthTuple {
                y: self.y,
                m: self.m + 1,
            }
        }
    }

    /// Gets a MonthTuple representing the month immediately preceding
    /// the current one. Will not go past Jan 0000.
    pub fn previous_month(self) -> MonthTuple {
        if self.y == 0 && self.m == 1 {
            return self;
        }
        if self.m == 1 {
            MonthTuple {
                y: self.y - 1,
                m: 12,
            }
        } else {
            MonthTuple {
                y: self.y,
                m: self.m - 1,
            }
        }
    }

    /// Adds a number of months to a MonthTuple.
    pub fn add_months(&mut self, months: u32) {
        for _ in 0..months {
            *self = self.next_month();
        }
    }

    /// Subtracts a number of months from a MonthTuple.
    pub fn subtract_months(&mut self, months: u32) {
        for _ in 0..months {
            *self = self.previous_month();
        }
    }

    /// Adds a number of years to a MonthTuple.
    pub fn add_years(&mut self, years: u16) {
        let mut new_years = self.y + years;
        if new_years > 9999 {
            new_years = 9999;
        }
        self.y = new_years;
    }

    /// Subtracts a number of years from a MonthTuple.
    pub fn subtract_years(&mut self, years: u16) {
        let mut new_years = i32::from(self.y) - i32::from(years);
        if new_years < 0 {
            new_years = 0;
        }
        self.y = new_years as u16;
    }

    /// Returns the month formatted to be human-readable.
    ///
    /// ## Examples
    /// * Jan 2018
    /// * Dec 1994
    pub fn to_readable_string(self) -> String {
        match MONTH_STRINGS.iter().nth(self.m as usize - 1) {
            Some(s) => return format!("{} {:04}", s, self.y),
            None => panic!("Invalid MonthTuple: {:?}", self),
        }
    }
}

impl fmt::Display for MonthTuple {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:04}-{:02}", self.y, self.m)
    }
}

impl FromStr for MonthTuple {
    type Err = String;

    fn from_str(s: &str) -> Result<MonthTuple, Self::Err> {
        lazy_static! {
            static ref VALID_FORMAT: Regex = Regex::new(r"^\d{4}-\d{2}$").unwrap();
            static ref LEGACY_FORMAT: Regex = Regex::new(r"^\d{6}$").unwrap();
        }

        if VALID_FORMAT.is_match(s) {
            match MonthTuple::new(
                u16::from_str(&s[0..4]).unwrap(),
                u8::from_str(&s[5..7]).unwrap(),
            ) {
                Ok(m) => Ok(m),
                Err(e) => Err(format!("Invalid month passed to from_str: {}", e)),
            }
        } else if LEGACY_FORMAT.is_match(s) {
            let (s1, s2) = s.split_at(4);
            match MonthTuple::new(u16::from_str(s1).unwrap(), u8::from_str(s2).unwrap()) {
                Ok(m) => Ok(m),
                Err(e) => Err(format!("Invalid month passed to from_str: {}", e)),
            }
        } else {
            Err(format!(
                "Invalid str formatting of MonthTuple: {}\nExpects a string formatted like 2018-11",
                s
            ))
        }
    }
}

impl PartialOrd for MonthTuple {
    fn partial_cmp(&self, other: &MonthTuple) -> Option<Ordering> {
        if self.y == other.y {
            self.m.partial_cmp(&other.m)
        } else {
            self.y.partial_cmp(&other.y)
        }
    }
}

#[cfg_attr(tarpaulin, skip)]
impl Ord for MonthTuple {
    fn cmp(&self, other: &MonthTuple) -> Ordering {
        if self.y == other.y {
            self.m.cmp(&other.m)
        } else {
            self.y.cmp(&other.y)
        }
    }
}

impl From<DateTuple> for MonthTuple {
    fn from(date: DateTuple) -> Self {
        MonthTuple {
            y: date.get_year(),
            m: date.get_month(),
        }
    }
}