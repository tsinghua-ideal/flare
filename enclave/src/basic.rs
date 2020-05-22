use std::{
	any, borrow::{Borrow, BorrowMut}, error, fmt, marker, ops::{self, Deref, DerefMut}, sync
};

pub trait Data:
    Clone
    + any::Any
    + Send
    + Sync
    + fmt::Debug
    + serde::ser::Serialize
    + serde::de::DeserializeOwned
    + 'static
{
}

impl<
        T: Clone
        + any::Any
        + Send
        + Sync
        + fmt::Debug
        + serde::ser::Serialize
        + serde::de::DeserializeOwned
        + 'static,
    > Data for T
{
}

/// Convenience wrapper around [std::sync::Arc<T>](std::sync::Arc) that automatically uses `serde_traitobject` for (de)serialization.
#[derive(Default, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Arc<T: ?Sized>(sync::Arc<T>);
impl<T> Arc<T> {
	/// Create a new Arc wrapper
	pub fn new(t: T) -> Self {
		Self(sync::Arc::new(t))
	}
}
impl<T: ?Sized + marker::Unsize<U>, U: ?Sized> ops::CoerceUnsized<Arc<U>> for Arc<T> {}
impl<T: ?Sized> Deref for Arc<T> {
	type Target = sync::Arc<T>;
	fn deref(&self) -> &Self::Target {
		&self.0
	}
}
impl<T: ?Sized> DerefMut for Arc<T> {
	fn deref_mut(&mut self) -> &mut Self::Target {
		&mut self.0
	}
}
impl<T: ?Sized> AsRef<sync::Arc<T>> for Arc<T> {
	fn as_ref(&self) -> &sync::Arc<T> {
		&self.0
	}
}
impl<T: ?Sized> AsMut<sync::Arc<T>> for Arc<T> {
	fn as_mut(&mut self) -> &mut sync::Arc<T> {
		&mut self.0
	}
}
impl<T: ?Sized> AsRef<T> for Arc<T> {
	fn as_ref(&self) -> &T {
		&*self.0
	}
}
impl<T: ?Sized> Borrow<T> for Arc<T> {
	fn borrow(&self) -> &T {
		&*self.0
	}
}
impl<T: ?Sized> From<sync::Arc<T>> for Arc<T> {
	fn from(t: sync::Arc<T>) -> Self {
		Self(t)
	}
}
impl<T: ?Sized> Into<sync::Arc<T>> for Arc<T> {
	fn into(self) -> sync::Arc<T> {
		self.0
	}
}
impl<T> From<T> for Arc<T> {
	fn from(t: T) -> Self {
		Self(sync::Arc::new(t))
	}
}
impl<T: ?Sized> Clone for Arc<T> {
	fn clone(&self) -> Self {
		Self(self.0.clone())
	}
}
impl<T: fmt::Debug + ?Sized> fmt::Debug for Arc<T> {
	fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
		self.0.fmt(f)
	}
}
impl<T: fmt::Display + ?Sized> fmt::Display for Arc<T> {
	fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
		self.0.fmt(f)
	}
}

