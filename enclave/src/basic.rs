use std::{
	any, boxed, borrow::{Borrow, BorrowMut}, error, fmt, marker, ops::{self, Deref, DerefMut}, sync, vec::Vec,
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

#[typetag::serde(tag = "type")]
pub trait AnyData:
    crate::dyn_clone::DynClone + any::Any + Send + Sync + fmt::Debug + 'static
{
    fn as_any(&self) -> &dyn any::Any;
    /// Convert to a `&mut std::any::Any`.
    fn as_any_mut(&mut self) -> &mut dyn any::Any;
    /// Convert to a `std::boxed::Box<dyn std::any::Any>`.
    fn into_any(self: boxed::Box<Self>) -> boxed::Box<dyn any::Any>;
    /// Convert to a `std::boxed::Box<dyn std::any::Any + Send>`.
    fn into_any_send(self: boxed::Box<Self>) -> boxed::Box<dyn any::Any + Send>;
    /// Convert to a `std::boxed::Box<dyn std::any::Any + Sync>`.
    fn into_any_sync(self: boxed::Box<Self>) -> boxed::Box<dyn any::Any + Sync>;
    /// Convert to a `std::boxed::Box<dyn std::any::Any + Send + Sync>`.
    fn into_any_send_sync(self: boxed::Box<Self>) -> boxed::Box<dyn any::Any + Send + Sync>;
}

crate::clone_trait_object!(AnyData);

// Automatically implementing the Data trait for all types which implements the required traits
impl<
        T: crate::dyn_clone::DynClone
            + any::Any
            + Send
            + Sync
            + fmt::Debug
            + serde::ser::Serialize
            + serde::de::DeserializeOwned
            + 'static,
    > AnyData for T
{
    fn as_any(&self) -> &dyn any::Any {
        self
    }
    fn as_any_mut(&mut self) -> &mut dyn any::Any {
        self
    }
    fn into_any(self: boxed::Box<Self>) -> boxed::Box<dyn any::Any> {
        self
    }
    fn into_any_send(self: boxed::Box<Self>) -> boxed::Box<dyn any::Any + Send> {
        self
    }
    fn into_any_sync(self: boxed::Box<Self>) -> boxed::Box<dyn any::Any + Sync> {
        self
    }
    fn into_any_send_sync(self: boxed::Box<Self>) -> boxed::Box<dyn any::Any + Send + Sync> {
        self
    }
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

