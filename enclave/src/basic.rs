use std::{
	any, boxed, borrow::{Borrow, BorrowMut}, error, fmt, marker, ops::{self, Deref, DerefMut}, sync, vec::Vec,
};
pub use serde_closure::structs::Peep;
pub use deepsize::DeepSizeOf;

pub trait Data:
    Clone
    + any::Any
    + Default
    + DeepSizeOf
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
        + Default
        + DeepSizeOf
        + Send
        + Sync
        + fmt::Debug
        + serde::ser::Serialize
        + serde::de::DeserializeOwned
        + 'static,
    > Data for T
{
}

pub trait AnyData:
    dyn_clone::DynClone + any::Any + Send + Sync + fmt::Debug + 'static
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

dyn_clone::clone_trait_object!(AnyData);

// Automatically implementing the Data trait for all types which implements the required traits
impl<
        T: dyn_clone::DynClone
            + any::Any
            + Send
            + Sync
            + fmt::Debug
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

#[derive(Clone, Default, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Box<T: ?Sized>(boxed::Box<T>);

impl<T> Box<T> {
    // Create a new Box wrapper
    pub fn new(t: T) -> Self {
        Self(boxed::Box::new(t))
    }
}

impl<T: ?Sized> Box<T> {
    // Convert to a regular `std::Boxed::Box<T>`. Coherence rules prevent currently prevent `impl Into<std::boxed::Box<T>> for Box<T>`.
    pub fn into_box(self) -> boxed::Box<T> {
        self.0
    }
}

impl Box<dyn AnyData> {
    // Convert into a `std::boxed::Box<dyn std::any::Any>`.
    pub fn into_any(self) -> boxed::Box<dyn any::Any> {
        self.0.into_any()
    }
}

impl<T: ?Sized + marker::Unsize<U>, U: ?Sized> ops::CoerceUnsized<Box<U>> for Box<T> {}

impl<T: ?Sized> Deref for Box<T> {
    type Target = boxed::Box<T>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T: ?Sized> DerefMut for Box<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl<T: ?Sized> AsRef<boxed::Box<T>> for Box<T> {
    fn as_ref(&self) -> &boxed::Box<T> {
        &self.0
    }
}

impl<T: ?Sized> AsMut<boxed::Box<T>> for Box<T> {
    fn as_mut(&mut self) -> &mut boxed::Box<T> {
        &mut self.0
    }
}

impl<T: ?Sized> AsRef<T> for Box<T> {
    fn as_ref(&self) -> &T {
        &*self.0
    }
}

impl<T: ?Sized> AsMut<T> for Box<T> {
    fn as_mut(&mut self) -> &mut T {
        &mut *self.0
    }
}

impl<T: ?Sized> Borrow<T> for Box<T> {
    fn borrow(&self) -> &T {
        &*self.0
    }
}

impl<T: ?Sized> BorrowMut<T> for Box<T> {
    fn borrow_mut(&mut self) -> &mut T {
        &mut *self.0
    }
}

impl<T: ?Sized> From<boxed::Box<T>> for Box<T> {
    fn from(t: boxed::Box<T>) -> Self {
        Self(t)
    }
}

impl<T> From<T> for Box<T> {
    fn from(t: T) -> Self {
        Self(boxed::Box::new(t))
    }
}

impl<T: error::Error> error::Error for Box<T> {
    fn description(&self) -> &str {
        error::Error::description(&**self)
    }
    #[allow(deprecated)]
    fn cause(&self) -> Option<&dyn error::Error> {
        error::Error::cause(&**self)
    }
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        error::Error::source(&**self)
    }
}

impl<T: fmt::Debug + ?Sized> fmt::Debug for Box<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        self.0.fmt(f)
    }
}

impl<T: fmt::Display + ?Sized> fmt::Display for Box<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        self.0.fmt(f)
    }
}

impl<A, F: ?Sized> ops::FnOnce<A> for Box<F>
where
    F: FnOnce<A>,
{
    type Output = F::Output;
    extern "rust-call" fn call_once(self, args: A) -> Self::Output {
        self.0.call_once(args)
    }
}

impl<A, F: ?Sized> ops::FnMut<A> for Box<F>
where
    F: FnMut<A>,
{
    extern "rust-call" fn call_mut(&mut self, args: A) -> Self::Output {
        self.0.call_mut(args)
    }
}

impl<A, F: ?Sized> ops::Fn<A> for Box<F>
where
    F: Func<A>,
{
    extern "rust-call" fn call(&self, args: A) -> Self::Output {
        self.0.call(args)
    }
}

pub trait SerFunc<Args>:
    Fn<Args>
    + Send
    + Sync
    + Clone
    + 'static
    + Peep
{
}

impl<Args, T> SerFunc<Args> for T where
    T: Fn<Args>
        + Send
        + Sync
        + Clone
        + 'static
        + Peep
{
}

pub trait Func<Args>:
    ops::Fn<Args> + Send + Sync + 'static + dyn_clone::DynClone + Peep
{
}

impl<T: ?Sized, Args> Func<Args> for T where
    T: ops::Fn<Args> + Send + Sync + 'static + dyn_clone::DynClone + Peep
{
}

impl<Args: 'static, Output: 'static> std::clone::Clone
    for boxed::Box<dyn Func<Args, Output = Output>>
{
    fn clone(&self) -> Self {
        dyn_clone::clone_box(&**self)
    }
}

impl<'a, Args, Output> AsRef<Self> for dyn Func<Args, Output = Output> + 'a {
    fn as_ref(&self) -> &Self {
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


impl<Args: 'static, Output: 'static> Peep for boxed::Box<dyn Func<Args, Output = Output>>
{
    fn get_ser_captured_var(&self) -> Vec<Vec<u8>> {
        (**self).get_ser_captured_var()
    }
    
    fn deser_captured_var(&mut self, ser: &Vec<Vec<u8>>) {
        (**self).deser_captured_var(ser)
    }

    fn has_captured_var(&self) -> bool {
        (**self).has_captured_var()
    }
    
}


