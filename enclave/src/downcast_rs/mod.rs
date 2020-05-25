#![deny(unsafe_code)]
//! Rust enums are great for types where all variations are known beforehand. But a
//! container of user-defined types requires an open-ended type like a **trait
//! object**. Some applications may want to cast these trait objects back to the
//! original concrete types to access additional functionality and performant
//! inlined implementations.
//!
//! `downcast-rs` adds this downcasting support to trait objects using only safe
//! Rust. It supports **type parameters**, **associated types**, and **constraints**.
//!
//! To make a trait downcastable, make it extend either `downcast::Downcast` or
//! `downcast::DowncastSync` and invoke `impl_downcast!` on it as in the examples
//! below.
//!
//! Since 1.1.0, the minimum supported Rust version is 1.33 to support `Rc` and `Arc`
//! in the receiver position.
//!
//! ```
//! # #[macro_use]
//! # extern crate downcast_rs;
//! # use downcast_rs::{Downcast, DowncastSync};
//! trait Trait: Downcast {}
//! impl_downcast!(Trait);
//!
//! // Also supports downcasting `Arc`-ed trait objects by extending `DowncastSync`
//! // and starting `impl_downcast!` with `sync`.
//! trait TraitSync: DowncastSync {}
//! impl_downcast!(sync TraitSync);
//!
//! // With type parameters.
//! trait TraitGeneric1<T>: Downcast {}
//! impl_downcast!(TraitGeneric1<T>);
//!
//! // With associated types.
//! trait TraitGeneric2: Downcast { type G; type H; }
//! impl_downcast!(TraitGeneric2 assoc G, H);
//!
//! // With constraints on types.
//! trait TraitGeneric3<T: Copy>: Downcast {
//!     type H: Clone;
//! }
//! impl_downcast!(TraitGeneric3<T> assoc H where T: Copy, H: Clone);
//!
//! // With concrete types.
//! trait TraitConcrete1<T: Copy>: Downcast {}
//! impl_downcast!(concrete TraitConcrete1<u32>);
//!
//! trait TraitConcrete2<T: Copy>: Downcast { type H; }
//! impl_downcast!(concrete TraitConcrete2<u32> assoc H=f64);
//! # fn main() {}
//! ```
//!
//! # Example without generics
//!
//! ```
//! # use std::rc::Rc;
//! # use std::sync::Arc;
//! // Import macro via `macro_use` pre-1.30.
//! #[macro_use]
//! extern crate downcast_rs;
//! use downcast_rs::DowncastSync;
//!
//! // To create a trait with downcasting methods, extend `Downcast` or `DowncastSync`
//! // and run `impl_downcast!()` on the trait.
//! trait Base: DowncastSync {}
//! impl_downcast!(sync Base);  // `sync` => also produce `Arc` downcasts.
//!
//! // Concrete types implementing Base.
//! #[derive(Debug)]
//! struct Foo(u32);
//! impl Base for Foo {}
//! #[derive(Debug)]
//! struct Bar(f64);
//! impl Base for Bar {}
//!
//! fn main() {
//!     // Create a trait object.
//!     let mut base: Box<Base> = Box::new(Foo(42));
//!
//!     // Try sequential downcasts.
//!     if let Some(foo) = base.downcast_ref::<Foo>() {
//!         assert_eq!(foo.0, 42);
//!     } else if let Some(bar) = base.downcast_ref::<Bar>() {
//!         assert_eq!(bar.0, 42.0);
//!     }
//!
//!     assert!(base.is::<Foo>());
//!
//!     // Fail to convert `Box<Base>` into `Box<Bar>`.
//!     let res = base.downcast::<Bar>();
//!     assert!(res.is_err());
//!     let base = res.unwrap_err();
//!     // Convert `Box<Base>` into `Box<Foo>`.
//!     assert_eq!(42, base.downcast::<Foo>().map_err(|_| "Shouldn't happen.").unwrap().0);
//!
//!     // Also works with `Rc`.
//!     let mut rc: Rc<Base> = Rc::new(Foo(42));
//!     assert_eq!(42, rc.downcast_rc::<Foo>().map_err(|_| "Shouldn't happen.").unwrap().0);
//!
//!     // Since this trait is `Sync`, it also supports `Arc` downcasts.
//!     let mut arc: Arc<Base> = Arc::new(Foo(42));
//!     assert_eq!(42, arc.downcast_arc::<Foo>().map_err(|_| "Shouldn't happen.").unwrap().0);
//! }
//! ```
//!
//! # Example with a generic trait with associated types and constraints
//!
//! ```
//! // Can call macro via namespace since rust 1.30.
//! extern crate downcast_rs;
//! use downcast_rs::Downcast;
//!
//! // To create a trait with downcasting methods, extend `Downcast` or `DowncastSync`
//! // and run `impl_downcast!()` on the trait.
//! trait Base<T: Clone>: Downcast { type H: Copy; }
//! downcast_rs::impl_downcast!(Base<T> assoc H where T: Clone, H: Copy);
//! // or: impl_downcast!(concrete Base<u32> assoc H=f32)
//!
//! // Concrete types implementing Base.
//! struct Foo(u32);
//! impl Base<u32> for Foo { type H = f32; }
//! struct Bar(f64);
//! impl Base<u32> for Bar { type H = f32; }
//!
//! fn main() {
//!     // Create a trait object.
//!     let mut base: Box<Base<u32, H=f32>> = Box::new(Bar(42.0));
//!
//!     // Try sequential downcasts.
//!     if let Some(foo) = base.downcast_ref::<Foo>() {
//!         assert_eq!(foo.0, 42);
//!     } else if let Some(bar) = base.downcast_ref::<Bar>() {
//!         assert_eq!(bar.0, 42.0);
//!     }
//!
//!     assert!(base.is::<Bar>());
//! }
//! ```

use std::any::Any;
use std::boxed::Box;

/// Supports conversion to `Any`. Traits to be extended by `impl_downcast!` must extend `Downcast`.
pub trait Downcast: Any {
    /// Convert `Box<Trait>` (where `Trait: Downcast`) to `Box<Any>`. `Box<Any>` can then be
    /// further `downcast` into `Box<ConcreteType>` where `ConcreteType` implements `Trait`.
    fn into_any(self: Box<Self>) -> Box<Any>;
    /// Convert `Rc<Trait>` (where `Trait: Downcast`) to `Rc<Any>`. `Rc<Any>` can then be
    /// further `downcast` into `Rc<ConcreteType>` where `ConcreteType` implements `Trait`.
    fn into_any_rc(self: ::std::rc::Rc<Self>) -> ::std::rc::Rc<Any>;
    /// Convert `&Trait` (where `Trait: Downcast`) to `&Any`. This is needed since Rust cannot
    /// generate `&Any`'s vtable from `&Trait`'s.
    fn as_any(&self) -> &Any;
    /// Convert `&mut Trait` (where `Trait: Downcast`) to `&Any`. This is needed since Rust cannot
    /// generate `&mut Any`'s vtable from `&mut Trait`'s.
    fn as_any_mut(&mut self) -> &mut Any;
}

impl<T: Any> Downcast for T {
    fn into_any(self: Box<Self>) -> Box<Any> { self }
    fn into_any_rc(self: ::std::rc::Rc<Self>) -> ::std::rc::Rc<Any> { self }
    fn as_any(&self) -> &Any { self }
    fn as_any_mut(&mut self) -> &mut Any { self }
}

/// Extends `Downcast` to support `Sync` traits that thus support `Arc` downcasting as well.
pub trait DowncastSync: Downcast + Send + Sync {
    /// Convert `Arc<Trait>` (where `Trait: Downcast`) to `Arc<Any>`. `Arc<Any>` can then be
    /// further `downcast` into `Arc<ConcreteType>` where `ConcreteType` implements `Trait`.
    fn into_any_arc(self: ::std::sync::Arc<Self>) -> ::std::sync::Arc<Any + Send + Sync>;
}

impl<T: Any + Send + Sync> DowncastSync for T {
    fn into_any_arc(self: ::std::sync::Arc<Self>) -> ::std::sync::Arc<Any + Send + Sync> { self }
}

/// Adds downcasting support to traits that extend `downcast::Downcast` by defining forwarding
/// methods to the corresponding implementations on `std::any::Any` in the standard library.
///
/// See https://users.rust-lang.org/t/how-to-create-a-macro-to-impl-a-provided-type-parametrized-trait/5289
/// for why this is implemented this way to support templatized traits.
#[macro_export(local_inner_macros)]
macro_rules! impl_downcast {
    (@impl_full
        $trait_:ident [$($param_types:tt)*]
        for [$($forall_types:ident),*]
        where [$($preds:tt)*]
    ) => {
        impl_downcast! {
            @inject_where
                [impl<$($forall_types),*> dyn $trait_<$($param_types)*>]
                types [$($forall_types),*]
                where [$($preds)*]
                [{
                    impl_downcast! { @impl_body $trait_ [$($param_types)*] }
                }]
        }
    };

    (@impl_full_sync
        $trait_:ident [$($param_types:tt)*]
        for [$($forall_types:ident),*]
        where [$($preds:tt)*]
    ) => {
        impl_downcast! {
            @inject_where
                [impl<$($forall_types),*> $trait_<$($param_types)*>]
                types [$($forall_types),*]
                where [$($preds)*]
                [{
                    impl_downcast! { @impl_body $trait_ [$($param_types)*] }
                    impl_downcast! { @impl_body_sync $trait_ [$($param_types)*] }
                }]
        }
    };

    (@impl_body $trait_:ident [$($types:tt)*]) => {
        /// Returns true if the trait object wraps an object of type `__T`.
        #[inline]
        pub fn is<__T: $trait_<$($types)*>>(&self) -> bool {
            $crate::downcast_rs::Downcast::as_any(self).is::<__T>()
        }
        /// Returns a boxed object from a boxed trait object if the underlying object is of type
        /// `__T`. Returns the original boxed trait if it isn't.
        #[inline]
        pub fn downcast<__T: $trait_<$($types)*>>(
            self: ::std::boxed::Box<Self>
        ) -> ::std::result::Result<::std::boxed::Box<__T>, ::std::boxed::Box<Self>> {
            if self.is::<__T>() {
                Ok($crate::downcast_rs::Downcast::into_any(self).downcast::<__T>().unwrap())
            } else {
                Err(self)
            }
        }
        /// Returns an `Rc`-ed object from an `Rc`-ed trait object if the underlying object is of
        /// type `__T`. Returns the original `Rc`-ed trait if it isn't.
        #[inline]
        pub fn downcast_rc<__T: $trait_<$($types)*>>(
            self: ::std::rc::Rc<Self>
        ) -> ::std::result::Result<::std::rc::Rc<__T>, ::std::rc::Rc<Self>> {
            if self.is::<__T>() {
                Ok($crate::downcast_rs::Downcast::into_any_rc(self).downcast::<__T>().unwrap())
            } else {
                Err(self)
            }
        }
        /// Returns a reference to the object within the trait object if it is of type `__T`, or
        /// `None` if it isn't.
        #[inline]
        pub fn downcast_ref<__T: $trait_<$($types)*>>(&self) -> ::std::option::Option<&__T> {
            $crate::downcast_rs::Downcast::as_any(self).downcast_ref::<__T>()
        }
        /// Returns a mutable reference to the object within the trait object if it is of type
        /// `__T`, or `None` if it isn't.
        #[inline]
        pub fn downcast_mut<__T: $trait_<$($types)*>>(&mut self) -> ::std::option::Option<&mut __T> {
            $crate::downcast_rs::Downcast::as_any_mut(self).downcast_mut::<__T>()
        }
    };

    (@impl_body_sync $trait_:ident [$($types:tt)*]) => {
        /// Returns an `Arc`-ed object from an `Arc`-ed trait object if the underlying object is of
        /// type `__T`. Returns the original `Arc`-ed trait if it isn't.
        #[inline]
        pub fn downcast_arc<__T: $trait_<$($types)*>>(
            self: ::std::sync::Arc<Self>,
        ) -> ::std::result::Result<::std::sync::Arc<__T>, ::std::sync::Arc<Self>>
            where __T: ::std::any::Any + ::std::marker::Send + ::std::marker::Sync
        {
            if self.is::<__T>() {
                Ok($crate::downcast_rs::DowncastSync::into_any_arc(self).downcast::<__T>().unwrap())
            } else {
                Err(self)
            }
        }
    };

    (@inject_where [$($before:tt)*] types [] where [] [$($after:tt)*]) => {
        impl_downcast! { @as_item $($before)* $($after)* }
    };

    (@inject_where [$($before:tt)*] types [$($types:ident),*] where [] [$($after:tt)*]) => {
        impl_downcast! {
            @as_item
                $($before)*
                where $( $types: ::std::any::Any + 'static ),*
                $($after)*
        }
    };
    (@inject_where [$($before:tt)*] types [$($types:ident),*] where [$($preds:tt)+] [$($after:tt)*]) => {
        impl_downcast! {
            @as_item
                $($before)*
                where
                    $( $types: ::std::any::Any + 'static, )*
                    $($preds)*
                $($after)*
        }
    };

    (@as_item $i:item) => { $i };

    // No type parameters.
    ($trait_:ident   ) => { impl_downcast! { @impl_full $trait_ [] for [] where [] } };
    ($trait_:ident <>) => { impl_downcast! { @impl_full $trait_ [] for [] where [] } };
    (sync $trait_:ident   ) => { impl_downcast! { @impl_full_sync $trait_ [] for [] where [] } };
    (sync $trait_:ident <>) => { impl_downcast! { @impl_full_sync $trait_ [] for [] where [] } };
    // Type parameters.
    ($trait_:ident < $($types:ident),* >) => {
        impl_downcast! { @impl_full $trait_ [$($types),*] for [$($types),*] where [] }
    };
    (sync $trait_:ident < $($types:ident),* >) => {
        impl_downcast! { @impl_full_sync $trait_ [$($types),*] for [$($types),*] where [] }
    };
    // Type parameters and where clauses.
    ($trait_:ident < $($types:ident),* > where $($preds:tt)+) => {
        impl_downcast! { @impl_full $trait_ [$($types),*] for [$($types),*] where [$($preds)*] }
    };
    (sync $trait_:ident < $($types:ident),* > where $($preds:tt)+) => {
        impl_downcast! { @impl_full_sync $trait_ [$($types),*] for [$($types),*] where [$($preds)*] }
    };
    // Associated types.
    ($trait_:ident assoc $($atypes:ident),*) => {
        impl_downcast! { @impl_full $trait_ [$($atypes = $atypes),*] for [$($atypes),*] where [] }
    };
    (sync $trait_:ident assoc $($atypes:ident),*) => {
        impl_downcast! { @impl_full_sync $trait_ [$($atypes = $atypes),*] for [$($atypes),*] where [] }
    };
    // Associated types and where clauses.
    ($trait_:ident assoc $($atypes:ident),* where $($preds:tt)+) => {
        impl_downcast! { @impl_full $trait_ [$($atypes = $atypes),*] for [$($atypes),*] where [$($preds)*] }
    };
    (sync $trait_:ident assoc $($atypes:ident),* where $($preds:tt)+) => {
        impl_downcast! { @impl_full_sync $trait_ [$($atypes = $atypes),*] for [$($atypes),*] where [$($preds)*] }
    };
    // Type parameters and associated types.
    ($trait_:ident < $($types:ident),* > assoc $($atypes:ident),*) => {
        impl_downcast! {
            @impl_full
                $trait_ [$($types),*, $($atypes = $atypes),*]
                for [$($types),*, $($atypes),*]
                where []
        }
    };
    (sync $trait_:ident < $($types:ident),* > assoc $($atypes:ident),*) => {
        impl_downcast! {
            @impl_full_sync
                $trait_ [$($types),*, $($atypes = $atypes),*]
                for [$($types),*, $($atypes),*]
                where []
        }
    };
    // Type parameters, associated types, and where clauses.
    ($trait_:ident < $($types:ident),* > assoc $($atypes:ident),* where $($preds:tt)+) => {
        impl_downcast! {
            @impl_full
                $trait_ [$($types),*, $($atypes = $atypes),*]
                for [$($types),*, $($atypes),*]
                where [$($preds)*]
        }
    };
    (sync $trait_:ident < $($types:ident),* > assoc $($atypes:ident),* where $($preds:tt)+) => {
        impl_downcast! {
            @impl_full_sync
                $trait_ [$($types),*, $($atypes = $atypes),*]
                for [$($types),*, $($atypes),*]
                where [$($preds)*]
        }
    };
    // Concretely-parametrized types.
    (concrete $trait_:ident < $($types:ident),* >) => {
        impl_downcast! { @impl_full $trait_ [$($types),*] for [] where [] }
    };
    (sync concrete $trait_:ident < $($types:ident),* >) => {
        impl_downcast! { @impl_full_sync $trait_ [$($types),*] for [] where [] }
    };
    // Concretely-associated types types.
    (concrete $trait_:ident assoc $($atypes:ident = $aty:ty),*) => {
        impl_downcast! { @impl_full $trait_ [$($atypes = $aty),*] for [] where [] }
    };
    (sync concrete $trait_:ident assoc $($atypes:ident = $aty:ty),*) => {
        impl_downcast! { @impl_full_sync $trait_ [$($atypes = $aty),*] for [] where [] }
    };
    // Concretely-parametrized types with concrete associated types.
    (concrete $trait_:ident < $($types:ident),* > assoc $($atypes:ident = $aty:ty),*) => {
        impl_downcast! { @impl_full $trait_ [$($types),*, $($atypes = $aty),*] for [] where [] }
    };
    (sync concrete $trait_:ident < $($types:ident),* > assoc $($atypes:ident = $aty:ty),*) => {
        impl_downcast! { @impl_full_sync $trait_ [$($types),*, $($atypes = $aty),*] for [] where [] }
    };
}


