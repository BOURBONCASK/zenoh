use std::{fmt, marker::PhantomData};

use nonempty_collections::NEVec;
use serde::{
    de::{self, MapAccess, SeqAccess, Visitor},
    Deserialize, Serialize,
};
#[allow(unused_imports)]
use zenoh_core::polyfill::*;
use zenoh_protocol::core::{RegionName, WhatAmIMatcher};

use crate::{Interface, ModeDependentValue, ModeValues, ZenohId};

impl<'de> Deserialize<'de> for ModeDependentValue<GatewayConf> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct UniqueOrDependent<U>(PhantomData<fn() -> U>);

        impl<'de> Visitor<'de> for UniqueOrDependent<ModeDependentValue<GatewayConf>> {
            type Value = ModeDependentValue<GatewayConf>;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("gateway config or mode dependent gateway config")
            }

            fn visit_map<M>(self, map: M) -> Result<Self::Value, M::Error>
            where
                M: MapAccess<'de>,
            {
                let value =
                    serde_json::Value::deserialize(de::value::MapAccessDeserializer::new(map))?;

                if let Ok(values) = ModeValues::deserialize(&value) {
                    return Ok(ModeDependentValue::Dependent(values));
                }

                Ok(ModeDependentValue::Unique(
                    GatewayConf::deserialize(&value).map_err(de::Error::custom)?,
                ))
            }
        }

        deserializer.deserialize_any(UniqueOrDependent(PhantomData))
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct GatewayConf {
    pub region_name: Option<RegionName>,
    pub north: Option<ModeDependentValue<NorthBoundConf>>,
    pub south: Option<ModeDependentValue<Vec<SouthBoundConf>>>,
    pub fallback: Option<FallbackBoundConf>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NorthBoundConf {
    /// North bound filters.
    ///
    /// If [`Some`], a subject matches this filter list iff it matches _any_ of the individual
    /// filters. Thus if the list is empty no subject ever matches.
    ///
    /// If [`None`], a subject _always_ matches.
    pub filters: Option<Vec<BoundFilterConf>>,
}

impl Default for ModeDependentValue<NorthBoundConf> {
    fn default() -> Self {
        ModeDependentValue::Dependent(ModeValues {
            router: Some(NorthBoundConf {
                filters: Some(vec![BoundFilterConf {
                    modes: Some(WhatAmIMatcher::empty().router()),
                    interfaces: None,
                    zids: None,
                    region_names: None,
                }]),
            }),
            peer: Some(NorthBoundConf {
                filters: Some(vec![BoundFilterConf {
                    modes: Some(WhatAmIMatcher::empty().router().peer()),
                    interfaces: None,
                    zids: None,
                    region_names: None,
                }]),
            }),
            client: Some(NorthBoundConf {
                filters: Some(vec![BoundFilterConf {
                    modes: Some(WhatAmIMatcher::empty().router().peer()),
                    interfaces: None,
                    zids: None,
                    region_names: None,
                }]),
            }),
        })
    }
}

impl<'de> Deserialize<'de> for ModeDependentValue<NorthBoundConf> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct UniqueOrDependent<U>(PhantomData<fn() -> U>);

        impl<'de> Visitor<'de> for UniqueOrDependent<ModeDependentValue<NorthBoundConf>> {
            type Value = ModeDependentValue<NorthBoundConf>;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("north bound config or mode dependent north bound config")
            }

            fn visit_map<M>(self, map: M) -> Result<Self::Value, M::Error>
            where
                M: MapAccess<'de>,
            {
                let value =
                    serde_json::Value::deserialize(de::value::MapAccessDeserializer::new(map))?;

                if let Ok(values) = ModeValues::deserialize(&value) {
                    return Ok(ModeDependentValue::Dependent(values));
                }

                Ok(ModeDependentValue::Unique(
                    NorthBoundConf::deserialize(&value).map_err(de::Error::custom)?,
                ))
            }
        }

        deserializer.deserialize_any(UniqueOrDependent(PhantomData))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SouthBoundConf {
    /// South bound filters.
    ///
    /// If [`Some`], a subject matches this filter list iff it matches _any_ of the individual
    /// filters. Thus if the list is empty no subject ever matches.
    ///
    /// If [`None`], a subject _always_ matches.
    pub filters: Option<Vec<BoundFilterConf>>,
}

impl Default for ModeDependentValue<Vec<SouthBoundConf>> {
    fn default() -> Self {
        ModeDependentValue::Dependent(ModeValues {
            router: Some(vec![SouthBoundConf {
                filters: Some(vec![BoundFilterConf {
                    modes: Some(WhatAmIMatcher::empty().peer().client()),
                    interfaces: None,
                    zids: None,
                    region_names: None,
                }]),
            }]),
            peer: Some(vec![SouthBoundConf {
                filters: Some(vec![BoundFilterConf {
                    modes: Some(WhatAmIMatcher::empty().client()),
                    interfaces: None,
                    zids: None,
                    region_names: None,
                }]),
            }]),
            client: Some(vec![]),
        })
    }
}

impl<'de> Deserialize<'de> for ModeDependentValue<Vec<SouthBoundConf>> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct UniqueOrDependent<U>(PhantomData<fn() -> U>);

        impl<'de> Visitor<'de> for UniqueOrDependent<ModeDependentValue<Vec<SouthBoundConf>>> {
            type Value = ModeDependentValue<Vec<SouthBoundConf>>;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("north south config or mode dependent south bound config")
            }

            fn visit_seq<A>(self, seq: A) -> Result<Self::Value, A::Error>
            where
                A: SeqAccess<'de>,
            {
                let value =
                    serde_json::Value::deserialize(de::value::SeqAccessDeserializer::new(seq))?;

                if let Ok(values) = ModeValues::deserialize(&value) {
                    return Ok(ModeDependentValue::Dependent(values));
                }

                Ok(ModeDependentValue::Unique(
                    Vec::<SouthBoundConf>::deserialize(&value).map_err(de::Error::custom)?,
                ))
            }
        }

        deserializer.deserialize_any(UniqueOrDependent(PhantomData))
    }
}

/// Bound filter.
///
/// A subject matches this filter iff it matches _all_ of its individual filter fields.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BoundFilterConf {
    pub modes: Option<WhatAmIMatcher>,
    pub interfaces: Option<NEVec<Interface>>,
    pub zids: Option<NEVec<ZenohId>>,
    pub region_names: Option<NEVec<RegionName>>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct FallbackBoundConf {
    pub enabled: bool,
}
