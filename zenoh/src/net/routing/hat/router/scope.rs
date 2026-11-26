//
// Copyright (c) 2026 ZettaScale Technology
//
// This program and the accompanying materials are made available under the
// terms of the Eclipse Public License 2.0 which is available at
// http://www.eclipse.org/legal/epl-2.0, or the Apache License, Version 2.0
// which is available at https://www.apache.org/licenses/LICENSE-2.0.
//
// SPDX-License-Identifier: EPL-2.0 OR Apache-2.0
//
// Contributors:
//   ZettaScale Zenoh Team, <zenoh@zettascale.tech>
//

//! Declaration scope of a south router subregion
//! (`gateway.south[].declaration_scope`).
//!
//! Each router face of the subregion gets a _scope_ derived from the Zenoh ID of the remote. A
//! subscriber, queryable or token declaration is withheld from a face iff its key expression is
//! included in a scoped key expression (e.g. `*/telemetry/**`) and intersects none of the key
//! expressions of that face (e.g. `<scope>/telemetry/**`).
//!
//! The decision depends only on the Zenoh ID of the face and on the key expression, both of
//! which are constant: an undeclaration is withheld iff the declaration was. The one exception is
//! a face that stays a transit next hop for [`TRANSIT_GRACE`]: it is no longer filtered for the
//! rest of its life, and what was withheld is replayed to it.
//!
//! In observe mode nothing is withheld: what would have been is only counted.

use std::{
    fmt,
    sync::atomic::{AtomicU64, Ordering::Relaxed},
    time::{Duration, Instant},
};

use zenoh_config::gateway::{DeclarationScopeConf, DeclarationScopeMode};
use zenoh_keyexpr::{keyexpr, OwnedKeyExpr};
use zenoh_protocol::core::ZenohIdProto;
use zenoh_result::ZResult;

/// The most digits a Zenoh ID can have: 128 bits in radix 2.
const MAX_DIGITS: usize = 128;

/// How long a scoped face that is a transit next hop stays filtered before it is no longer
/// filtered.
///
/// A router that loses its link to this region and reconnects at once, for instance when a
/// gateway restarts, may announce on its new link, for a moment, the links of its previous
/// connection (the previous gateway and the routers behind it): it reconnects before it is done
/// closing the lost transport. It is then a transit next hop until its next link state. That
/// takes about as long as processing the declarations it held, up to a few seconds on a slow
/// router; the grace period leaves a margin over that and is still short next to the time an
/// operator takes to notice a router that is really wired as a transit router.
pub(super) const TRANSIT_GRACE: Duration = Duration::from_secs(10);

/// The declaration scope of a region.
pub(super) struct DeclarationScope {
    radix: u32,
    skip: usize,
    length: usize,
    /// Bit `d` is set iff the digit of value `d` may appear in a scope.
    alphabet: u64,
    /// The scoped key expressions. The first chunk of each is `*`.
    key_exprs: Vec<OwnedKeyExpr>,
    mode: DeclarationScopeMode,
}

/// Why a Zenoh ID has no scope.
#[derive(Debug, PartialEq, Eq)]
pub(super) enum NoScope {
    /// The text of the Zenoh ID does not have the configured length.
    Length { actual: usize, expected: usize },
    /// The scope contains a character that is not in the configured alphabet.
    Alphabet(char),
    /// The scope does not make a valid key expression.
    KeyExpr,
}

impl fmt::Display for NoScope {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            NoScope::Length { actual, expected } => {
                write!(f, "its text has {actual} characters instead of {expected}")
            }
            NoScope::Alphabet(c) => write!(f, "{c:?} is not in the scope alphabet"),
            NoScope::KeyExpr => write!(f, "it does not make a valid key expression"),
        }
    }
}

/// The scope of a face, computed once when the face is created.
#[derive(Debug)]
pub(super) struct FaceScope {
    /// The scoped key expressions with their first chunk replaced by the scope of the face.
    key_exprs: Box<[OwnedKeyExpr]>,
    /// The face is a transit next hop towards a router outside of its scope, as of the last
    /// computation of the trees.
    pub(super) transit: Option<Transit>,
    /// The face stayed a transit next hop for the grace period: it is no longer filtered.
    pub(super) failed_open: bool,
    /// How many times the face was a transit next hop for less than the grace period.
    pub(super) transient: u64,
    /// Declarations and undeclarations that were withheld (enforce mode) or would have been
    /// (observe mode). Replays count again.
    pub(super) out_of_scope: [AtomicU64; 2],
    /// Declarations received from the router of the face itself that are scoped but out of its
    /// scope.
    pub(super) foreign: AtomicU64,
}

/// A scoped face that is a transit next hop towards a router outside of its scope.
#[derive(Debug, Clone, Copy)]
pub(super) struct Transit {
    /// A router outside of the scope of the face that is behind it, as of the last computation
    /// of the trees.
    pub(super) towards: ZenohIdProto,
    /// When the face was found to be a transit next hop, after it was last found to be a leaf.
    pub(super) since: Instant,
}

impl FaceScope {
    /// Returns `true` if a declaration on `key_expr` can concern the face.
    #[inline]
    pub(super) fn concerns(&self, key_expr: &keyexpr) -> bool {
        self.key_exprs.iter().any(|ke| ke.intersects(key_expr))
    }

    /// The scope of the face.
    pub(super) fn scope(&self) -> &str {
        let ke = self.key_exprs[0].as_str();
        ke.split('/').next().unwrap_or(ke)
    }

    /// Counts a declaration (`undeclaration` is `false`) or an undeclaration that is out of
    /// scope. Returns `true` if it is the first of its kind.
    #[inline]
    pub(super) fn count_out_of_scope(&self, undeclaration: bool) -> bool {
        self.out_of_scope[undeclaration as usize].fetch_add(1, Relaxed) == 0
    }

    pub(super) fn counts(&self) -> [u64; 3] {
        [
            self.out_of_scope[0].load(Relaxed),
            self.out_of_scope[1].load(Relaxed),
            self.foreign.load(Relaxed),
        ]
    }
}

impl DeclarationScope {
    pub(super) fn new(conf: &DeclarationScopeConf) -> ZResult<Self> {
        let zid = &conf.zid;
        if !(2..=36).contains(&zid.radix) {
            bail!(
                "declaration_scope: `zid.radix` must be between 2 and 36, got {}",
                zid.radix
            );
        }
        if zid.length == 0 {
            bail!("declaration_scope: `zid.length` must not be 0");
        }
        if zid.skip.saturating_add(zid.length) > MAX_DIGITS {
            bail!(
                "declaration_scope: `zid.skip` + `zid.length` must not exceed {MAX_DIGITS}, got {}",
                zid.skip.saturating_add(zid.length)
            );
        }
        if zid.alphabet.is_empty() {
            bail!("declaration_scope: `zid.alphabet` must not be empty");
        }
        let mut alphabet = 0u64;
        for c in zid.alphabet.chars() {
            match c.to_digit(zid.radix) {
                Some(digit) if !c.is_ascii_lowercase() => alphabet |= 1 << digit,
                _ => bail!(
                    "declaration_scope: {c:?} in `zid.alphabet` is not a digit of radix {} \
                     (digits are `0-9` then `A-Z`)",
                    zid.radix
                ),
            }
        }
        let key_exprs = conf.key_exprs.iter().cloned().collect::<Vec<_>>();
        for ke in &key_exprs {
            if suffix_after_scope(ke).is_none() {
                bail!("declaration_scope: the first chunk of key expression `{ke}` must be `*`");
            }
        }
        Ok(Self {
            radix: zid.radix,
            skip: zid.skip,
            length: zid.length,
            alphabet,
            key_exprs,
            mode: conf.mode,
        })
    }

    /// Returns `true` in observe mode: nothing is withheld.
    #[inline]
    pub(super) fn observe(&self) -> bool {
        self.mode == DeclarationScopeMode::Observe
    }

    /// Returns `true` if declarations on `key_expr` are subject to scoping, i.e. if `key_expr` is
    /// included in one of the scoped key expressions.
    #[inline]
    pub(super) fn is_scoped(&self, key_expr: &keyexpr) -> bool {
        self.key_exprs.iter().any(|ke| ke.includes(key_expr))
    }

    /// Computes the scope of the face of `zid`.
    pub(super) fn face_scope(&self, zid: &ZenohIdProto) -> Result<FaceScope, NoScope> {
        let mut buf = [0u8; MAX_DIGITS];
        let scope = self.scope_of(zid, &mut buf)?;
        let key_exprs = self
            .key_exprs
            .iter()
            .map(|ke| {
                // The first chunk of `ke` is `*`, see `DeclarationScope::new`.
                let suffix = suffix_after_scope(ke).unwrap_or_default();
                OwnedKeyExpr::new([scope, suffix].concat()).map_err(|_| NoScope::KeyExpr)
            })
            .collect::<Result<_, _>>()?;
        Ok(FaceScope {
            key_exprs,
            transit: None,
            failed_open: false,
            transient: 0,
            out_of_scope: Default::default(),
            foreign: AtomicU64::new(0),
        })
    }

    /// Returns `true` if `zid` yields the scope of `face`.
    pub(super) fn has_scope_of(&self, zid: &ZenohIdProto, face: &FaceScope) -> bool {
        let mut buf = [0u8; MAX_DIGITS];
        self.scope_of(zid, &mut buf)
            .is_ok_and(|scope| scope == face.scope())
    }

    /// Writes the scope of `zid` to `buf` and returns it.
    fn scope_of<'b>(
        &self,
        zid: &ZenohIdProto,
        buf: &'b mut [u8; MAX_DIGITS],
    ) -> Result<&'b str, NoScope> {
        let radix = self.radix as u128;
        let mut value = u128::from_le_bytes(zid.to_le_bytes());
        let mut start = MAX_DIGITS;
        while value != 0 {
            start -= 1;
            buf[start] = (value % radix) as u8;
            value /= radix;
        }
        let digits = &mut buf[start..];
        let expected = self.skip + self.length;
        if digits.len() != expected {
            return Err(NoScope::Length {
                actual: digits.len(),
                expected,
            });
        }
        let scope = &mut digits[self.skip..];
        for digit in scope.iter_mut() {
            let c = char::from_digit(*digit as u32, self.radix)
                .unwrap_or('?')
                .to_ascii_uppercase();
            if self.alphabet & (1 << *digit) == 0 {
                return Err(NoScope::Alphabet(c));
            }
            *digit = c as u8;
        }
        // The scope only holds ASCII digits and upper case letters.
        std::str::from_utf8(scope).map_err(|_| NoScope::KeyExpr)
    }
}

/// Returns what follows the first chunk of `key_expr` if that chunk is `*`: `""` for `*`,
/// `"/a/**"` for `*/a/**`.
fn suffix_after_scope(key_expr: &keyexpr) -> Option<&str> {
    let suffix = key_expr.as_str().strip_prefix('*')?;
    (suffix.is_empty() || suffix.starts_with('/')).then_some(suffix)
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use zenoh_config::gateway::{DeclarationScopeConf, ZidScopeConf};

    use super::*;

    /// 0-9 and A-Z without I and O.
    const ALPHABET: &str = "0123456789ABCDEFGHJKLMNPQRSTUVWXYZ";

    fn conf(key_exprs: &[&str]) -> DeclarationScopeConf {
        serde_json::from_value(serde_json::json!({
            "zid": { "radix": 36, "skip": 1, "length": 19, "alphabet": ALPHABET },
            "key_exprs": key_exprs,
        }))
        .unwrap()
    }

    fn scope() -> DeclarationScope {
        DeclarationScope::new(&conf(&["*/svc/**"])).unwrap()
    }

    fn zid(hex: &str) -> ZenohIdProto {
        ZenohIdProto::from_str(hex).unwrap()
    }

    /// Hexadecimal Zenoh ID of the integer written `text` in radix 36.
    fn zid_of(text: &str) -> ZenohIdProto {
        zid(&format!("{:x}", u128::from_str_radix(text, 36).unwrap()))
    }

    fn scope_of(scope: &DeclarationScope, zid: &ZenohIdProto) -> Result<String, NoScope> {
        let mut buf = [0u8; MAX_DIGITS];
        scope.scope_of(zid, &mut buf).map(str::to_string)
    }

    fn ke(s: &str) -> &keyexpr {
        keyexpr::new(s).unwrap()
    }

    #[test]
    fn scope_is_the_radix_36_text_of_the_zid_after_the_skipped_characters() {
        let scope = scope();
        // Reference vector: `X` + scope, read in radix 36, written in hexadecimal.
        assert_eq!(
            scope_of(&scope, &zid("9e7d846d76b278486140000092")).as_deref(),
            Ok("TEST000000000000042")
        );
        for (tag, sn) in [
            ("X", "TEST000000000000001"),
            ("S", "TEST000000000000001"),
            ("X", "000000000000000000A"),
            ("Z", "ZZZZZZZZZZZZZZZZZZZ"),
            ("1", "0000000000000000000"),
        ] {
            assert_eq!(
                scope_of(&scope, &zid_of(&format!("{tag}{sn}"))).as_deref(),
                Ok(sn)
            );
        }
    }

    #[test]
    fn zid_without_a_valid_scope_is_rejected() {
        let scope = scope();
        // Too short and too long.
        assert_eq!(
            scope_of(&scope, &zid("1")),
            Err(NoScope::Length {
                actual: 1,
                expected: 20
            })
        );
        assert_eq!(
            scope_of(&scope, &zid_of("XTEST0000000000000012")),
            Err(NoScope::Length {
                actual: 21,
                expected: 20
            })
        );
        // A leading zero is not written, so the text is one character short.
        assert_eq!(
            scope_of(&scope, &zid_of("0TEST000000000000001")),
            Err(NoScope::Length {
                actual: 19,
                expected: 20
            })
        );
        // The largest Zenoh ID has 25 characters in radix 36.
        assert_eq!(
            scope_of(&scope, &zid("ffffffffffffffffffffffffffffffff")),
            Err(NoScope::Length {
                actual: 25,
                expected: 20
            })
        );
        // Characters outside of the alphabet.
        assert_eq!(
            scope_of(&scope, &zid_of("XTEST00000000000000I")),
            Err(NoScope::Alphabet('I'))
        );
        assert_eq!(
            scope_of(&scope, &zid_of("XOEST000000000000001")),
            Err(NoScope::Alphabet('O'))
        );
        // The skipped characters are not checked against the alphabet.
        assert!(scope_of(&scope, &zid_of("ITEST000000000000001")).is_ok());
        assert!(scope.face_scope(&zid("1")).is_err());
    }

    #[test]
    fn derivation_follows_the_configuration() {
        let mut conf = conf(&["*/**"]);
        conf.zid = ZidScopeConf {
            radix: 16,
            skip: 0,
            length: 4,
            alphabet: "0123456789ABCDEF".to_string(),
        };
        let scope = DeclarationScope::new(&conf).unwrap();
        assert_eq!(scope_of(&scope, &zid("beef")).as_deref(), Ok("BEEF"));
        assert!(scope_of(&scope, &zid("beef1")).is_err());

        conf.zid = ZidScopeConf {
            radix: 2,
            skip: 0,
            length: 128,
            alphabet: "01".to_string(),
        };
        let scope = DeclarationScope::new(&conf).unwrap();
        assert_eq!(
            scope_of(&scope, &zid("ffffffffffffffffffffffffffffffff")).as_deref(),
            Ok("1".repeat(128).as_str())
        );
    }

    #[test]
    fn invalid_configuration_is_rejected() {
        let invalid = |f: &dyn Fn(&mut DeclarationScopeConf)| {
            let mut conf = conf(&["*/svc/**"]);
            f(&mut conf);
            DeclarationScope::new(&conf).is_err()
        };
        assert!(!invalid(&|_| {}));
        assert!(invalid(&|c| c.zid.radix = 1));
        assert!(invalid(&|c| c.zid.radix = 37));
        assert!(invalid(&|c| c.zid.length = 0));
        assert!(invalid(&|c| c.zid.skip = 110));
        assert!(invalid(&|c| c.zid.skip = usize::MAX));
        assert!(invalid(&|c| c.zid.alphabet = String::new()));
        // Lower case letters, and digits the radix does not have.
        assert!(invalid(&|c| c.zid.alphabet = "0123abc".to_string()));
        assert!(invalid(&|c| c.zid.alphabet = "01/".to_string()));
        assert!(invalid(&|c| {
            c.zid.radix = 10;
            c.zid.alphabet = "0123456789A".to_string()
        }));
        // The first chunk of a scoped key expression must be `*`.
        for ke in ["svc/**", "**", "**/svc", "a/*/svc/**", "$*a/b", "@a/b"] {
            assert!(
                DeclarationScope::new(&conf(&[ke])).is_err(),
                "{ke} should be rejected"
            );
        }
        for ke in ["*", "*/**", "*/svc/**", "*/a/*/b"] {
            assert!(
                DeclarationScope::new(&conf(&[ke])).is_ok(),
                "{ke} should be accepted"
            );
        }
    }

    /// The rule: withheld iff scoped and not concerning the face.
    fn withheld(scope: &DeclarationScope, face: &FaceScope, key_expr: &str) -> bool {
        scope.is_scoped(ke(key_expr)) && !face.concerns(ke(key_expr))
    }

    #[test]
    fn declaration_is_withheld_iff_scoped_and_foreign() {
        const OWN: &str = "TEST000000000000001";
        const OTHER: &str = "TEST000000000000002";

        let scope = scope();
        let face = scope.face_scope(&zid_of(&format!("X{OWN}"))).unwrap();
        assert_eq!(
            face.key_exprs
                .iter()
                .map(|ke| ke.as_str())
                .collect::<Vec<_>>(),
            [format!("{OWN}/svc/**")]
        );

        let withheld_keys = [
            format!("{OTHER}/svc/default/pub/x"),
            format!("{OTHER}/svc"),
            format!("{OTHER}/svc/**"),
            format!("{OTHER}/svc/*/pub/*"),
            // A scope that is a prefix or an extension of the face's own is another scope.
            format!("{}/svc/x", &OWN[..18]),
            format!("{OWN}1/svc/x"),
            // Not a wildcard for the face's scope.
            "TEST00000000000000$*2/svc/x".to_string(),
        ];
        for key in &withheld_keys {
            assert!(withheld(&scope, &face, key), "{key} should be withheld");
        }

        let passing_keys = [
            // The face's own scope.
            format!("{OWN}/svc/default/pub/x"),
            format!("{OWN}/svc"),
            format!("{OWN}/svc/**"),
            // Wildcards that intersect the face's own scope.
            "*/svc/default/pub/x".to_string(),
            "*/svc/**".to_string(),
            "TEST00000000000000$*/svc/x".to_string(),
            "$*1/svc/x".to_string(),
            // Not included in the scoped key expression: never withheld.
            "**".to_string(),
            "**/x".to_string(),
            "*/**".to_string(),
            format!("{OTHER}/**"),
            OTHER.to_string(),
            format!("{OTHER}/other/x"),
            format!("fleet/{OTHER}/svc/x"),
            "svc/x".to_string(),
            format!("@/{OTHER}/svc/x"),
            format!("@{OTHER}/svc/x"),
        ];
        for key in &passing_keys {
            assert!(!withheld(&scope, &face, key), "{key} should pass");
        }
    }

    #[test]
    fn several_scoped_key_expressions() {
        const OWN: &str = "TEST000000000000001";
        const OTHER: &str = "TEST000000000000002";

        let scope = DeclarationScope::new(&conf(&["*/svc/**", "*/@raw/**", "*"])).unwrap();
        let face = scope.face_scope(&zid_of(&format!("X{OWN}"))).unwrap();
        for key in [
            format!("{OTHER}/svc/x"),
            format!("{OTHER}/@raw/x"),
            OTHER.to_string(),
        ] {
            assert!(withheld(&scope, &face, &key), "{key} should be withheld");
        }
        for key in [
            format!("{OWN}/svc/x"),
            format!("{OWN}/@raw/x"),
            OWN.to_string(),
            "*".to_string(),
            format!("{OTHER}/other"),
        ] {
            assert!(!withheld(&scope, &face, &key), "{key} should pass");
        }
    }
    /// The key shapes of a production deployment, with the configuration recommended for it.
    #[test]
    fn production_key_shapes() {
        const OWN: &str = "TEST000000000000001";
        const OTHER: &str = "TEST000000000000002";

        let scope = DeclarationScope::new(&conf(&["*/aorta/**", "*/http_bridge/**"])).unwrap();
        let own = scope.face_scope(&zid_of(&format!("X{OWN}"))).unwrap();
        let other = scope.face_scope(&zid_of(&format!("X{OTHER}"))).unwrap();
        assert_eq!(own.scope(), OWN);

        // Declared on the scope of one robot, by the cloud or by the robot itself: only the
        // bridge of that robot gets them, and the identity check accepts them from that bridge.
        for shape in [
            "aorta/default/act/feedback/cloud/nav",
            "aorta/default/srv/cloud/map/get",
            "aorta/default/act/*/cloud/nav",
            "aorta/default/sys/svc_owner/cloud/nav/node1",
            "http_bridge/app/rpc/**",
            "http_bridge/app/pub/sessions/**",
        ] {
            let key = format!("{OWN}/{shape}");
            assert!(scope.is_scoped(ke(&key)), "{key} should be scoped");
            assert!(own.concerns(ke(&key)), "{key} should reach its own bridge");
            assert!(
                withheld(&scope, &other, &key),
                "{key} should not reach another bridge"
            );
        }

        // Declared by the cloud for every robot: every bridge gets them.
        for key in [
            "**/aorta/default/srv/cloud/credential/issue",
            "aorta/default/sys/svc_owner/cloud/credential/issue/node1",
            "*/aorta/default/pub/x",
        ] {
            assert!(
                !withheld(&scope, &own, key),
                "{key} should reach every bridge"
            );
            assert!(
                !withheld(&scope, &other, key),
                "{key} should reach every bridge"
            );
        }
        // The credential queryable answers a query of a robot on its own scope.
        assert!(
            ke("**/aorta/default/srv/cloud/credential/issue").intersects(ke(&format!(
                "{OWN}/aorta/default/srv/cloud/credential/issue"
            )))
        );
    }
}
