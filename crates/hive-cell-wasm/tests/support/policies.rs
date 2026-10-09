//! Policy plugins for tests, as component text. Each is a core module whose `check` reads the
//! request as the canonical ABI lays it out, lifted to the world in `wit/policy.wit`.
//!
//! The request has more than 16 flat values, so `check` gets a pointer to it. The project is at
//! 0, the source's case at 8 and its string at 12, the backend at 20, the CPU, memory and disk at
//! 28, 32 and 36, the QoS at 40, the network profile at 48, whether there is a hard TTL at 56 and
//! the TTL at 64, `trusted-image` at 72, the labels at 76 and the env names at 84. Each string or
//! list is a pointer and a length, a label is 16 bytes and an env name 8. The verdict goes at 16:
//! its case at 16, then for a change whether there is a network profile at 24 and the profile at
//! 28, whether there is a TTL at 40 and the TTL at 48, and the labels at 56, or for a denial the
//! reason at 24.

/// The policy called `name`, one of `allow`, `small`, `rl`, `echo`, `nowhere`, `trap`, `spin`
/// and `hog`.
#[allow(dead_code)]
pub(crate) fn component(name: &str) -> String {
    let body = match name {
        "allow" => "(i32.store8 (i32.const 16) (i32.const 0)) (i32.const 16)",
        // Turns away a cell that wants more than 4 GiB.
        "small" => {
            r#"(i32.store8 (i32.const 16) (i32.const 0))
            (if (i32.gt_u (i32.load offset=32 (local.get $r)) (i32.const 4096))
              (then (i32.store8 (i32.const 16) (i32.const 2))
                    (i32.store (i32.const 24) (i32.const 512))
                    (i32.store (i32.const 28) (i32.const 15))))
            (i32.const 16)"#
        }
        // Gives a cell of a project named `rl-*` no network, at most an hour and the label
        // `policy=rl`, and leaves every other cell alone.
        "rl" => {
            r#"(local $p i32)
            (local.set $p (i32.load (local.get $r)))
            (i32.store8 (i32.const 16) (i32.const 0))
            (if (i32.and (i32.ge_u (i32.load offset=4 (local.get $r)) (i32.const 3))
                  (i32.and (i32.eq (i32.load8_u (local.get $p)) (i32.const 114))
                    (i32.and (i32.eq (i32.load8_u offset=1 (local.get $p)) (i32.const 108))
                             (i32.eq (i32.load8_u offset=2 (local.get $p)) (i32.const 45)))))
              (then
                (i32.store8 (i32.const 16) (i32.const 1))
                (call $string (i32.const 24) (i32.const 528) (i32.const 4))
                (i32.store8 (i32.const 40) (i32.const 1))
                (i64.store (i32.const 48) (i64.const 3600))
                (if (i32.load8_u offset=56 (local.get $r))
                  (then (if (i64.lt_u (i64.load offset=64 (local.get $r)) (i64.const 3600))
                    (then (i64.store (i32.const 48) (i64.load offset=64 (local.get $r)))))))
                (i32.store (i32.const 56) (i32.const 1024))
                (i32.store (i32.const 60) (i32.const 1))
                (call $pair (i32.const 1024) (i32.const 540) (i32.const 6) (i32.const 548) (i32.const 2))))
            (i32.const 16)"#
        }
        // Everything it was given, as a change: the backend as the network profile, the numbers
        // as one TTL (the source's case times 10^17, the CPU times 10^12, the memory times 10^6,
        // the disk times 1000, 100 for each label, 10 for each env name and 1 when the image is
        // trusted), and labels for the project, the source, the QoS, the network profile, the
        // first label as it is and the last env name. Wants at least one label and env name.
        "echo" => {
            r#"(local $l i32) (local $e i32)
            (if (i32.or (i32.eqz (i32.load offset=80 (local.get $r)))
                        (i32.eqz (i32.load offset=88 (local.get $r))))
              (then unreachable))
            (i32.store8 (i32.const 16) (i32.const 1))
            (call $string (i32.const 24) (i32.load offset=20 (local.get $r)) (i32.load offset=24 (local.get $r)))
            (i32.store8 (i32.const 40) (i32.const 1))
            (i64.store (i32.const 48)
              (i64.add (i64.mul (i64.load8_u offset=8 (local.get $r)) (i64.const 100000000000000000))
              (i64.add (i64.mul (i64.load32_u offset=28 (local.get $r)) (i64.const 1000000000000))
              (i64.add (i64.mul (i64.load32_u offset=32 (local.get $r)) (i64.const 1000000))
              (i64.add (i64.mul (i64.load32_u offset=36 (local.get $r)) (i64.const 1000))
              (i64.add (i64.mul (i64.load32_u offset=80 (local.get $r)) (i64.const 100))
              (i64.add (i64.mul (i64.load32_u offset=88 (local.get $r)) (i64.const 10))
                       (i64.load8_u offset=72 (local.get $r)))))))))
            (i32.store (i32.const 56) (i32.const 1024))
            (i32.store (i32.const 60) (i32.const 6))
            (call $pair (i32.const 1024) (i32.const 560) (i32.const 7)
              (i32.load (local.get $r)) (i32.load offset=4 (local.get $r)))
            (call $pair (i32.const 1040) (i32.const 568) (i32.const 6)
              (i32.load offset=12 (local.get $r)) (i32.load offset=16 (local.get $r)))
            (call $pair (i32.const 1056) (i32.const 576) (i32.const 3)
              (i32.load offset=40 (local.get $r)) (i32.load offset=44 (local.get $r)))
            (call $pair (i32.const 1072) (i32.const 580) (i32.const 7)
              (i32.load offset=48 (local.get $r)) (i32.load offset=52 (local.get $r)))
            (local.set $l (i32.load offset=76 (local.get $r)))
            (call $pair (i32.const 1088) (i32.load (local.get $l)) (i32.load offset=4 (local.get $l))
              (i32.load offset=8 (local.get $l)) (i32.load offset=12 (local.get $l)))
            (local.set $e (i32.add (i32.load offset=84 (local.get $r))
              (i32.mul (i32.sub (i32.load offset=88 (local.get $r)) (i32.const 1)) (i32.const 8))))
            (call $pair (i32.const 1104) (i32.const 588) (i32.const 3)
              (i32.load (local.get $e)) (i32.load offset=4 (local.get $e)))
            (i32.const 16)"#
        }
        // Moves every cell to a network profile no node has.
        "nowhere" => {
            r#"(i32.store8 (i32.const 16) (i32.const 1))
            (call $string (i32.const 24) (i32.const 592) (i32.const 7))
            (i32.store8 (i32.const 40) (i32.const 0))
            (i32.store (i32.const 56) (i32.const 0))
            (i32.store (i32.const 60) (i32.const 0))
            (i32.const 16)"#
        }
        "trap" => "unreachable",
        "spin" => "(loop $l (br $l)) unreachable",
        // Wants 128 MiB more, and traps without it.
        "hog" => {
            r#"(if (i32.eq (memory.grow (i32.const 2048)) (i32.const -1)) (then unreachable))
            (i32.store8 (i32.const 16) (i32.const 0))
            (i32.const 16)"#
        }
        _ => panic!("no test policy {name}"),
    };
    format!(
        r#"(component
  (core module $m
    (memory (export "memory") 1)
    (global $bump (mut i32) (i32.const 4096))
    (data (i32.const 512) "more than 4 GiB")
    (data (i32.const 528) "none")
    (data (i32.const 540) "policy")
    (data (i32.const 548) "rl")
    (data (i32.const 560) "project")
    (data (i32.const 568) "source")
    (data (i32.const 576) "qos")
    (data (i32.const 580) "network")
    (data (i32.const 588) "env")
    (data (i32.const 592) "nowhere")
    (func (export "realloc") (param i32 i32) (param $align i32) (param $size i32) (result i32)
      (local $p i32) (local $want i32)
      (local.set $p (i32.and (i32.add (global.get $bump) (i32.sub (local.get $align) (i32.const 1)))
                             (i32.sub (i32.const 0) (local.get $align))))
      (global.set $bump (i32.add (local.get $p) (local.get $size)))
      (local.set $want (i32.sub (global.get $bump) (i32.shl (memory.size) (i32.const 16))))
      (if (i32.gt_s (local.get $want) (i32.const 0))
        (then (if (i32.eq (memory.grow (i32.add (i32.shr_u (local.get $want) (i32.const 16)) (i32.const 1)))
                          (i32.const -1))
                (then unreachable))))
      (local.get $p))
    ;; Some string at ptr and len, after a 1 at `at` that says it is there.
    (func $string (param $at i32) (param $ptr i32) (param $len i32)
      (i32.store8 (local.get $at) (i32.const 1))
      (i32.store offset=4 (local.get $at) (local.get $ptr))
      (i32.store offset=8 (local.get $at) (local.get $len)))
    ;; A label at `at`: its key, then its value.
    (func $pair (param $at i32) (param $k i32) (param $kl i32) (param $v i32) (param $vl i32)
      (i32.store (local.get $at) (local.get $k))
      (i32.store offset=4 (local.get $at) (local.get $kl))
      (i32.store offset=8 (local.get $at) (local.get $v))
      (i32.store offset=12 (local.get $at) (local.get $vl)))
    (func (export "check") (param $r i32) (result i32)
      {body}))
  (core instance $i (instantiate $m))
  (type $source' (variant (case "template" string) (case "image" string) (case "snapshot" string)))
  (export $source "source" (type $source'))
  (type $request' (record (field "project" string) (field "source" $source)
    (field "backend" string) (field "vcpu-milli" u32) (field "mem-mib" u32)
    (field "disk-gib" u32) (field "qos" string) (field "network-profile" string)
    (field "hard-ttl-s" (option u64)) (field "trusted-image" bool)
    (field "labels" (list (tuple string string))) (field "env" (list string))))
  (export $request "request" (type $request'))
  (type $change' (record (field "network-profile" (option string))
    (field "hard-ttl-s" (option u64)) (field "labels" (list (tuple string string)))))
  (export $change "change" (type $change'))
  (type $verdict' (variant (case "allow") (case "change" $change) (case "deny" string)))
  (export $verdict "verdict" (type $verdict'))
  (func $check (param "request" $request) (result $verdict)
    (canon lift (core func $i "check") (memory (core memory $i "memory"))
      (realloc (core func $i "realloc"))))
  (instance $iface
    (export "source" (type $source))
    (export "request" (type $request))
    (export "change" (type $change))
    (export "verdict" (type $verdict))
    (export "check" (func $check)))
  (export "hivebox:policy/admit@0.1.0" (instance $iface)))
"#
    )
}
