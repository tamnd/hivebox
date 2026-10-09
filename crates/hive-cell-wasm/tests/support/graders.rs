//! Reward plugins for tests, as component text. Each is a core module whose `score` reads the
//! input as the canonical ABI lays it out, lifted to the world in `wit/reward.wit`.
//!
//! `score` takes the input flat: the task, the runs, the files, `passed` and the tampered paths,
//! each list as a pointer and a length. A run is 32 bytes, with the exit code at 0, `timed-out`
//! at 4, stdout at 8, stderr at 16 and the wall time at 24, and a file is 20, with its path at
//! 0, whether it was there at 8 and its bytes at 12. The result goes at 16: 0 at 16 and then the
//! reward at 24 and the detail at 32 for a grade, or 1 at 16 and the message at 24 for an error.

/// The grader called `name`, one of `fraction`, `exact`, `echo`, `fail`, `trap`, `spin`, `hog`
/// and `nan`.
#[allow(dead_code)]
pub(crate) fn component(name: &str) -> String {
    let body = match name {
        // The share of runs that exited with 0, said to be the fraction.
        "fraction" => {
            r#"(local $i i32) (local $n i32) (local $p i32)
            (block $done
              (loop $l
                (br_if $done (i32.ge_u (local.get $i) (local.get $runs_len)))
                (local.set $p (i32.add (local.get $runs) (i32.mul (local.get $i) (i32.const 32))))
                (if (i32.and (i32.eqz (i32.load (local.get $p)))
                             (i32.eqz (i32.load8_u offset=4 (local.get $p))))
                  (then (local.set $n (i32.add (local.get $n) (i32.const 1)))))
                (local.set $i (i32.add (local.get $i) (i32.const 1)))
                (br $l)))
            (f64.div (f64.convert_i32_u (local.get $n)) (f64.convert_i32_u (local.get $runs_len)))
            (call $grade (i32.const 512) (i32.const 8))"#
        }
        // 1 when the last run's stdout is the task, byte for byte, and 0 when it is not.
        "exact" => {
            r#"(local $p i32) (local $s i32) (local $len i32) (local $i i32) (local $same i32)
            (if (i32.eqz (local.get $runs_len)) (then unreachable))
            (local.set $p (i32.add (local.get $runs)
              (i32.mul (i32.sub (local.get $runs_len) (i32.const 1)) (i32.const 32))))
            (local.set $s (i32.load offset=8 (local.get $p)))
            (local.set $len (i32.load offset=12 (local.get $p)))
            (local.set $same (i32.eq (local.get $len) (local.get $task_len)))
            (block $done
              (loop $l
                (br_if $done (i32.eqz (local.get $same)))
                (br_if $done (i32.ge_u (local.get $i) (local.get $len)))
                (local.set $same (i32.eq
                  (i32.load8_u (i32.add (local.get $s) (local.get $i)))
                  (i32.load8_u (i32.add (local.get $task) (local.get $i)))))
                (local.set $i (i32.add (local.get $i) (i32.const 1)))
                (br $l)))
            (f64.convert_i32_u (local.get $same))
            (call $grade (i32.const 520) (i32.const 5))"#
        }
        // Everything it was given as one number, with the task as its detail: `passed`, then 10
        // for each tampered path, 100 for each file asked for, 1000 for each that was there and
        // 10000 for each run.
        "echo" => {
            r#"(local $i i32) (local $there i32)
            (block $done
              (loop $l
                (br_if $done (i32.ge_u (local.get $i) (local.get $files_len)))
                (local.set $there (i32.add (local.get $there) (i32.load8_u offset=8
                  (i32.add (local.get $files) (i32.mul (local.get $i) (i32.const 20))))))
                (local.set $i (i32.add (local.get $i) (i32.const 1)))
                (br $l)))
            (f64.convert_i32_u (i32.add (local.get $passed)
              (i32.add (i32.mul (local.get $tampered_len) (i32.const 10))
                (i32.add (i32.mul (local.get $files_len) (i32.const 100))
                  (i32.add (i32.mul (local.get $there) (i32.const 1000))
                    (i32.mul (local.get $runs_len) (i32.const 10000)))))))
            (call $grade (local.get $task) (local.get $task_len))"#
        }
        "fail" => {
            r#"(i32.store8 (i32.const 16) (i32.const 1))
            (i32.store (i32.const 24) (i32.const 528))
            (i32.store (i32.const 28) (i32.const 9))
            (i32.const 16)"#
        }
        "trap" => "unreachable",
        "spin" => "(loop $l (br $l)) unreachable",
        // Wants 128 MiB more, and traps without it.
        "hog" => {
            r#"(if (i32.eq (memory.grow (i32.const 2048)) (i32.const -1)) (then unreachable))
            (f64.const 1)
            (call $grade (i32.const 0) (i32.const 0))"#
        }
        "nan" => "(f64.const nan) (call $grade (i32.const 0) (i32.const 0))",
        _ => panic!("no test grader {name}"),
    };
    format!(
        r#"(component
  (core module $m
    (memory (export "memory") 1)
    (global $bump (mut i32) (i32.const 4096))
    (data (i32.const 512) "fraction")
    (data (i32.const 520) "exact")
    (data (i32.const 528) "no answer")
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
    ;; A grade of the reward on the stack and the detail at ptr and len.
    (func $grade (param $reward f64) (param $ptr i32) (param $len i32) (result i32)
      (i32.store8 (i32.const 16) (i32.const 0))
      (f64.store (i32.const 24) (local.get $reward))
      (i32.store (i32.const 32) (local.get $ptr))
      (i32.store (i32.const 36) (local.get $len))
      (i32.const 16))
    (func (export "score")
      (param $task i32) (param $task_len i32) (param $runs i32) (param $runs_len i32)
      (param $files i32) (param $files_len i32) (param $passed i32)
      (param $tampered i32) (param $tampered_len i32) (result i32)
      {body}))
  (core instance $i (instantiate $m))
  (type $run' (record (field "exit-code" s32) (field "timed-out" bool) (field "stdout" (list u8))
    (field "stderr" (list u8)) (field "wall-ms" u64)))
  (export $run "run" (type $run'))
  (type $input' (record (field "task" (list u8)) (field "runs" (list $run))
    (field "files" (list (tuple string (option (list u8))))) (field "passed" bool)
    (field "tampered" (list string))))
  (export $input "input" (type $input'))
  (type $grade' (record (field "reward" f64) (field "detail" string)))
  (export $grade "grade" (type $grade'))
  (func $score (param "input" $input) (result (result $grade (error string)))
    (canon lift (core func $i "score") (memory (core memory $i "memory"))
      (realloc (core func $i "realloc"))))
  (instance $iface
    (export "run" (type $run))
    (export "input" (type $input))
    (export "grade" (type $grade))
    (export "score" (func $score)))
  (export "hivebox:reward/score@0.1.0" (instance $iface)))
"#
    )
}
