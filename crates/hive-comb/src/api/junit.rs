//! Reads the JUnit XML report a verifier's tests write, as pytest's `--junitxml` does and most
//! test runners can. A run cut short, as by a `sys.exit(0)` the subject slipped into the code under
//! test, exits with 0 all the same, but it leaves no report, or one without the tests that had to
//! pass.
//!
//! Only as much XML as a report uses is read: tags and their attributes, comments, CDATA and the
//! character entities. Text between tags is passed over.

/// How a test came out, from best to worst.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(super) enum Outcome {
    Passed,
    Skipped,
    Failed,
    Error,
}

/// One `testcase` in a report.
#[derive(Debug, PartialEq, Eq)]
pub(super) struct Case {
    /// The class name and the name, joined by a dot, as `tests.test_a.TestA.test_b`.
    pub(super) id: String,
    pub(super) outcome: Outcome,
}

/// What a report says of a run.
#[derive(Debug, PartialEq)]
pub(super) struct Verdict {
    /// The report was there, had tests, none failed or errored and every one that had to pass
    /// did.
    pub(super) ok: bool,
    /// The tests that had to pass that the report does not show passing, in the order asked for.
    pub(super) not_passed: Vec<String>,
    /// The tests passed, failed, errored and skipped, with the names the pytest summary gets in
    /// the scores. Empty when there was no report.
    pub(super) counts: Vec<(&'static str, f64)>,
}

/// The cases in `xml`, or `None` when it is cut short or has no `testsuite` tag and so is not a
/// report.
pub(super) fn cases(xml: &[u8]) -> Option<Vec<Case>> {
    let xml = String::from_utf8_lossy(xml);
    let mut rest: &str = &xml;
    let mut suite = false;
    let mut cases = Vec::new();
    let mut open: Option<Case> = None;
    while let Some(at) = rest.find('<') {
        rest = &rest[at..];
        if let Some(r) = rest.strip_prefix("<!--") {
            rest = after(r, "-->")?;
            continue;
        }
        if let Some(r) = rest.strip_prefix("<![CDATA[") {
            rest = after(r, "]]>")?;
            continue;
        }
        if rest.starts_with("<?") || rest.starts_with("<!") {
            rest = after(rest, ">")?;
            continue;
        }
        let (tag, r) = tag(rest)?;
        rest = r;
        match tag.name {
            "testsuite" | "testsuites" => suite = true,
            "testcase" if tag.end => cases.extend(open.take()),
            "testcase" => {
                let class = tag.attr("classname");
                let name = tag.attr("name");
                let id = if class.is_empty() { name } else { format!("{class}.{name}") };
                let case = Case { id, outcome: Outcome::Passed };
                if tag.empty {
                    cases.push(case);
                } else {
                    // A case left open is cut short, which a whole report never is.
                    if open.replace(case).is_some() {
                        return None;
                    }
                }
            }
            "failure" | "error" | "skipped" if !tag.end => {
                if let Some(case) = &mut open {
                    let outcome = match tag.name {
                        "failure" => Outcome::Failed,
                        "error" => Outcome::Error,
                        _ => Outcome::Skipped,
                    };
                    case.outcome = case.outcome.max(outcome);
                }
            }
            _ => {}
        }
    }
    (suite && open.is_none()).then_some(cases)
}

/// Judges a run by its report's `cases`, `None` when it left none, and the tests that had to pass.
/// A test passes when it is in the report and passed each time it is there, since pytest writes
/// an error in teardown as a second case of the same name.
pub(super) fn judge(cases: Option<&[Case]>, must_pass: &[String]) -> Verdict {
    let Some(cases) = cases else {
        return Verdict { ok: false, not_passed: must_pass.to_vec(), counts: Vec::new() };
    };
    let count = |o| cases.iter().filter(|c| c.outcome == o).count() as f64;
    let counts = vec![
        ("tests_passed", count(Outcome::Passed)),
        ("tests_failed", count(Outcome::Failed)),
        ("tests_errors", count(Outcome::Error)),
        ("tests_skipped", count(Outcome::Skipped)),
    ];
    let not_passed: Vec<String> = must_pass
        .iter()
        .filter(|t| {
            let id = report_id(t);
            let mut seen = cases.iter().filter(|c| c.id == id).peekable();
            seen.peek().is_none() || seen.any(|c| c.outcome != Outcome::Passed)
        })
        .cloned()
        .collect();
    let bad = cases.iter().any(|c| c.outcome >= Outcome::Failed);
    Verdict { ok: !cases.is_empty() && !bad && not_passed.is_empty(), not_passed, counts }
}

/// A test as the report names it. A pytest node id, like `tests/test_a.py::TestA::test_b[x]`,
/// becomes `tests.test_a.TestA.test_b[x]`, and anything else is taken as it is.
pub(super) fn report_id(id: &str) -> String {
    let (head, params) = id.find('[').map_or((id, ""), |i| id.split_at(i));
    let Some((file, rest)) = head.split_once("::") else {
        return id.to_owned();
    };
    let module = file.strip_suffix(".py").unwrap_or(file).replace('/', ".");
    format!("{module}.{}{params}", rest.replace("::", "."))
}

/// What follows the first `end` in `s`.
fn after<'a>(s: &'a str, end: &str) -> Option<&'a str> {
    s.find(end).map(|i| &s[i + end.len()..])
}

/// A start, end or empty tag.
struct Tag<'a> {
    name: &'a str,
    /// `</name>`.
    end: bool,
    /// `<name/>`.
    empty: bool,
    attrs: Vec<(&'a str, &'a str)>,
}

impl Tag<'_> {
    /// The value of attribute `key`, with its entities read, or empty.
    fn attr(&self, key: &str) -> String {
        self.attrs.iter().find(|(k, _)| *k == key).map(|(_, v)| unescape(v)).unwrap_or_default()
    }
}

/// The tag `s` starts with, and what follows it. Every byte it stops at is ASCII, so the slices
/// fall on character boundaries.
fn tag(s: &str) -> Option<(Tag<'_>, &str)> {
    let b = s.as_bytes();
    let end = b.get(1) == Some(&b'/');
    let mut i = if end { 2 } else { 1 };
    let start = i;
    while i < b.len() && !matches!(b[i], b'/' | b'>') && !b[i].is_ascii_whitespace() {
        i += 1;
    }
    let mut tag = Tag { name: &s[start..i], end, empty: false, attrs: Vec::new() };
    loop {
        while b.get(i).is_some_and(u8::is_ascii_whitespace) {
            i += 1;
        }
        match (b.get(i)?, b.get(i + 1)) {
            (b'>', _) => return Some((tag, &s[i + 1..])),
            (b'/', Some(b'>')) => {
                tag.empty = true;
                return Some((tag, &s[i + 2..]));
            }
            _ => {}
        }
        let key = i;
        while i < b.len() && !matches!(b[i], b'=' | b'/' | b'>') && !b[i].is_ascii_whitespace() {
            i += 1;
        }
        let key = &s[key..i];
        while b.get(i).is_some_and(u8::is_ascii_whitespace) {
            i += 1;
        }
        if b.get(i) != Some(&b'=') {
            // A stray `/` or a word with no value, which is not XML, but is passed over.
            i += usize::from(key.is_empty());
            continue;
        }
        i += 1;
        while b.get(i).is_some_and(u8::is_ascii_whitespace) {
            i += 1;
        }
        let quote = *b.get(i)?;
        if quote != b'"' && quote != b'\'' {
            return None;
        }
        let len = s[i + 1..].find(char::from(quote))?;
        tag.attrs.push((key, &s[i + 1..i + 1 + len]));
        i += len + 2;
    }
}

/// `s` with its entities read. One that cannot be read is left as it is.
fn unescape(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut rest = s;
    while let Some(at) = rest.find('&') {
        out.push_str(&rest[..at]);
        rest = &rest[at..];
        let read = rest.find(';').and_then(|end| {
            let c = match &rest[1..end] {
                "lt" => '<',
                "gt" => '>',
                "amp" => '&',
                "quot" => '"',
                "apos" => '\'',
                e => {
                    let n = match e.strip_prefix("#x").or_else(|| e.strip_prefix("#X")) {
                        Some(hex) => u32::from_str_radix(hex, 16).ok()?,
                        None => e.strip_prefix('#')?.parse().ok()?,
                    };
                    char::from_u32(n)?
                }
            };
            Some((c, end))
        });
        match read {
            Some((c, end)) => {
                out.push(c);
                rest = &rest[end + 1..];
            }
            None => {
                out.push('&');
                rest = &rest[1..];
            }
        }
    }
    out.push_str(rest);
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    /// As pytest 8 writes it, with a teardown error written as a second case of the same test.
    const REPORT: &str = r#"<?xml version="1.0" encoding="utf-8"?><testsuites name="pytest tests"><testsuite name="pytest" errors="1" failures="1" skipped="1" tests="5" time="0.050" timestamp="2026-10-07T17:40:00" hostname="cell"><testcase classname="tests.test_a" name="test_ok" time="0.001" /><testcase classname="tests.test_a.TestB" name="test_p[a&lt;b-1]" time="0.001"><system-out><![CDATA[<failure> in the output is not one
]]></system-out></testcase><testcase classname="tests.test_a" name="test_bad" time="0.002"><failure message="assert 1 == 2">def test_bad():
&gt;       assert 1 == 2
E       assert 1 == 2</failure></testcase><testcase classname="tests.test_a" name="test_skip" time="0.000"><skipped type="pytest.skip" message="later">tests/test_a.py:9: later</skipped></testcase><!-- <testcase name="hidden"/> --><testcase classname="tests.test_a" name="test_ok" time="0.001"><error message="failed on teardown with &quot;boom&quot;">teardown</error></testcase></testsuite></testsuites>"#;

    #[test]
    fn a_pytest_report_is_read_case_by_case() {
        let got = cases(REPORT.as_bytes()).unwrap();
        let got: Vec<_> = got.iter().map(|c| (c.id.as_str(), c.outcome)).collect();
        assert_eq!(
            got,
            [
                ("tests.test_a.test_ok", Outcome::Passed),
                ("tests.test_a.TestB.test_p[a<b-1]", Outcome::Passed),
                ("tests.test_a.test_bad", Outcome::Failed),
                ("tests.test_a.test_skip", Outcome::Skipped),
                ("tests.test_a.test_ok", Outcome::Error),
            ]
        );
    }

    #[test]
    fn what_is_not_a_whole_report_is_none() {
        let cut = &REPORT[..REPORT.find("test_bad").unwrap()];
        assert_eq!(cases(cut.as_bytes()), None);
        assert_eq!(cases(b"Ran 3 tests in 0.001s\n\nOK\n"), None);
        assert_eq!(cases(b"<testsuite><testcase name='a'>"), None);
        assert_eq!(cases(b"<testsuite tests='0'></testsuite>"), Some(Vec::new()));
        // Single quotes, a `>` in a value, and spaces around the `=`.
        let got = cases(b"<testsuite><testcase classname = 'm' name='a>b' /></testsuite>").unwrap();
        assert_eq!(got, [Case { id: "m.a>b".into(), outcome: Outcome::Passed }]);
    }

    #[test]
    fn a_run_passes_on_its_report_only_when_every_test_it_needs_passed() {
        let all = cases(REPORT.as_bytes()).unwrap();
        let must = |ids: &[&str]| ids.iter().map(|s| (*s).to_owned()).collect::<Vec<_>>();
        let v = judge(Some(all.as_slice()), &must(&["tests/test_a.py::TestB::test_p[a<b-1]"]));
        assert_eq!(v.not_passed, Vec::<String>::new());
        assert!(!v.ok, "a test failed");
        assert_eq!(
            v.counts,
            [
                ("tests_passed", 2.0),
                ("tests_failed", 1.0),
                ("tests_errors", 1.0),
                ("tests_skipped", 1.0)
            ]
        );
        let v = judge(
            Some(all.as_slice()),
            &must(&[
                "tests/test_a.py::test_ok",
                "tests.test_a.test_skip",
                "test_gone",
                "tests/test_a.py::test_bad",
            ]),
        );
        assert_eq!(
            v.not_passed,
            [
                "tests/test_a.py::test_ok",
                "tests.test_a.test_skip",
                "test_gone",
                "tests/test_a.py::test_bad"
            ]
        );

        let good: Vec<_> = all.into_iter().filter(|c| c.outcome < Outcome::Failed).collect();
        assert!(judge(Some(good.as_slice()), &must(&["tests/test_a.py::TestB::test_p[a<b-1]"])).ok);
        assert!(!judge(Some(&[][..]), &[]).ok, "no tests ran");
        let v = judge(None, &must(&["a"]));
        assert_eq!(
            (v.ok, v.not_passed.as_slice(), v.counts.len()),
            (false, &["a".to_owned()][..], 0)
        );
    }

    #[test]
    fn node_ids_become_report_names() {
        assert_eq!(report_id("tests/test_a.py::TestA::test_b"), "tests.test_a.TestA.test_b");
        assert_eq!(report_id("test_a.py::test_b[x::y/z.py]"), "test_a.test_b[x::y/z.py]");
        assert_eq!(report_id("tests.test_a.test_b"), "tests.test_a.test_b");
        assert_eq!(unescape("a &amp;&lt;&#65;&#x42;&bogus; &"), "a &<AB&bogus; &");
    }
}
