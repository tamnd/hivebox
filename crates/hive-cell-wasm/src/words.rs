//! Splits a command line into words the way a POSIX shell does for a simple command, without the
//! rest of the shell: there are no variables, globs, pipes or redirects to expand.

use hive_types::{Error, Reason};

/// The words of `line`. Single quotes keep everything up to the next one. Double quotes keep
/// everything but a backslash before `"`, `\`, `$` or a backtick, and a backslash outside quotes
/// keeps the character after it. An unquoted character a shell would do more with than keep,
/// such as `|`, `>`, `;`, `$` or `*`, is refused, since there is no shell to do it.
pub(crate) fn split(line: &str) -> Result<Vec<String>, Error> {
    let mut words = Vec::new();
    let mut word = String::new();
    // A word can be empty, as `''` is, so whether one is under way is kept apart from its text.
    let mut open = false;
    let mut chars = line.chars();
    while let Some(c) = chars.next() {
        match c {
            ' ' | '\t' | '\n' => {
                if open {
                    words.push(std::mem::take(&mut word));
                    open = false;
                }
            }
            '\'' => {
                open = true;
                loop {
                    match chars.next() {
                        Some('\'') => break,
                        Some(c) => word.push(c),
                        None => return Err(unclosed('\'')),
                    }
                }
            }
            '"' => {
                open = true;
                loop {
                    match chars.next() {
                        Some('"') => break,
                        Some('\\') => match chars.next() {
                            Some(c @ ('"' | '\\' | '$' | '`')) => word.push(c),
                            Some('\n') => {}
                            Some(c) => {
                                word.push('\\');
                                word.push(c);
                            }
                            None => return Err(unclosed('"')),
                        },
                        Some(c @ ('$' | '`')) => return Err(no_shell(c)),
                        Some(c) => word.push(c),
                        None => return Err(unclosed('"')),
                    }
                }
            }
            '\\' => match chars.next() {
                Some('\n') => {}
                Some(c) => {
                    open = true;
                    word.push(c);
                }
                None => return Err(Error::new(Reason::InvalidArgument, "the command ends in \\")),
            },
            '#' if !open => break,
            '~' if !open => return Err(no_shell(c)),
            '|' | '&' | ';' | '<' | '>' | '(' | ')' | '$' | '`' | '*' | '?' | '[' => {
                return Err(no_shell(c));
            }
            c => {
                open = true;
                word.push(c);
            }
        }
    }
    if open {
        words.push(word);
    }
    Ok(words)
}

fn unclosed(quote: char) -> Error {
    Error::new(Reason::InvalidArgument, format!("the command has a {quote} that is never closed"))
}

fn no_shell(c: char) -> Error {
    Error::new(
        Reason::InvalidArgument,
        format!("wasm cells run one program with no shell, so {c} has to be quoted"),
    )
}

#[cfg(test)]
mod tests {
    use super::split;

    fn words(line: &str) -> Vec<String> {
        split(line).unwrap()
    }

    #[test]
    fn words_split_on_blanks_and_quotes_keep_them() {
        assert_eq!(words("  python  -c 'print(6 * 7)' "), ["python", "-c", "print(6 * 7)"]);
        assert_eq!(
            words(r#"echo "a \"b\" \$c \n" d\ e '' "#),
            ["echo", r#"a "b" $c \n"#, "d e", ""]
        );
        assert_eq!(words("a'b'\"c\"d"), ["abcd"]);
        assert_eq!(words("run # the rest is a comment"), ["run"]);
        assert_eq!(words("a#b"), ["a#b"]);
        assert!(words("").is_empty());
    }

    #[test]
    fn what_only_a_shell_could_do_is_refused() {
        for line in [
            "a | b",
            "a > f",
            "a; b",
            "a && b",
            "echo $HOME",
            "echo \"$HOME\"",
            "ls *.py",
            "cat ~/f",
            "echo `id`",
            "'open",
            "\"open",
            "end\\",
        ] {
            assert!(split(line).is_err(), "{line}");
        }
        assert_eq!(words("'a | b' \"*\" \\$"), ["a | b", "*", "$"]);
    }
}
