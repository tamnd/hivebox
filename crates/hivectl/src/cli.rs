//! The commands, and the small argument parser they share. Flags come before positional
//! arguments or after them, and `--` ends the flags, so `hivectl run ID -- ls -la` passes `-la`
//! to `ls`.

use hive_proto::v1;
use hive_sdk::{Cell, Client, Command, Output, Selector};
use hive_types::{Backend, CellSpec, Resources, Source};
use std::collections::BTreeMap;
use std::io::{IsTerminal, Write};
use std::time::{Duration, SystemTime};
use tokio::io::AsyncReadExt;

/// What `hivectl help` prints.
pub const USAGE: &str = "\
usage: hivectl [--socket PATH | --endpoint URL] [--project NAME] COMMAND

Cells:
  create IMAGE [-n COUNT] [-l KEY=VALUE]... [--mem MIB] [--cpu MILLICORES]
         [--net PROFILE] [--ttl DURATION] [--idle DURATION] [--key KEY]
  ls [-l KEY=VALUE]... [--state STATE]...
  get ID
  pause|resume|stop ID... | -l KEY=VALUE...
  watch [ID | -l KEY=VALUE...]

Commands:
  run ID [-e KEY=VALUE]... [--cwd DIR] [--timeout DURATION] [--user UID[:GID]] [-i] -- ARGV...
  sh ID SCRIPT...

Files:
  cat ID PATH
  cp SRC DST          one of them is ID:PATH, the other a local file or - for stdin or stdout
  files ID PATH [--depth N]
  rm ID PATH [-r]

The socket is $HIVE_SOCKET, or /run/hivebox/comb.sock. The project is $HIVE_PROJECT, or local.
Durations are seconds, or a number with s, m or h. `run` exits with the command's exit code, or 124 when it timed out.
";

/// A command line split into flags and the rest.
#[derive(Debug, Default)]
pub struct Args {
    flags: Vec<(String, Option<String>)>,
    pub(crate) rest: Vec<String>,
}

/// Flags that stand alone. Every other flag takes a value.
const SWITCHES: &[&str] = &["-i", "-r", "--all", "-h", "--help"];

impl Args {
    /// Splits `args`.
    ///
    /// # Errors
    ///
    /// A flag that needs a value is last.
    pub fn parse(args: impl IntoIterator<Item = String>) -> Result<Self, String> {
        let mut out = Self::default();
        let mut it = args.into_iter();
        while let Some(a) = it.next() {
            if a == "--" {
                out.rest.extend(it);
                break;
            }
            if a.len() > 1 && a.starts_with('-') {
                if let Some((k, v)) = a.split_once('=').filter(|_| a.starts_with("--")) {
                    out.flags.push((k.to_string(), Some(v.to_string())));
                } else if SWITCHES.contains(&a.as_str()) {
                    out.flags.push((a, None));
                } else {
                    let v = it.next().ok_or_else(|| format!("{a} needs a value"))?;
                    out.flags.push((a, Some(v)));
                }
            } else {
                out.rest.push(a);
            }
        }
        Ok(out)
    }

    fn all(&self, names: &[&str]) -> Vec<&str> {
        let wanted = |k: &String| names.contains(&k.as_str());
        self.flags.iter().filter(|(k, _)| wanted(k)).filter_map(|(_, v)| v.as_deref()).collect()
    }

    /// The last value of a flag, which wins over earlier ones.
    fn one(&self, names: &[&str]) -> Option<&str> {
        self.all(names).pop()
    }

    fn has(&self, name: &str) -> bool {
        self.flags.iter().any(|(k, _)| k == name)
    }

    fn take(&mut self, names: &[&str]) -> Option<String> {
        let at = self.flags.iter().rposition(|(k, _)| names.contains(&k.as_str()))?;
        let v = self.flags[at].1.clone();
        self.flags.retain(|(k, _)| !names.contains(&k.as_str()));
        v
    }

    fn labels(&self) -> Result<BTreeMap<String, String>, String> {
        pairs(&self.all(&["-l", "--label"]))
    }

    fn check(&self, known: &[&str]) -> Result<(), String> {
        match self.flags.iter().find(|(k, _)| !known.contains(&k.as_str())) {
            Some((k, _)) => Err(format!("{k} is not a flag of this command")),
            None => Ok(()),
        }
    }
}

/// Runs a whole command line and returns the exit code.
pub async fn main(args: Vec<String>) -> Result<i32, String> {
    let mut args = Args::parse(args)?;
    let socket = args.take(&["--socket", "-s"]);
    let endpoint = args.take(&["--endpoint"]);
    let project = args.take(&["--project", "-p"]).or_else(|| std::env::var("HIVE_PROJECT").ok());
    if args.rest.is_empty() || args.has("-h") || args.has("--help") {
        print!("{USAGE}");
        return Ok(if args.rest.is_empty() { 2 } else { 0 });
    }
    let command = args.rest.remove(0);
    match command.as_str() {
        "help" => {
            print!("{USAGE}");
            return Ok(0);
        }
        "version" => {
            println!("hivectl {}", env!("CARGO_PKG_VERSION"));
            return Ok(0);
        }
        _ => {}
    }
    let endpoint = match (endpoint, socket) {
        (Some(e), _) => e,
        (None, Some(s)) => format!("unix:{s}"),
        (None, None) => std::env::var("HIVE_ENDPOINT").unwrap_or_else(|_| {
            let s = std::env::var("HIVE_SOCKET")
                .unwrap_or_else(|_| hive_sdk::DEFAULT_SOCKET.to_string());
            format!("unix:{s}")
        }),
    };
    let mut client = Client::connect(&endpoint).await.map_err(|e| e.to_string())?;
    if let Some(p) = project {
        client = client.project(&p).map_err(|e| e.to_string())?;
    }
    match command.as_str() {
        "create" => create(&client, &args).await,
        "ls" => ls(&client, &args).await,
        "get" => get(&client, &args).await,
        "pause" | "resume" | "stop" => bulk(&client, &command, &args).await,
        "watch" => watch(&client, &args).await,
        "run" => run(&client, &args).await,
        "sh" => sh(&client, &args).await,
        "cat" => cat(&client, &args).await,
        "cp" => cp(&client, &args).await,
        "files" => files(&client, &args).await,
        "rm" => rm(&client, &args).await,
        _ => Err(format!("{command} is not a command. Try hivectl help.")),
    }
}

fn err(e: hive_sdk::Error) -> String {
    e.to_string()
}

/// The positional arguments, exactly `n` of them.
fn exactly<'a>(args: &'a Args, n: usize, what: &str) -> Result<&'a [String], String> {
    if args.rest.len() == n { Ok(&args.rest) } else { Err(format!("this needs {what}")) }
}

async fn create(client: &Client, args: &Args) -> Result<i32, String> {
    args.check(&["-n", "-l", "--label", "--mem", "--cpu", "--net", "--ttl", "--idle", "--key"])?;
    let [image] = exactly(args, 1, "an image")? else { unreachable!() };
    let mut spec = CellSpec::new(Source::Image(image.clone()), Backend::Container);
    let number = |flag: &str| -> Result<Option<u32>, String> {
        args.one(&[flag])
            .map(|v| v.parse().map_err(|_| format!("{flag} {v} is not a number")))
            .transpose()
    };
    spec.resources = Resources {
        mem_mib: number("--mem")?.unwrap_or(Resources::DEFAULT.mem_mib),
        vcpu_milli: number("--cpu")?.unwrap_or(Resources::DEFAULT.vcpu_milli),
        ..Resources::DEFAULT
    };
    if let Some(n) = args.one(&["--net"]) {
        spec.network_profile = n.to_string();
    }
    spec.hard_ttl = args.one(&["--ttl"]).map(duration).transpose()?;
    spec.idle_ttl = args.one(&["--idle"]).map(duration).transpose()?;
    spec.labels = args.labels()?;
    let count = number("-n")?.unwrap_or(1);
    let made = client.create_many(&spec, count, args.one(&["--key"])).await.map_err(err)?;
    let mut failed = 0;
    for m in made {
        match m {
            Ok(c) => println!("{}", c.id()),
            Err(e) => {
                failed += 1;
                eprintln!("hivectl: {e}");
            }
        }
    }
    Ok(i32::from(failed > 0))
}

async fn ls(client: &Client, args: &Args) -> Result<i32, String> {
    args.check(&["-l", "--label", "--state"])?;
    exactly(args, 0, "no arguments")?;
    let states = args
        .all(&["--state"])
        .into_iter()
        .map(|s| {
            v1::CellState::from_str_name(&format!("CELL_STATE_{}", s.to_uppercase()))
                .ok_or_else(|| format!("{s} is not a state"))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let cells = client.list(&args.labels()?, &states).await.map_err(err)?;
    let rows: Vec<[String; 5]> = cells
        .iter()
        .map(|c| {
            let i = c.info();
            [
                i.id.clone(),
                state_name(c.state()),
                i.created_at.map_or_else(String::new, |t| age(t.seconds)),
                image(i),
                i.labels.iter().map(|(k, v)| format!("{k}={v}")).collect::<Vec<_>>().join(","),
            ]
        })
        .collect();
    table(&["ID", "STATE", "AGE", "IMAGE", "LABELS"], &rows);
    Ok(0)
}

async fn get(client: &Client, args: &Args) -> Result<i32, String> {
    args.check(&[])?;
    let [id] = exactly(args, 1, "a cell id")? else { unreachable!() };
    let c = client.get(id).await.map_err(err)?;
    let i = c.info();
    let spec = i.spec.clone().unwrap_or_default();
    let r = spec.resources.unwrap_or_default();
    println!("id:       {}", i.id);
    println!("project:  {}", i.project);
    println!("state:    {}", state_name(c.state()));
    if i.cause() != v1::Cause::Unspecified {
        println!("cause:    {}", enum_name(i.cause().as_str_name(), "CAUSE_"));
    }
    println!("image:    {}", image(i));
    println!("backend:  {}", enum_name(i.backend().as_str_name(), "BACKEND_"));
    println!("node:     {}", i.node);
    println!("cpu:      {} millicores", r.vcpu_milli);
    println!("memory:   {} MiB", r.mem_mib);
    println!("network:  {}", spec.network_profile);
    if let Some(t) = i.created_at {
        println!("age:      {}", age(t.seconds));
    }
    for (k, v) in &i.labels {
        println!("label:    {k}={v}");
    }
    Ok(0)
}

fn selectors(args: &Args) -> Result<Vec<Selector>, String> {
    let labels = args.labels()?;
    match (args.rest.is_empty(), labels.is_empty()) {
        (false, true) => Ok(args.rest.iter().map(|id| Selector::Id(id.clone())).collect()),
        (true, false) => Ok(vec![Selector::Labels(labels)]),
        _ => Err("name cells by id or by -l KEY=VALUE, not both".into()),
    }
}

async fn bulk(client: &Client, what: &str, args: &Args) -> Result<i32, String> {
    args.check(&["-l", "--label"])?;
    let mut failed = 0;
    for sel in selectors(args)? {
        let r = match what {
            "pause" => client.pause(&sel).await,
            "resume" => client.resume(&sel).await,
            _ => client.stop(&sel).await,
        };
        let r = match r {
            Ok(r) => r,
            Err(e) => {
                failed += 1;
                eprintln!("hivectl: {e}");
                continue;
            }
        };
        for f in &r.failures {
            failed += 1;
            let e = f.error.as_ref();
            eprintln!(
                "hivectl: {}: {} {}",
                f.cell_id,
                e.map_or("", |e| e.reason.as_str()),
                e.map_or("", |e| e.message.as_str())
            );
        }
        if matches!(sel, Selector::Labels(_)) {
            println!("{} matched, {} {what}d", r.matched, r.succeeded);
        }
    }
    Ok(i32::from(failed > 0))
}

async fn watch(client: &Client, args: &Args) -> Result<i32, String> {
    args.check(&["-l", "--label"])?;
    let sel = match (args.rest.as_slice(), args.labels()?) {
        ([id], l) if l.is_empty() => Selector::Id(id.clone()),
        ([], l) => Selector::Labels(l),
        _ => return Err("watch one cell by id, or cells by -l KEY=VALUE".into()),
    };
    let mut events = client.watch(&sel).await.map_err(err)?;
    while let Some(e) = futures::StreamExt::next(&mut events).await {
        let e = e.map_err(err)?;
        let from = e.from();
        let Some(c) = e.cell else { continue };
        let to = c.state();
        if from == v1::CellState::Unspecified {
            println!("{} {}", c.id, state_name(to));
        } else {
            println!("{} {} -> {}", c.id, state_name(from), state_name(to));
        }
    }
    Ok(0)
}

async fn run(client: &Client, args: &Args) -> Result<i32, String> {
    args.check(&["-e", "--env", "--cwd", "--timeout", "--user", "-i"])?;
    let Some((id, argv)) = args.rest.split_first().filter(|(_, a)| !a.is_empty()) else {
        return Err("run needs a cell id and a command".into());
    };
    let cmd = command(args, Command::new(argv.iter().cloned()))?;
    stream(client, id, cmd, args.has("-i")).await
}

async fn sh(client: &Client, args: &Args) -> Result<i32, String> {
    args.check(&["-e", "--env", "--cwd", "--timeout", "--user", "-i"])?;
    let Some((id, script)) = args.rest.split_first().filter(|(_, s)| !s.is_empty()) else {
        return Err("sh needs a cell id and a script".into());
    };
    let cmd = command(args, Command::shell(script.join(" ")))?;
    stream(client, id, cmd, args.has("-i")).await
}

fn command(args: &Args, mut cmd: Command) -> Result<Command, String> {
    for (k, v) in pairs(&args.all(&["-e", "--env"]))? {
        cmd = cmd.env(k, v);
    }
    if let Some(d) = args.one(&["--cwd"]) {
        cmd = cmd.cwd(d);
    }
    if let Some(t) = args.one(&["--timeout"]) {
        cmd = cmd.timeout(duration(t)?);
    }
    if let Some(u) = args.one(&["--user"]) {
        cmd = cmd.user(u);
    }
    Ok(cmd)
}

/// Runs `cmd` with its output copied to ours as it comes, and with `stdin` ours copied to it.
async fn stream(client: &Client, id: &str, cmd: Command, stdin: bool) -> Result<i32, String> {
    let cell = client.get(id).await.map_err(err)?;
    let mut p = cell.start(cmd).await.map_err(err)?;
    let (tx, mut rx) = tokio::sync::mpsc::channel::<Vec<u8>>(4);
    if stdin {
        tokio::spawn(async move {
            let mut input = tokio::io::stdin();
            let mut buf = vec![0u8; 64 << 10];
            while let Ok(n @ 1..) = input.read(&mut buf).await {
                if tx.send(buf[..n].to_vec()).await.is_err() {
                    break;
                }
            }
        });
    } else {
        drop(tx);
    }
    let mut open = true;
    loop {
        tokio::select! {
            data = rx.recv(), if open => match data {
                Some(d) => p.write(d).await.map_err(err)?,
                None => {
                    open = false;
                    p.close_stdin();
                }
            },
            out = p.next() => match out.map_err(err)? {
                Some(Output::Stdout(b)) => write_all(&mut std::io::stdout(), &b)?,
                Some(Output::Stderr(b)) => write_all(&mut std::io::stderr(), &b)?,
                Some(Output::Exit(r)) => {
                    if r.timed_out {
                        // What coreutils' timeout exits with, so scripts can tell it apart.
                        eprintln!("hivectl: the command timed out");
                        return Ok(124);
                    }
                    return Ok(if r.signal > 0 { 128 + r.signal } else { r.exit_code });
                }
                None => return Err("the command's output ended before it exited".into()),
            },
        }
    }
}

fn write_all(w: &mut impl Write, b: &[u8]) -> Result<(), String> {
    w.write_all(b).and_then(|()| w.flush()).map_err(|e| e.to_string())
}

async fn cat(client: &Client, args: &Args) -> Result<i32, String> {
    args.check(&[])?;
    let [id, path] = exactly(args, 2, "a cell id and a path")? else { unreachable!() };
    let cell = client.get(id).await.map_err(err)?;
    download(&cell, path, "-").await?;
    Ok(0)
}

/// `ID:PATH` as the cell and the path, or `None` for a local path.
fn remote(s: &str) -> Option<(&str, &str)> {
    s.split_once(':').filter(|(id, path)| !id.is_empty() && !id.contains('/') && !path.is_empty())
}

async fn cp(client: &Client, args: &Args) -> Result<i32, String> {
    args.check(&[])?;
    let [src, dst] = exactly(args, 2, "a source and a destination")? else { unreachable!() };
    match (remote(src), remote(dst)) {
        (Some((id, path)), None) => {
            let cell = client.get(id).await.map_err(err)?;
            download(&cell, path, dst).await?;
        }
        (None, Some((id, path))) => {
            let cell = client.get(id).await.map_err(err)?;
            let data = if src == "-" {
                let mut b = Vec::new();
                tokio::io::stdin().read_to_end(&mut b).await.map_err(|e| e.to_string())?;
                b
            } else {
                tokio::fs::read(src).await.map_err(|e| format!("{src}: {e}"))?
            };
            cell.write(path, data).await.map_err(err)?;
        }
        _ => return Err("one side of cp is ID:PATH and the other is local".into()),
    }
    Ok(0)
}

/// Copies a file out of a cell to `to`, or to stdout for `-`, a chunk at a time.
async fn download(cell: &Cell, path: &str, to: &str) -> Result<(), String> {
    use futures::StreamExt;
    use tokio::io::AsyncWriteExt;
    let mut chunks = cell.open(path, 0, 0).await.map_err(err)?;
    let mut out: Box<dyn tokio::io::AsyncWrite + Unpin + Send> = if to == "-" {
        Box::new(tokio::io::stdout())
    } else {
        Box::new(tokio::fs::File::create(to).await.map_err(|e| format!("{to}: {e}"))?)
    };
    while let Some(c) = chunks.next().await {
        out.write_all(&c.map_err(err)?).await.map_err(|e| e.to_string())?;
    }
    out.flush().await.map_err(|e| e.to_string())
}

async fn files(client: &Client, args: &Args) -> Result<i32, String> {
    args.check(&["--depth"])?;
    let [id, path] = exactly(args, 2, "a cell id and a path")? else { unreachable!() };
    let depth = match args.one(&["--depth"]) {
        Some(d) => d.parse().map_err(|_| format!("--depth {d} is not a number"))?,
        None => 1,
    };
    let cell = client.get(id).await.map_err(err)?;
    let r = cell.list(path, depth).await.map_err(err)?;
    let rows: Vec<[String; 4]> = r
        .entries
        .iter()
        .map(|e| {
            let kind = match e.r#type() {
                v1::FileType::Dir => "d",
                v1::FileType::Symlink => "l",
                v1::FileType::File => "-",
                _ => "?",
            };
            let name = if e.symlink_target.is_empty() {
                e.path.clone()
            } else {
                format!("{} -> {}", e.path, e.symlink_target)
            };
            [format!("{kind}{}", perms(e.mode)), e.size.to_string(), mtime(e), name]
        })
        .collect();
    table(&["MODE", "SIZE", "MODIFIED", "PATH"], &rows);
    if r.truncated {
        eprintln!("hivectl: the listing stopped at the cell's limit");
    }
    Ok(0)
}

async fn rm(client: &Client, args: &Args) -> Result<i32, String> {
    args.check(&["-r"])?;
    let [id, path] = exactly(args, 2, "a cell id and a path")? else { unreachable!() };
    let cell = client.get(id).await.map_err(err)?;
    cell.remove(path, args.has("-r")).await.map_err(err)?;
    Ok(0)
}

fn pairs(items: &[&str]) -> Result<BTreeMap<String, String>, String> {
    items
        .iter()
        .map(|p| match p.split_once('=') {
            Some((k, v)) if !k.is_empty() => Ok((k.to_string(), v.to_string())),
            _ => Err(format!("{p} is not KEY=VALUE")),
        })
        .collect()
}

/// Seconds, or a number with `s`, `m` or `h`.
pub(crate) fn duration(s: &str) -> Result<Duration, String> {
    let bad = || format!("{s} is not a duration");
    let (n, unit) = match s.char_indices().last() {
        Some((i, 's')) => (&s[..i], 1),
        Some((i, 'm')) => (&s[..i], 60),
        Some((i, 'h')) => (&s[..i], 3600),
        _ => (s, 1),
    };
    let n: f64 = n.parse().map_err(|_| bad())?;
    if !n.is_finite() || n < 0.0 {
        return Err(bad());
    }
    Duration::try_from_secs_f64(n * f64::from(unit)).map_err(|_| bad())
}

fn state_name(s: v1::CellState) -> String {
    enum_name(s.as_str_name(), "CELL_STATE_")
}

fn enum_name(name: &str, prefix: &str) -> String {
    name.strip_prefix(prefix).unwrap_or(name).to_lowercase()
}

fn image(c: &v1::Cell) -> String {
    match c.spec.as_ref().and_then(|s| s.source.as_ref()) {
        Some(v1::cell_spec::Source::Image(i)) => i.r#ref.clone(),
        Some(v1::cell_spec::Source::Template(t)) => format!("template:{t}"),
        Some(v1::cell_spec::Source::Snapshot(s)) => format!("snapshot:{}", s.id),
        None => String::new(),
    }
}

/// How long ago `seconds` since the epoch was, in the largest unit that fits.
fn age(seconds: i64) -> String {
    let now = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).map_or(0, |d| d.as_secs());
    let s = now.saturating_sub(u64::try_from(seconds).unwrap_or(0));
    match s {
        0..60 => format!("{s}s"),
        60..3600 => format!("{}m", s / 60),
        3600..86_400 => format!("{}h", s / 3600),
        _ => format!("{}d", s / 86_400),
    }
}

/// `mode` as `ls -l` shows it, like `rwxr-xr-x`.
fn perms(mode: u32) -> String {
    let mut out = String::with_capacity(9);
    for (i, c) in "rwxrwxrwx".chars().enumerate() {
        out.push(if mode & (0o400 >> i) == 0 { '-' } else { c });
    }
    out
}

fn mtime(e: &v1::FileInfo) -> String {
    e.modified_at.map_or_else(String::new, |t| format!("{} ago", age(t.seconds)))
}

/// Prints rows under a header, with each column as wide as its widest cell.
fn table<const N: usize>(header: &[&str; N], rows: &[[String; N]]) {
    let mut width: [usize; N] = header.map(str::len);
    for r in rows {
        for (w, c) in width.iter_mut().zip(r) {
            *w = (*w).max(c.len());
        }
    }
    let mut out = std::io::stdout().lock();
    let line = |out: &mut std::io::StdoutLock<'_>, cells: &[&str]| {
        let mut s = String::new();
        for (i, (c, w)) in cells.iter().zip(width).enumerate() {
            if i + 1 == N {
                s.push_str(c);
            } else {
                s.push_str(&format!("{c:<w$}  "));
            }
        }
        let _ = writeln!(out, "{}", s.trim_end());
    };
    if std::io::stdout().is_terminal() || !rows.is_empty() {
        line(&mut out, header);
    }
    for r in rows {
        line(&mut out, &r.each_ref().map(String::as_str));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(s: &str) -> Args {
        Args::parse(s.split_whitespace().map(String::from)).unwrap()
    }

    #[test]
    fn flags_go_anywhere_until_the_double_dash() {
        let a = args("run -e A=1 abc --timeout 5s -i -- ls -la --cwd x");
        assert_eq!(a.rest, ["run", "abc", "ls", "-la", "--cwd", "x"]);
        assert_eq!(a.one(&["--timeout"]), Some("5s"));
        assert!(a.has("-i"));
        assert_eq!(a.one(&["--cwd"]), None);
        assert_eq!(a.labels().unwrap().len(), 0);
        let a = args("ls -l a=1 --label=b=2");
        assert_eq!(
            a.labels().unwrap().into_iter().collect::<Vec<_>>(),
            [("a".to_string(), "1".to_string()), ("b".to_string(), "2".to_string())]
        );
        assert!(Args::parse(["ls".to_string(), "-l".to_string()]).is_err());
        assert!(args("ls --nope 1").check(&["-l"]).is_err());
        assert!(pairs(&["=1"]).is_err());
    }

    #[test]
    fn durations_have_units() {
        assert_eq!(duration("30").unwrap(), Duration::from_secs(30));
        assert_eq!(duration("1.5s").unwrap(), Duration::from_millis(1500));
        assert_eq!(duration("10m").unwrap(), Duration::from_secs(600));
        assert_eq!(duration("2h").unwrap(), Duration::from_secs(7200));
        assert!(duration("-1").is_err());
        assert!(duration("soon").is_err());
    }

    #[test]
    fn modes_read_like_ls() {
        assert_eq!(perms(0o755), "rwxr-xr-x");
        assert_eq!(perms(0o100_640), "rw-r-----");
    }

    #[test]
    fn remote_paths_name_a_cell() {
        assert_eq!(remote("c1:/tmp/x"), Some(("c1", "/tmp/x")));
        assert_eq!(remote("./a:b"), None);
        assert_eq!(remote("/tmp/x"), None);
        assert_eq!(remote(":x"), None);
    }
}
