use anyhow::{Context, Result, bail};
use clap::Parser;
use crossterm::{
    cursor,
    event::{Event, EventStream, KeyCode, KeyEvent, KeyEventKind, KeyModifiers},
    execute,
    terminal::{self, EnterAlternateScreen, LeaveAlternateScreen},
};
use futures_util::StreamExt;
use pulse::{
    config::{Args, ColorMode, Settings, normalize_server},
    engine, history,
    model::{Phase, ResultRecord, Snapshot},
    provider::{self, Catalog, Endpoint, Provider},
    ui::{self, Overlay, View},
};
use ratatui::{Terminal, backend::CrosstermBackend};
use std::{
    io::{self, IsTerminal, Stdout},
    path::PathBuf,
    time::Duration,
};
use tokio::{sync::watch, task::JoinHandle};
use tokio_util::sync::CancellationToken;

struct TerminalGuard {
    terminal: Terminal<CrosstermBackend<Stdout>>,
}
impl TerminalGuard {
    fn enter() -> Result<Self> {
        terminal::enable_raw_mode()?;
        let result = (|| {
            let mut stdout = io::stdout();
            execute!(stdout, EnterAlternateScreen, cursor::Hide)?;
            Ok::<_, anyhow::Error>(Self {
                terminal: Terminal::new(CrosstermBackend::new(stdout))?,
            })
        })();
        if result.is_err() {
            restore_terminal();
        }
        result
    }
}
impl Drop for TerminalGuard {
    fn drop(&mut self) {
        restore_terminal();
    }
}
fn restore_terminal() {
    let _ = terminal::disable_raw_mode();
    let _ = execute!(io::stdout(), LeaveAlternateScreen, cursor::Show);
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    match args.color {
        ColorMode::Always => crossterm::style::force_color_output(true),
        ColorMode::Never => crossterm::style::force_color_output(false),
        ColorMode::Auto => {}
    }
    let settings = Settings::from_args(&args)?;
    if args.list_servers {
        let catalog = provider::discover(&settings).await?;
        if args.json {
            println!("{}", serde_json::to_string_pretty(&catalog.servers)?);
        } else {
            for server in catalog.servers {
                println!(
                    "{}\t{}\t{}",
                    server
                        .id
                        .map(|id| id.to_string())
                        .unwrap_or_else(|| "-".into()),
                    server.name,
                    server.base_url
                );
            }
            if !catalog.notice.is_empty() {
                eprintln!("{}", catalog.notice);
            }
        }
        return Ok(());
    }
    let history_dir = args
        .history_dir
        .clone()
        .unwrap_or_else(history::default_dir);
    setup_logging(&history_dir);
    if args.json {
        let (tx, _rx) = watch::channel(Snapshot::default());
        let cancel = CancellationToken::new();
        let result = tokio::select! {
            result = engine::run(settings, tx, cancel.clone()) => result,
            _ = tokio::signal::ctrl_c() => { cancel.cancel(); bail!("Test cancelled"); }
        }?;
        if !result.simulated
            && let Err(error) = history::save(&history_dir, &result)
        {
            eprintln!("History: {error:#}");
        }
        if let Some(output) = &args.output {
            history::write_json(output, &result)?;
        }
        println!("{}", serde_json::to_string_pretty(&result)?);
        return Ok(());
    }
    if !io::stdout().is_terminal() || !io::stdin().is_terminal() {
        bail!("The dashboard needs an interactive terminal. Use --json for a headless test.");
    }
    let original_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        restore_terminal();
        original_hook(info);
    }));
    dashboard(args, settings, history_dir).await
}

fn setup_logging(history_dir: &std::path::Path) {
    let dir = history_dir.parent().unwrap_or(history_dir);
    if std::fs::create_dir_all(dir).is_ok()
        && let Ok(file) = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(dir.join("pulse.log"))
    {
        let _ = tracing_subscriber::fmt()
            .with_ansi(false)
            .with_env_filter(
                tracing_subscriber::EnvFilter::try_from_default_env()
                    .unwrap_or_else(|_| "warn".into()),
            )
            .with_writer(std::sync::Mutex::new(file))
            .try_init();
    }
}

struct App {
    args: Args,
    settings: Settings,
    history_dir: PathBuf,
    selected_server: Option<usize>,
    snapshot: Snapshot,
    overlay: Overlay,
    notice: String,
    tx: watch::Sender<Snapshot>,
    rx: watch::Receiver<Snapshot>,
    job: Option<JoinHandle<Result<ResultRecord>>>,
    catalog_job: Option<JoinHandle<Result<Catalog>>>,
    pending_start: bool,
    cancel: CancellationToken,
}

impl App {
    fn start(&mut self) {
        if self.job.is_some() {
            return;
        }
        if self.catalog_job.is_some() {
            self.pending_start = true;
            self.notice = "Loading public servers · the test will start when ready".into();
            return;
        }
        self.notice.clear();
        let mut settings = self.settings.clone();
        if let Some(i) = self.selected_server {
            settings.servers = vec![settings.servers[i].clone()];
        }
        self.cancel = CancellationToken::new();
        self.snapshot = Snapshot {
            phase: Phase::Selecting,
            simulated: settings.demo,
            ..Default::default()
        };
        self.tx.send_replace(self.snapshot.clone());
        self.job = Some(tokio::spawn(engine::run(
            settings,
            self.tx.clone(),
            self.cancel.clone(),
        )));
    }
    fn export(&mut self) {
        let record = match &self.overlay {
            Overlay::History { selected, records } => records.get(*selected).cloned(),
            _ => self.snapshot.result.clone(),
        };
        let Some(record) = record else {
            self.notice = "Complete a test before exporting".into();
            return;
        };
        let path = self.args.output.clone().unwrap_or_else(|| {
            self.history_dir
                .parent()
                .unwrap_or(&self.history_dir)
                .join("exports")
                .join(format!(
                    "pulse-{}-{}.json",
                    record.timestamp_unix_ms, record.id
                ))
        });
        self.notice = match history::write_json(&path, &record) {
            Ok(()) => format!("Exported {}", path.display()),
            Err(e) => format!("Export failed: {e:#}"),
        };
    }
    fn key(&mut self, mut key: KeyEvent) -> bool {
        if key.kind != KeyEventKind::Press {
            return false;
        }
        if matches!(key.code, KeyCode::Char('c' | 'C'))
            && key.modifiers.contains(KeyModifiers::CONTROL)
        {
            return true;
        }
        if !matches!(self.overlay, Overlay::Servers { input: Some(_), .. })
            && let KeyCode::Char(c) = key.code
        {
            key.code = KeyCode::Char(c.to_ascii_lowercase());
        }
        match &mut self.overlay {
            Overlay::Servers {
                selected,
                input: Some(input),
                error,
            } => {
                match key.code {
                    KeyCode::Esc => {
                        self.overlay = Overlay::Servers {
                            selected: *selected,
                            input: None,
                            error: String::new(),
                        };
                    }
                    KeyCode::Backspace => {
                        input.pop();
                    }
                    KeyCode::Char(c) if !c.is_control() && input.len() < 2048 => input.push(c),
                    KeyCode::Enter => {
                        if self.settings.servers.len() >= 256 {
                            *error = "At most 256 servers can be configured".into();
                        } else {
                            match normalize_server(input) {
                                Ok(url) => {
                                    if let Some(index) =
                                        self.settings.servers.iter().position(|s| s.base_url == url)
                                    {
                                        *selected = index + 1;
                                    } else {
                                        self.settings.servers.push(
                                            Endpoint::from_base(self.settings.provider, &url)
                                                .expect("URL was already validated"),
                                        );
                                        *selected = self.settings.servers.len();
                                    }
                                    self.overlay = Overlay::Servers {
                                        selected: *selected,
                                        input: None,
                                        error: String::new(),
                                    };
                                }
                                Err(e) => *error = e.to_string(),
                            }
                        }
                    }
                    _ => {}
                }
                return false;
            }
            Overlay::Servers { selected, .. } => {
                match key.code {
                    KeyCode::Esc | KeyCode::Char('s') => self.overlay = Overlay::None,
                    KeyCode::Up | KeyCode::Char('k') => *selected = selected.saturating_sub(1),
                    KeyCode::Down | KeyCode::Char('j') => {
                        *selected = (*selected + 1).min(self.settings.servers.len())
                    }
                    KeyCode::Char('a') => {
                        self.overlay = Overlay::Servers {
                            selected: *selected,
                            input: Some(String::new()),
                            error: String::new(),
                        }
                    }
                    KeyCode::Enter => {
                        self.selected_server = selected.checked_sub(1);
                        self.overlay = Overlay::None;
                        self.snapshot = Snapshot {
                            simulated: self.settings.demo,
                            ..Default::default()
                        };
                        self.notice = "Server selected · Enter to start a test".into();
                    }
                    KeyCode::Char('q') => return true,
                    _ => {}
                }
                return false;
            }
            Overlay::History { selected, records } => {
                match key.code {
                    KeyCode::Esc | KeyCode::Char('h') => self.overlay = Overlay::None,
                    KeyCode::Up | KeyCode::Char('k') => *selected = selected.saturating_sub(1),
                    KeyCode::Down | KeyCode::Char('j') => {
                        *selected = (*selected + 1).min(records.len().saturating_sub(1))
                    }
                    KeyCode::Char('e') => self.export(),
                    KeyCode::Char('q') => return true,
                    _ => {}
                }
                return false;
            }
            Overlay::Help => {
                match key.code {
                    KeyCode::Esc | KeyCode::Char('?') => self.overlay = Overlay::None,
                    KeyCode::Char('q') => return true,
                    _ => {}
                }
                return false;
            }
            Overlay::None => {}
        }
        match key.code {
            KeyCode::Enter => self.start(),
            KeyCode::Esc => {
                self.pending_start = false;
                if self.job.is_some() {
                    self.cancel.cancel();
                    self.notice = "Stopping transfer tasks…".into();
                }
            }
            KeyCode::Char('q') => return true,
            KeyCode::Char('s') => {
                if self.catalog_job.is_some() {
                    self.notice = "Loading public servers…".into();
                } else if self.job.is_some() {
                    self.notice = "Cancel or finish this test before changing servers".into();
                } else {
                    self.overlay = Overlay::Servers {
                        selected: self.selected_server.map(|i| i + 1).unwrap_or(0),
                        input: None,
                        error: String::new(),
                    };
                }
            }
            KeyCode::Char('h') => match history::load(&self.history_dir) {
                Ok(records) => {
                    self.overlay = Overlay::History {
                        selected: 0,
                        records,
                    }
                }
                Err(e) => self.notice = format!("History: {e:#}"),
            },
            KeyCode::Char('e') => self.export(),
            KeyCode::Char('?') => self.overlay = Overlay::Help,
            _ => {}
        }
        false
    }
    async fn finish_catalog(&mut self) {
        if self
            .catalog_job
            .as_ref()
            .is_none_or(|job| !job.is_finished())
        {
            return;
        }
        let job = self.catalog_job.take().unwrap();
        match job.await {
            Ok(Ok(catalog)) => {
                self.settings.servers = catalog.servers;
                self.notice = catalog.notice;
            }
            Ok(Err(error)) => {
                self.pending_start = false;
                self.notice = format!("Public server list: {error:#}");
            }
            Err(error) => {
                self.pending_start = false;
                self.notice = format!("Server discovery: {error}");
            }
        }
        if self.pending_start {
            self.pending_start = false;
            self.start();
        }
    }
    async fn finish_job(&mut self) {
        if self.job.as_ref().is_none_or(|job| !job.is_finished()) {
            return;
        }
        let job = self.job.take().unwrap();
        self.snapshot = self.rx.borrow_and_update().clone();
        match job.await {
            Ok(Ok(record)) => {
                let mut messages = Vec::new();
                if !record.simulated {
                    match history::save(&self.history_dir, &record) {
                        Ok(_) => messages.push("Saved to history".into()),
                        Err(e) => messages.push(format!("History failed: {e:#}")),
                    }
                }
                if let Some(path) = &self.args.output {
                    match history::write_json(path, &record) {
                        Ok(_) => messages.push(format!("Exported {}", path.display())),
                        Err(e) => messages.push(format!("Export failed: {e:#}")),
                    }
                }
                self.notice = messages.join(" · ");
            }
            Ok(Err(error)) => {
                self.snapshot.phase = if self.cancel.is_cancelled() {
                    Phase::Cancelled
                } else {
                    Phase::Failed
                };
                self.snapshot.warming_up = false;
                self.snapshot.message = if self.cancel.is_cancelled() {
                    "Test cancelled · partial metrics were not saved".into()
                } else {
                    format!("{error:#}")
                };
                tracing::warn!(error = %error, "Measurement ended without a result");
                self.notice = self.snapshot.message.clone();
            }
            Err(error) => {
                self.snapshot.phase = Phase::Failed;
                self.snapshot.message = format!("Measurement task: {error}");
                self.notice = self.snapshot.message.clone();
            }
        }
    }
}

async fn dashboard(args: Args, settings: Settings, history_dir: PathBuf) -> Result<()> {
    let (tx, rx) = watch::channel(Snapshot::default());
    let catalog_job = if settings.provider == Provider::Librespeed
        && settings.servers.is_empty()
        && !settings.demo
    {
        let config = settings.clone();
        Some(tokio::spawn(
            async move { provider::discover(&config).await },
        ))
    } else {
        None
    };
    let mut app = App {
        snapshot: Snapshot {
            simulated: settings.demo,
            ..Default::default()
        },
        args,
        settings,
        history_dir,
        selected_server: None,
        overlay: Overlay::None,
        notice: String::new(),
        tx,
        rx,
        job: None,
        catalog_job,
        pending_start: false,
        cancel: CancellationToken::new(),
    };
    let mut terminal = TerminalGuard::enter()?;
    let mut events = EventStream::new();
    let mut tick = tokio::time::interval(Duration::from_millis(33));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let result = async {
        loop {
            tokio::select! {
                event = events.next() => match event {
                    Some(Ok(Event::Key(key))) if app.key(key) => break,
                    Some(Err(error)) => return Err(anyhow::Error::from(error)),
                    None => break,
                    _ => {},
                },
                _ = tick.tick() => {},
            }
            if app.rx.has_changed().unwrap_or(false) {
                app.snapshot = app.rx.borrow_and_update().clone();
            }
            app.finish_job().await;
            app.finish_catalog().await;
            terminal
                .terminal
                .draw(|frame| {
                    ui::render(
                        frame,
                        &View {
                            snapshot: &app.snapshot,
                            settings: &app.settings,
                            overlay: &app.overlay,
                            ascii: app.args.ascii,
                            notice: &app.notice,
                            selected_server: app.selected_server,
                        },
                    )
                })
                .context("Rendering dashboard")?;
        }
        Ok(())
    }
    .await;
    app.cancel.cancel();
    if let Some(job) = app.catalog_job.take() {
        job.abort();
    }
    if let Some(job) = app.job.take() {
        let _ = tokio::time::timeout(Duration::from_secs(3), job).await;
    }
    result
}
