use crate::{
    config::Settings,
    model::{Phase, ResultRecord, Snapshot},
};
use ratatui::{
    Frame,
    layout::{Constraint, Layout, Rect},
    style::{Color, Modifier, Style},
    symbols::Marker,
    text::{Line, Span},
    widgets::{
        Axis, Block, BorderType, Borders, Cell, Chart, Clear, Dataset, Gauge, GraphType, Paragraph,
        Row, Table, Wrap,
    },
};

const BG: Color = Color::Rgb(15, 19, 27);
const PANEL: Color = Color::Rgb(20, 26, 36);
const TEXT: Color = Color::Rgb(226, 232, 240);
const MUTED: Color = Color::Rgb(139, 153, 175);
const BORDER: Color = Color::Rgb(49, 63, 82);
const CYAN: Color = Color::Rgb(75, 213, 233);
const VIOLET: Color = Color::Rgb(177, 150, 255);
const AMBER: Color = Color::Rgb(245, 190, 88);
const GREEN: Color = Color::Rgb(116, 220, 165);

#[derive(Default)]
pub enum Overlay {
    #[default]
    None,
    Servers {
        selected: usize,
        input: Option<String>,
        error: String,
    },
    History {
        selected: usize,
        records: Vec<ResultRecord>,
    },
    Help,
}

pub struct View<'a> {
    pub snapshot: &'a Snapshot,
    pub settings: &'a Settings,
    pub overlay: &'a Overlay,
    pub ascii: bool,
    pub notice: &'a str,
    pub selected_server: Option<usize>,
}

fn panel(title: impl Into<Line<'static>>, ascii: bool) -> Block<'static> {
    let block = Block::default()
        .borders(Borders::ALL)
        .border_type(if ascii {
            BorderType::Plain
        } else {
            BorderType::Rounded
        })
        .border_style(Style::default().fg(BORDER))
        .style(Style::default().bg(PANEL))
        .title(title.into().style(Style::default().fg(MUTED)));
    if ascii {
        block.border_set(ratatui::symbols::border::Set {
            top_left: "+",
            top_right: "+",
            bottom_left: "+",
            bottom_right: "+",
            horizontal_top: "-",
            horizontal_bottom: "-",
            vertical_left: "|",
            vertical_right: "|",
        })
    } else {
        block
    }
}

fn value_ms(value: Option<f64>) -> String {
    value
        .map(|v| format!("{v:.1} ms"))
        .unwrap_or_else(|| "-- ms".into())
}

pub fn render(frame: &mut Frame, view: &View<'_>) {
    let area = frame.area();
    frame.render_widget(
        Block::default().style(Style::default().bg(BG).fg(TEXT)),
        area,
    );
    if area.width < 48 || area.height < if area.width < 80 { 24 } else { 20 } {
        let s = view.snapshot;
        let text = format!(
            "PULSE{}\n{}\n\nLatency {}  Jitter {}\nDownload {:.2} Mbps / {:.2} MB/s\nUpload   {:.2} Mbps / {:.2} MB/s\n\nEnlarge terminal for live charts\nEnter start · Esc cancel · Q quit",
            if s.simulated { " · DEMO" } else { "" },
            s.phase.label(),
            value_ms(s.idle.median_ms),
            value_ms(s.idle.jitter_ms),
            s.download_mbps,
            s.download_mbps / 8.0,
            s.upload_mbps,
            s.upload_mbps / 8.0
        );
        frame.render_widget(Paragraph::new(text).wrap(Wrap { trim: true }), area);
        return;
    }
    let narrow = area.width < 80;
    let metrics_height = if narrow { 10 } else { 5 };
    let layout = Layout::vertical([
        Constraint::Length(4),
        Constraint::Length(metrics_height),
        Constraint::Min(0),
        Constraint::Length(6),
        Constraint::Length(1),
    ])
    .margin(1)
    .split(area);
    header(frame, layout[0], view);
    metrics(frame, layout[1], view, narrow);
    graphs(frame, layout[2], view);
    status(frame, layout[3], view);
    let footer = Line::from(vec![
        Span::styled(" Enter ", Style::default().fg(CYAN)),
        Span::raw("Start/Retest "),
        Span::styled("Esc", Style::default().fg(CYAN)),
        Span::raw(" Cancel  "),
        Span::styled("S", Style::default().fg(CYAN)),
        Span::raw(" Servers  "),
        Span::styled("H", Style::default().fg(CYAN)),
        Span::raw(" History  "),
        Span::styled("E", Style::default().fg(CYAN)),
        Span::raw(" Export  "),
        Span::styled("?", Style::default().fg(CYAN)),
        Span::raw(" Help  "),
        Span::styled("Q", Style::default().fg(CYAN)),
        Span::raw(" Quit"),
    ]);
    frame.render_widget(
        Paragraph::new(footer).style(Style::default().fg(MUTED)),
        layout[4],
    );
    match view.overlay {
        Overlay::None => {}
        Overlay::Servers {
            selected,
            input,
            error,
        } => server_overlay(frame, view, *selected, input.as_deref(), error),
        Overlay::History { selected, records } => history_overlay(frame, view, *selected, records),
        Overlay::Help => help_overlay(frame, view),
    }
}

fn header(frame: &mut Frame, area: Rect, view: &View<'_>) {
    let s = view.snapshot;
    let title = Line::from(vec![
        Span::styled(
            " P U L S E ",
            Style::default().fg(CYAN).add_modifier(Modifier::BOLD),
        ),
        Span::styled(" NETWORK SPEED & QUALITY ", Style::default().fg(MUTED)),
    ]);
    let server = if s.simulated && s.server_name.is_empty() {
        "Demo · simulated network"
    } else if s.server_name.is_empty() {
        view.selected_server
            .map(|i| view.settings.servers[i].as_str())
            .unwrap_or("Automatic server selection")
    } else {
        &s.server_name
    };
    let transport = if s.simulated {
        "SIMULATED"
    } else if s.server_url.starts_with("https://") {
        "HTTPS"
    } else {
        "HTTP"
    };
    let line = Line::from(vec![
        Span::styled(format!(" {server}"), Style::default().fg(TEXT)),
        Span::styled(
            format!(
                "  ·  {transport}  ·  {}  ·  {} conn.",
                if s.simulated {
                    "No network traffic".into()
                } else {
                    view.settings.route()
                },
                view.settings.connections
            ),
            Style::default().fg(MUTED),
        ),
    ]);
    let label = if s.simulated {
        " DEMO · NO NETWORK TRAFFIC ".to_owned()
    } else if s.server_url.is_empty() {
        " Enter to begin · Local server: pulse-server".into()
    } else {
        format!(" {}", s.server_url)
    };
    frame.render_widget(
        Paragraph::new(vec![
            line,
            Line::styled(
                label,
                Style::default().fg(if s.simulated { AMBER } else { MUTED }),
            ),
        ])
        .block(panel(title, view.ascii)),
        area,
    );
}

fn card(
    frame: &mut Frame,
    area: Rect,
    title: &str,
    value: String,
    secondary: [String; 2],
    color: Color,
    ascii: bool,
) {
    let [subtitle, extra] = secondary;
    let lines = vec![
        Line::styled(
            value,
            Style::default().fg(color).add_modifier(Modifier::BOLD),
        ),
        Line::styled(subtitle, Style::default().fg(MUTED)),
        Line::styled(extra, Style::default().fg(MUTED)),
    ];
    frame.render_widget(
        Paragraph::new(lines)
            .centered()
            .block(panel(format!(" {title} "), ascii)),
        area,
    );
}

fn metrics(frame: &mut Frame, area: Rect, view: &View<'_>, narrow: bool) {
    let s = view.snapshot;
    let areas: Vec<Rect> = if narrow {
        Layout::vertical([Constraint::Percentage(50); 2])
            .split(area)
            .iter()
            .flat_map(|row| {
                Layout::horizontal([Constraint::Percentage(50); 2])
                    .split(*row)
                    .to_vec()
            })
            .collect()
    } else {
        Layout::horizontal([Constraint::Percentage(25); 4])
            .split(area)
            .to_vec()
    };
    let method = if s.server_url.starts_with("https://") {
        "Idle HTTPS RTT"
    } else {
        "Idle HTTP RTT"
    };
    card(
        frame,
        areas[0],
        "LATENCY",
        value_ms(s.idle.median_ms),
        [method.into(), format!("P95 {}", value_ms(s.idle.p95_ms))],
        AMBER,
        view.ascii,
    );
    card(
        frame,
        areas[1],
        "JITTER",
        value_ms(s.idle.jitter_ms),
        [
            "Idle variation".into(),
            format!("{} ok · {} failed", s.idle.successful, s.idle.failed),
        ],
        AMBER,
        view.ascii,
    );
    let download_label = if s.phase == Phase::Download {
        if s.warming_up {
            "Warming up"
        } else {
            "Live · smoothed"
        }
    } else if s.download_points.is_empty() {
        "Waiting"
    } else {
        "Measured average"
    };
    let upload_label = if s.phase == Phase::Upload {
        if s.warming_up {
            "Warming up"
        } else {
            "Live · confirmed"
        }
    } else if s.upload_points.is_empty() {
        "Waiting"
    } else {
        "Measured average"
    };
    card(
        frame,
        areas[2],
        "DOWNLOAD",
        format!("{:.2} Mbps", s.download_mbps),
        [
            format!("{:.2} MB/s", s.download_mbps / 8.0),
            download_label.into(),
        ],
        CYAN,
        view.ascii,
    );
    card(
        frame,
        areas[3],
        "UPLOAD",
        format!("{:.2} Mbps", s.upload_mbps),
        [
            format!("{:.2} MB/s", s.upload_mbps / 8.0),
            upload_label.into(),
        ],
        VIOLET,
        view.ascii,
    );
}

type ChartSeries<'a> = (&'a str, &'a [(f64, f64)], Color);

fn plot(
    frame: &mut Frame,
    area: Rect,
    title: String,
    series: &[ChartSeries<'_>],
    unit: &str,
    bounds: [f64; 2],
    ascii: bool,
) {
    let [x_min, x_max] = bounds;
    if area.height < 4 || area.width < 16 {
        return;
    }
    let max = series
        .iter()
        .flat_map(|(_, p, _)| p.iter().map(|p| p.1))
        .fold(1.0_f64, f64::max)
        * 1.15;
    let datasets = series
        .iter()
        .map(|(name, points, color)| {
            Dataset::default()
                .name(*name)
                .marker(if ascii { Marker::Dot } else { Marker::Braille })
                .graph_type(GraphType::Line)
                .style(Style::default().fg(*color))
                .data(points)
        })
        .collect::<Vec<_>>();
    let axis_style = Style::default().fg(BORDER);
    let labels = Style::default().fg(MUTED);
    let chart = Chart::new(datasets)
        .block(panel(title, ascii))
        .x_axis(
            Axis::default()
                .style(axis_style)
                .bounds([x_min, x_max.max(x_min + 1.0)])
                .labels([
                    Span::styled(format!("{x_min:.0}s"), labels),
                    Span::styled(format!("{x_max:.0}s"), labels),
                ]),
        )
        .y_axis(
            Axis::default()
                .style(axis_style)
                .title(Span::styled(unit.to_string(), labels))
                .bounds([0.0, max])
                .labels([
                    Span::styled("0", labels),
                    Span::styled(format!("{max:.0}"), labels),
                ]),
        );
    frame.render_widget(chart, area);
}

fn graphs(frame: &mut Frame, area: Rect, view: &View<'_>) {
    let s = view.snapshot;
    let duration = view.settings.duration.as_secs_f64();
    let last = s.latency_points.last().map(|p| p.0).unwrap_or(1.0);
    let latency_title = format!(
        " LATENCY · {} · loaded jitter {} ",
        value_ms(s.current_latency),
        value_ms(s.loaded.jitter_ms)
    );
    if area.height >= 18 {
        let rows = Layout::vertical([Constraint::Ratio(1, 3); 3]).split(area);
        plot(
            frame,
            rows[0],
            " DOWNLOAD · received payload ".into(),
            &[("Download", &s.download_points, CYAN)],
            "Mbps",
            [0.0, duration],
            view.ascii,
        );
        plot(
            frame,
            rows[1],
            " UPLOAD · server-confirmed payload ".into(),
            &[("Upload", &s.upload_points, VIOLET)],
            "Mbps",
            [0.0, duration],
            view.ascii,
        );
        plot(
            frame,
            rows[2],
            latency_title,
            &[("RTT", &s.latency_points, AMBER)],
            "ms",
            [0.0, last.max(10.0)],
            view.ascii,
        );
    } else if area.height >= 6 && area.width >= 90 {
        let cols = Layout::horizontal([Constraint::Percentage(62), Constraint::Percentage(38)])
            .split(area);
        plot(
            frame,
            cols[0],
            " THROUGHPUT · per-phase elapsed time ".into(),
            &[
                ("Down", &s.download_points, CYAN),
                ("Up", &s.upload_points, VIOLET),
            ],
            "Mbps",
            [0.0, duration],
            view.ascii,
        );
        plot(
            frame,
            cols[1],
            latency_title,
            &[("RTT", &s.latency_points, AMBER)],
            "ms",
            [0.0, last.max(10.0)],
            view.ascii,
        );
    } else {
        plot(
            frame,
            area,
            " THROUGHPUT · per-phase elapsed time ".into(),
            &[
                ("Down", &s.download_points, CYAN),
                ("Up", &s.upload_points, VIOLET),
            ],
            "Mbps",
            [0.0, duration],
            view.ascii,
        );
    }
}

fn status(frame: &mut Frame, area: Rect, view: &View<'_>) {
    let s = view.snapshot;
    let inner = panel(
        format!(
            " {}{} ",
            s.phase.label().to_uppercase(),
            if s.warming_up { " · WARM-UP" } else { "" }
        ),
        view.ascii,
    );
    let inside = inner.inner(area);
    frame.render_widget(inner, area);
    let rows = Layout::vertical([Constraint::Length(1); 4]).split(inside);
    if s.phase.running() {
        let ratio = if s.duration > 0.0 {
            (s.elapsed / s.duration).clamp(0.0, 1.0)
        } else {
            0.0
        };
        frame.render_widget(
            Gauge::default()
                .gauge_style(
                    Style::default()
                        .fg(if s.phase == Phase::Upload {
                            VIOLET
                        } else {
                            CYAN
                        })
                        .bg(BORDER),
                )
                .ratio(ratio)
                .label(format!(
                    "{}  {:.1} / {:.1} s",
                    s.phase.label(),
                    s.elapsed,
                    s.duration
                )),
            rows[0],
        );
    } else {
        let color = match s.phase {
            Phase::Failed => AMBER,
            Phase::Complete => GREEN,
            _ => MUTED,
        };
        frame.render_widget(
            Paragraph::new(if s.phase == Phase::Ready {
                "Ready · Enter to measure this connection".into()
            } else {
                s.message.clone()
            })
            .style(Style::default().fg(color)),
            rows[0],
        );
    }
    let details = if let Some(result) = &s.result {
        format!(
            "Idle {} · Down loaded {} · Up loaded {}",
            value_ms(result.idle_latency.median_ms),
            value_ms(result.download.loaded_latency.median_ms),
            value_ms(result.upload.loaded_latency.median_ms)
        )
    } else {
        format!(
            "Loaded RTT {} · P95 {} · {} probe failures",
            value_ms(s.loaded.median_ms),
            value_ms(s.loaded.p95_ms),
            s.loaded.failed
        )
    };
    frame.render_widget(
        Paragraph::new(format!(
            "Transferred  Down {:.2} MB   Up {:.2} MB",
            s.download_bytes as f64 / 1e6,
            s.upload_bytes as f64 / 1e6
        ))
        .style(Style::default().fg(MUTED)),
        rows[1],
    );
    frame.render_widget(
        Paragraph::new(details).style(Style::default().fg(MUTED)),
        rows[2],
    );
    let notice = if !view.notice.is_empty() {
        view.notice
    } else if s.phase.running() {
        &s.message
    } else {
        "Decimal units · 1 MB = 1,000,000 bytes · measurement excludes warm-up"
    };
    frame.render_widget(
        Paragraph::new(notice).style(Style::default().fg(MUTED)),
        rows[3],
    );
}

fn modal(frame: &mut Frame, title: &str, ascii: bool) -> Rect {
    let full = frame.area();
    let width = full.width.saturating_sub(6).min(100);
    let height = full.height.saturating_sub(4).min(25);
    let area = Rect::new(
        (full.width - width) / 2,
        (full.height - height) / 2,
        width,
        height,
    );
    frame.render_widget(Clear, area);
    let block = panel(format!(" {title} "), ascii);
    let inside = block.inner(area);
    frame.render_widget(block, area);
    inside
}

fn server_overlay(
    frame: &mut Frame,
    view: &View<'_>,
    selected: usize,
    input: Option<&str>,
    error: &str,
) {
    let area = modal(frame, "SERVERS", view.ascii);
    let rows = Layout::vertical([Constraint::Min(3), Constraint::Length(4)]).split(area);
    let mut lines = vec![
        Line::styled(
            "Choose the endpoint for your next test",
            Style::default().fg(MUTED),
        ),
        Line::raw(""),
    ];
    let start = selected.saturating_sub(rows[0].height.saturating_sub(4) as usize);
    let choices = std::iter::once("Automatic · lowest median RTT".to_owned())
        .chain(view.settings.servers.iter().cloned())
        .collect::<Vec<_>>();
    for (i, server) in choices
        .iter()
        .enumerate()
        .skip(start)
        .take(rows[0].height.saturating_sub(2) as usize)
    {
        let style = if i == selected {
            Style::default().fg(BG).bg(CYAN)
        } else {
            Style::default().fg(TEXT)
        };
        lines.push(Line::styled(
            format!(" {} {server}", if i == selected { ">" } else { " " }),
            style,
        ));
    }
    frame.render_widget(Paragraph::new(lines), rows[0]);
    let footer = if let Some(input) = input {
        format!("Server URL: {input}_\nEnter add · Esc discard\n{error}")
    } else {
        format!(
            "Up/Down select · Enter use · A add URL · Esc close\nAdded servers last for this session.\n{error}"
        )
    };
    frame.render_widget(
        Paragraph::new(footer)
            .style(Style::default().fg(MUTED))
            .wrap(Wrap { trim: false }),
        rows[1],
    );
}

fn history_overlay(frame: &mut Frame, view: &View<'_>, selected: usize, records: &[ResultRecord]) {
    let area = modal(frame, "HISTORY · last 100 completed tests", view.ascii);
    let parts = Layout::vertical([Constraint::Min(3), Constraint::Length(5)]).split(area);
    if records.is_empty() {
        frame.render_widget(
            Paragraph::new("No completed tests yet. Demo runs are not stored.")
                .style(Style::default().fg(MUTED)),
            parts[0],
        );
    } else {
        let visible = parts[0].height.saturating_sub(1).max(1) as usize;
        let start = selected.saturating_sub(visible - 1);
        let rows = records
            .iter()
            .enumerate()
            .skip(start)
            .take(visible)
            .map(|(i, r)| {
                let elapsed = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis()
                    .saturating_sub(r.timestamp_unix_ms as u128)
                    / 1000;
                Row::new(vec![
                    Cell::from(format!(
                        "{} {}m ago",
                        if i == selected { ">" } else { " " },
                        elapsed / 60
                    )),
                    Cell::from(r.server_name.clone()),
                    Cell::from(format!("{:.2}", r.download.average_mbps)),
                    Cell::from(format!("{:.2}", r.upload.average_mbps)),
                    Cell::from(value_ms(r.idle_latency.median_ms)),
                ])
                .style(if i == selected {
                    Style::default().bg(BORDER).fg(TEXT)
                } else {
                    Style::default().fg(MUTED)
                })
            });
        frame.render_widget(
            Table::new(
                rows,
                [
                    Constraint::Length(14),
                    Constraint::Min(16),
                    Constraint::Length(14),
                    Constraint::Length(14),
                    Constraint::Length(12),
                ],
            )
            .header(
                Row::new(["When", "Server", "Down Mbps", "Up Mbps", "Idle RTT"])
                    .style(Style::default().fg(CYAN)),
            ),
            parts[0],
        );
    }
    let text = records.get(selected).map(|r| format!("{}\nDown {:.2} MB/s · Up {:.2} MB/s · Idle jitter {}\nLoaded RTT: down {} · up {}\nUp/Down browse · E export selected · Esc close", r.server_url, r.download.average_mbps / 8.0, r.upload.average_mbps / 8.0, value_ms(r.idle_latency.jitter_ms), value_ms(r.download.loaded_latency.median_ms), value_ms(r.upload.loaded_latency.median_ms))).unwrap_or_else(|| "Esc close".into());
    frame.render_widget(
        Paragraph::new(text)
            .style(Style::default().fg(MUTED))
            .wrap(Wrap { trim: false }),
        parts[1],
    );
}

fn help_overlay(frame: &mut Frame, view: &View<'_>) {
    let area = modal(frame, "PULSE · HELP", view.ascii);
    let text = "Enter     Start or repeat a full test\nEsc       Cancel test / close dialog\nS         Choose or add a Pulse server\nH         Browse completed tests\nE         Export current or selected history result\nQ / Ctrl-C Quit and stop transfers\n\nMbps      Megabits per second\nMB/s      Megabytes per second (Mbps / 8)\nMB        Total payload transferred, including warm-up\nLatency   Median idle HTTP(S) request/response RTT\nJitter    Mean absolute change between adjacent RTT probes\nLoaded    Probes collected during each transfer phase\n\nDirect connections are the default. Use --proxy explicitly.\nRemote speed tests require a remote Pulse server.\nResults reflect the chosen server and route.\n\nEsc closes this dialog";
    frame.render_widget(
        Paragraph::new(text)
            .style(Style::default().fg(TEXT))
            .wrap(Wrap { trim: false }),
        area,
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;
    use ratatui::{Terminal, backend::TestBackend};

    #[test]
    fn dashboard_and_dialogs_render_at_supported_sizes() {
        let args = crate::config::Args::parse_from(["pulse", "--demo"]);
        let settings = Settings::from_args(&args).unwrap();
        let snapshot = Snapshot {
            phase: Phase::Upload,
            simulated: true,
            download_mbps: 245.6,
            upload_mbps: 82.4,
            ..Default::default()
        };
        for (width, height) in [(120, 42), (100, 30), (80, 24), (60, 24), (48, 18), (30, 12)] {
            for overlay in [
                Overlay::None,
                Overlay::Help,
                Overlay::Servers {
                    selected: 0,
                    input: None,
                    error: String::new(),
                },
                Overlay::History {
                    selected: 0,
                    records: Vec::new(),
                },
            ] {
                let mut terminal = Terminal::new(TestBackend::new(width, height)).unwrap();
                terminal
                    .draw(|f| {
                        render(
                            f,
                            &View {
                                snapshot: &snapshot,
                                settings: &settings,
                                overlay: &overlay,
                                ascii: false,
                                notice: "",
                                selected_server: None,
                            },
                        )
                    })
                    .unwrap();
                assert!(
                    terminal
                        .backend()
                        .buffer()
                        .content
                        .iter()
                        .any(|c| c.symbol() != " ")
                );
            }
        }
    }
}
