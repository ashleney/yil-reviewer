use std::collections::HashMap;

use anyhow::{bail, Context, Result};
use csv::Writer;
use riichi::convlog::tenhou::{EndStatus, Log};
use riichi::mjai::Event;
use riichi::must_tile;
use riichi::state::PlayerState;

macro_rules! csv_struct {
    ($(#[$meta:meta])* $vis:vis struct $name:ident {
        $( $(#[$field_meta:meta])* $field:ident : $ty:ty ),* $(,)?
    }) => {
        $(#[$meta])*
        #[derive(Debug, Default)]
        $vis struct $name {
            $( $(#[$field_meta])* pub $field: $ty, )*
        }

        impl $name {
            pub const CSV_HEADERS: &'static [&'static str] = &[$(stringify!($field)),*];

            pub fn to_csv_values(&self) -> Vec<String> {
                vec![$(self.$field.to_string()),*]
            }
        }
    };
}

csv_struct! {
    pub struct PlayerInfo {
        /// Count of StartKyoku events
        kyoku_count: u32,
        /// Count of Hora events from self
        agari_count: u32,
        /// Count of Hora events targeting self
        dealin_count: u32,
        /// Count of ReachAccepted events
        riichi_count: u32,
        /// Count of Hora events from self where self declared riichi
        riichi_agari_count: u32,
        /// Count of Hora events from self where menzen and not self declared riichi
        dama_agari_count: u32,
        /// Count of Hora events from self where not menzen
        open_agari_count: u32,
        /// Count of EndKyoku events where not menzen
        open_count: u32,
        /// Sum of self shanten at StartKyoku
        total_haipai_shanten: u32,
        /// Sum of score deltas for Hora events from self
        total_agari_score: u32,
        /// Sum of score deltas for Hora events targeting self
        total_dealin_score: u32,
        /// Sum of uradora in tehai for Hora events from self where self declared riichi
        ura_count: u32,
        /// Count of Hora events from self where agari is yakuman
        yakuman_count: u32,
        /// Count of Hora events from self where agari is sanbaiman or more
        sanbaiman_count: u32,
        /// Count of Hora events from self where agari is baiman or more
        baiman_count: u32,
        /// Count of EndKyoku events where self is tenpai for any yakuman, includes won yakuman
        yakuman_chance: u32,
        /// Count of Hora events targetting self where someone else wins with ippatsu and we were not in riichi
        ippatsu_dealin_count: u32,
        /// Count of Dahai events from self where agari has ippatsu chance and we discarded a non-genbutsu tile while we were not in riichi
        ippatsu_brazen_count: u32,
        /// Sum of how many tiles are being waited on when riichi is called
        total_riichi_wait: u32,
        /// Sum of how many tiles were being waited on when self agari
        total_agari_waits: u32,
        /// Count of Hora events targetting self where actor is closed and not riichi
        dama_dealin_count: u32,
        /// Count of Hora events targetting self where actor is closed and not riichi and point delta is mangan or more
        dama_mangan_dealin_count: u32,
        /// Count of actions taken but not necessarily recorded (if state.can_act() is true)
        action_count: u32,
        /// Total time spent in a game
        seconds_played: u32
    }
}

fn main() -> Result<()> {
    let log_directory = std::path::Path::new("./downloads");
    let info_output_file = std::path::Path::new("./info.csv");
    let yaku_output_file = std::path::Path::new("./yaku.csv");

    // single accumulator for every player across every log
    let mut players_info: HashMap<String, PlayerInfo> = HashMap::new();
    let mut yaku_info: HashMap<String, HashMap<String, u32>> = HashMap::new();

    for entry in std::fs::read_dir(log_directory).context("cannot read log directory")? {
        let entry = entry?;
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        println!("Processing: {path:?}");

        let json_string = std::fs::read_to_string(&path).with_context(|| format!("failed to read file {path:?}"))?;
        let log = Log::from_json_str(&json_string)?;

        let json_value: serde_json::Value = serde_json::from_str(&json_string)?;
        let duration = if let Some(mjshead) = json_value.get("mjshead") {
            let start_time = mjshead.get("start_time").context("no mjshead.start_time")?.as_u64().unwrap();
            let end_time = mjshead.get("end_time").context("no mjshead.end_time")?.as_u64().unwrap();
            Some(end_time - start_time)
        } else {
            None
        };

        let events = riichi::convlog::tenhou_to_mjai(&log)?;

        for tenhou_kyoku in log.kyokus.iter() {
            match &tenhou_kyoku.end_status {
                EndStatus::Hora { details } => {
                    for hora_detail in details {
                        let actor_name = log.names[hora_detail.who as usize].clone();
                        let collected_yaku = yaku_info.entry(actor_name).or_default();
                        for yaku in &hora_detail.yaku {
                            let Some((yaku_name, yaku_count)) = yaku.split_once('(') else {
                                bail!("invalid tenhou yaku name");
                            };
                            if yaku_name == "Ura Dora" && yaku_count.starts_with('0') {
                                continue;
                            }
                            *collected_yaku.entry(yaku_name.to_owned()).or_default() += 1;
                        }
                    }
                }
                EndStatus::Ryukyoku { .. } => {}
            }
        }

        for player_id in 0..4 {
            let name = log.names[player_id].clone();
            let info = players_info.entry(name.clone()).or_default();

            if let Some(duration) = duration {
                info.seconds_played += duration as u32;
            }

            let mut state = PlayerState::new(player_id as u8);

            for event in &events {
                let danger_before_event = if matches!(event, Event::Dahai { actor, .. } if *actor == player_id as u8) {
                    // this is very slow and needs to be optimized
                    state.calculate_danger().map(|d| d.tile_weights)
                } else {
                    [[0.; 34]; 4]
                };
                state.update(event)?;
                if duration.is_some() {
                    info.action_count += state.last_cans.can_act() as u32;
                }
                match event {
                    Event::StartKyoku { .. } => {
                        info.kyoku_count += 1;
                        info.total_haipai_shanten += state.shanten as u32;
                    }
                    Event::ReachAccepted { actor } if *actor == player_id as u8 => {
                        info.riichi_count += 1;
                        info.total_riichi_wait += state
                            .waits
                            .iter()
                            .enumerate()
                            .filter(|&(_, &is_wait)| is_wait)
                            .map(|(tile, _)| 4 - state.tiles_seen[tile] as u32)
                            .sum::<u32>();
                    }
                    Event::Dahai { actor, pai, .. } if *actor == player_id as u8 => {
                        for (player_kawa, player_danger) in state.kawa.iter().zip(danger_before_event).skip(1) {
                            let is_ippatsu = player_kawa
                                .last()
                                .is_some_and(|item| item.as_ref().is_some_and(|item| item.sutehai.is_riichi));
                            if is_ippatsu && !state.self_riichi_accepted() && player_danger[pai.deaka().as_usize()] > 0. {
                                info.ippatsu_brazen_count += 1;
                            }
                        }
                    }
                    Event::Hora {
                        actor,
                        target,
                        deltas,
                        ura_markers,
                    } => {
                        let Some(deltas) = deltas else { bail!("missing deltas") };

                        let mut normalized_self_delta =
                            deltas[player_id] - state.honba as i32 * 300 - state.kyotaku as i32 * 1000;
                        if state.is_oya() {
                            normalized_self_delta = normalized_self_delta * 2 / 3;
                        }

                        if *actor == player_id as u8 {
                            info.agari_count += 1;
                            info.total_agari_score += deltas[player_id] as u32;
                            if state.is_menzen {
                                if state.self_riichi_declared() {
                                    info.riichi_agari_count += 1;
                                } else {
                                    info.dama_agari_count += 1;
                                }
                            } else {
                                info.open_agari_count += 1;
                            }
                            if let Some(ura_markers) = ura_markers {
                                let ura_count = state
                                    .tehai
                                    .iter()
                                    .enumerate()
                                    .map(|(tile, count)| {
                                        if ura_markers.contains(&must_tile!(tile).next()) {
                                            *count
                                        } else {
                                            0
                                        }
                                    })
                                    .sum::<u8>();
                                info.ura_count += ura_count as u32;
                            }

                            if normalized_self_delta >= 32000 {
                                info.yakuman_count += 1;
                            }
                            if normalized_self_delta >= 24000 {
                                info.sanbaiman_count += 1;
                            }
                            if normalized_self_delta >= 16000 {
                                info.baiman_count += 1;
                            }
                            info.total_agari_waits += 1 + state
                                .waits
                                .iter()
                                .enumerate()
                                .filter(|&(_, &is_wait)| is_wait)
                                .map(|(tile, _)| 4 - state.tiles_seen[tile] as u32)
                                .sum::<u32>();
                        } else if *target == player_id as u8 {
                            info.dealin_count += 1;
                            info.total_dealin_score += (-deltas[player_id]) as u32;
                            let is_ippatsu = state.kawa[*actor as usize]
                                .last()
                                .is_some_and(|item| item.as_ref().is_some_and(|item| item.sutehai.is_riichi));
                            if is_ippatsu && !state.self_riichi_accepted() {
                                info.ippatsu_dealin_count += 1;
                            }
                            if !state.riichi_declared[state.rel(*actor)] && state.fuuro_overview[state.rel(*actor)].is_empty() {
                                info.dama_dealin_count += 1;
                                if normalized_self_delta <= -8000 {
                                    info.dama_mangan_dealin_count += 1;
                                }
                            }
                        }
                    }
                    Event::EndKyoku => {
                        if !state.is_menzen {
                            info.open_count += 1;
                        }
                        if state.real_time_shanten() == 0 {
                            let waits = state
                                .waits
                                .iter()
                                .enumerate()
                                .filter(|&(_, &is_wait)| is_wait)
                                .map(|(tile, _)| must_tile!(tile))
                                .collect::<Vec<_>>();
                            let has_yakuman_chance = waits.into_iter().any(|winning_tile| {
                                let Ok(Some(agari)) = state.calculate_agari(winning_tile, false, &[]) else {
                                    return false;
                                };
                                agari.agari.point(false).ron >= 32000
                            });
                            if has_yakuman_chance {
                                info.yakuman_chance += 1;
                            }
                        }
                    }
                    _ => {}
                }
            }
        }
    }

    // player info
    let mut csv_writer = Writer::from_path(info_output_file)?;

    let base_headers = PlayerInfo::CSV_HEADERS.iter().copied();
    let header: Vec<&str> = std::iter::once("name").chain(base_headers).collect();
    csv_writer.write_record(&header)?;

    let mut entries: Vec<(String, PlayerInfo)> = players_info
        .into_iter()
        .filter(|(name, info)| info.kyoku_count > 100 && !name.contains("ashlen"))
        .collect();
    entries.sort_by(|(_, l), (_, r)| r.kyoku_count.cmp(&l.kyoku_count));
    let name_order = entries.iter().map(|(name, _)| name.clone()).collect::<Vec<_>>();
    for (name, info) in entries {
        let row: Vec<String> = std::iter::once(name).chain(info.to_csv_values()).collect();
        csv_writer.write_record(&row)?;
    }
    csv_writer.flush()?;

    // yaku info
    let mut csv_writer = Writer::from_path(yaku_output_file)?;

    let mut total_yaku_counts: HashMap<String, u32> = HashMap::new();
    for inner_map in yaku_info.values() {
        for (key, value) in inner_map {
            *total_yaku_counts.entry(key.clone()).or_insert(0) += value;
        }
    }
    let mut yaku_order: Vec<(String, u32)> = total_yaku_counts.into_iter().collect();
    yaku_order.sort_by(|(_, l), (_, r)| r.cmp(l));

    let header = std::iter::once("name")
        .chain(yaku_order.iter().map(|(y, _)| y.as_str()))
        .collect::<Vec<_>>();
    csv_writer.write_record(header)?;

    let mut entries: Vec<(String, HashMap<String, u32>)> =
        yaku_info.into_iter().filter(|(name, _)| name_order.contains(name)).collect();
    entries.sort_by_key(|(name, _)| name_order.iter().position(|n| n == name));

    for (name, info) in entries {
        let entries = yaku_order
            .iter()
            .map(|(yaku, _)| info.get(yaku).cloned().unwrap_or(0).to_string());
        let row: Vec<String> = std::iter::once(name).chain(entries).collect();
        csv_writer.write_record(row)?;
    }
    csv_writer.flush()?;

    Ok(())
}
