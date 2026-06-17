# headphoneMotion (AirPod) session scan

_Generated 2026-06-16 20:27:31_

## Result (iOS only)

**16 of 273 iOS sessions (5.86%) contained headphoneMotion data.**

| Metric | Count |
| --- | ---: |
| iOS sessions with headphoneMotion | 16 |
| iOS sessions without headphoneMotion | 257 |
| **Total iOS sessions** | **273** |

## All sessions examined

| Category | Count |
| --- | ---: |
| iOS (with headphoneMotion) | 16 |
| iOS (without headphoneMotion) | 257 |
| Non-iOS (android / no iOS chunks) | 24 |
| No data (no main session) | 0 |
| **Total classified** | **297** |

## How this was produced

- Source: MongoDB collection `tracking_sessions_v3` (field `uuid`).
- Per session: read `main_session.json` for the capture id, list iOS chunks, then sample **12** chunks at intervals across the session timeline (seed `0`) and check each for the raw `headphone_internal_sensor` stream, stopping at the first hit.
- Matching sessions (with `deliveryType`) are listed in [`headphoneMotion_uuid_list.csv`](./headphoneMotion_uuid_list.csv); every examined UUID + status is in `headphoneMotion_checked.log`.
- Sampling (not full scan) means a session that used AirPods only briefly, outside the sampled chunks, could be missed; raise `--samples` to reduce this.
