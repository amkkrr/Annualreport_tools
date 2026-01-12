# Implementation Plan: Bulk Queue Management in Monitoring Dashboard

The goal is to provide a way to manage large queues (e.g., 23k+ pending downloads) efficiently by adding global "Clear All" and "Reset All" operations.

## Proposed Changes

### 1. Database Layer Enhancements (`annual_report_mda/sqlite_db.py`)
Add efficient global update methods that use set-based SQL statements:
- `clear_all_pending_downloads(conn)`: Sets `download_status = 'skipped'` where it is currently `pending`.
- `clear_all_pending_converts(conn)`: Sets `convert_status = 'skipped'` where it is currently `pending`.
- `reset_all_failed(conn, phase: str)`: Resets `failed` to `pending` for a specific phase (download/convert/extract).

### 2. WebUI Component Layer (`webui/components/db_utils.py`)
- Expose the new database methods with proper connection management using `sqlite_db.connection_context()`.

### 3. Monitoring Dashboard UI (`webui/pages/1_监控仪表盘.py`)
Add a "Danger Zone (Global Operations)" section at the bottom:
- Use `st.popover` for each bulk action to provide a safe two-step confirmation process.
- Buttons for:
    - **Clear Download Queue**: Set all pending downloads to skipped.
    - **Clear Convert Queue**: Set all pending converts to skipped.
    - **Reset Failed Records**: Batch reset failed records for a selected phase.

## Critical Files
- `annual_report_mda/sqlite_db.py`
- `webui/components/db_utils.py`
- `webui/pages/1_监控仪表盘.py`

## Verification Plan

### Manual Verification
1. Open the Monitoring Dashboard.
2. Observe the "Pending Downloads" count (currently ~23k).
3. Go to the new "Danger Zone" section.
4. Open the "Clear Download Queue" popover and click confirm.
5. Verify that the "Pending Downloads" count drops to 0 and the records are marked as `skipped` in the database.
6. Verify "Reset Failed" works for a specific phase (e.g., download) if failed records exist.
