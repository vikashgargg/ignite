#!/usr/bin/env bash
# Install the Vajra disk-pressure watchdog as a macOS launchd agent that runs every 10 minutes
# (threshold-driven prune, see scripts/prune_maintenance.sh). launchd is the prod-grade macOS
# scheduler: it survives reboots/logout, runs on a real interval, and (unlike the old 3am
# crontab) reacts to disk pressure many times a day.
#
# Idempotent: re-running reinstalls cleanly. Also removes the superseded crontab entry.
set -euo pipefail

ROOT="${VAJRA_ROOT:-/Users/vikashgarg/Desktop/ignite}"
LABEL="com.vajra.prune"
PLIST="$HOME/Library/LaunchAgents/${LABEL}.plist"
INTERVAL="${VAJRA_PRUNE_INTERVAL_SEC:-600}"   # every 10 minutes

# macOS TCC blocks launchd background agents from EXECUTING a script located under a protected
# folder (~/Desktop, ~/Documents, ~/Downloads). The repo lives under ~/Desktop, so we install a
# copy of the watchdog to a non-protected location (~/.vajra) and point launchd at that. The
# watchdog still `cd`s into $VAJRA_ROOT to prune the real target.
AGENT_DIR="$HOME/.vajra"
AGENT_SCRIPT="$AGENT_DIR/prune_maintenance.sh"
mkdir -p "$AGENT_DIR" "$HOME/Library/LaunchAgents"
cp "$ROOT/scripts/prune_maintenance.sh" "$AGENT_SCRIPT"
chmod +x "$AGENT_SCRIPT"

cat > "$PLIST" <<PLIST
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key><string>${LABEL}</string>
    <key>ProgramArguments</key>
    <array>
        <string>/bin/bash</string>
        <string>${AGENT_SCRIPT}</string>
    </array>
    <key>EnvironmentVariables</key>
    <dict><key>VAJRA_ROOT</key><string>${ROOT}</string></dict>
    <key>StartInterval</key><integer>${INTERVAL}</integer>
    <key>RunAtLoad</key><true/>
    <key>StandardOutPath</key><string>/tmp/vajra_prune.launchd.out</string>
    <key>StandardErrorPath</key><string>/tmp/vajra_prune.launchd.err</string>
    <key>LowPriorityIO</key><true/>
    <key>Nice</key><integer>10</integer>
</dict>
</plist>
PLIST

# Reload cleanly (ignore "not loaded" on first install).
launchctl unload "$PLIST" 2>/dev/null || true
launchctl load  "$PLIST"
echo "loaded launchd agent ${LABEL} (every ${INTERVAL}s) -> $PLIST"

# Retire the superseded once-a-day crontab entry, if present.
if crontab -l 2>/dev/null | grep -q "prune_maintenance.sh"; then
  crontab -l 2>/dev/null | grep -v "prune_maintenance.sh" | crontab - || true
  echo "removed stale 3am crontab entry for prune_maintenance.sh"
fi

echo "watchdog installed. Log: /tmp/vajra_prune.log"
