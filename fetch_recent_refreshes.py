import csv
import json
import os
import statistics
from datetime import datetime, timezone
from pathlib import Path

import tableauserverclient as TSC
from dotenv import load_dotenv

# Load credentials from .env file
load_dotenv()

TABLEAU_SERVER_URL = os.getenv("TABLEAU_SERVER_URL")
SITE_ID = os.getenv("TABLEAU_SITE_ID")
PAT_NAME = os.getenv("TABLEAU_PAT_NAME")
PAT_VALUE = os.getenv("TABLEAU_PAT_VALUE")

# --- Target Configuration ---
# Set the name of the datasource you want to analyze
DATASOURCE_NAME = "Your Datasource Name"

# If the name isn't unique on your site, paste the datasource ID here instead.
# Leave as None to search by name. If multiple matches are found, the script
# will print all of them with their IDs so you can copy the right one.
DATASOURCE_ID = None

# How many recent successful refreshes to include in the analysis
REFRESH_SAMPLE_SIZE = 10

# Output files for execution history (extra challenge)
HISTORY_CSV = "refresh_history.csv"
HISTORY_JSON = "refresh_history.json"


# ---------------------------------------------------------------------------
# Step 1: Find the datasource
# ---------------------------------------------------------------------------

def find_datasource(server, name, datasource_id=None):
    """
    Finds a datasource on the site by ID (if provided) or by name.

    Using an ID is the most reliable approach — datasource names may not
    always be unique across projects on a given site. If you search
    by name and get multiple matches, this function will print all of them
    with their IDs so you can set DATASOURCE_ID and re-run.

    Returns a single DatasourceItem, or None if the lookup fails.
    """
    # If a specific ID was provided, go straight to it — no ambiguity possible
    if datasource_id:
        datasource = server.datasources.get_by_id(datasource_id)
        print(f"Found datasource by ID: '{datasource.name}' ({datasource.id})")
        return datasource

    # Search by name using a server-side filter so we're not paging
    # through every datasource on the site
    options = TSC.RequestOptions()
    options.filter.add(
        TSC.Filter("name", TSC.RequestOptions.Operator.Equals, name)
    )

    matching, _ = server.datasources.get(options)

    if len(matching) == 0:
        print(f"No datasource found with the name '{name}'.")
        print("Double-check the spelling or review your site to confirm the exact name.")
        return None

    if len(matching) > 1:
        # Multiple datasources share this name — we can't safely pick one.
        # Print all matches so the user can copy the right ID.
        print(f"Found {len(matching)} datasources named '{name}'.")
        print("Set DATASOURCE_ID in the script to one of the following:\n")
        for ds in matching:
            print(f"  Name:    {ds.name}")
            print(f"  ID:      {ds.id}")
            print(f"  Project: {ds.project_name}\n")
        return None

    datasource = matching[0]
    print(f"Found datasource: '{datasource.name}' (ID: {datasource.id})")
    return datasource


# ---------------------------------------------------------------------------
# Step 2: Retrieve recent successful refreshes
# ---------------------------------------------------------------------------

def get_recent_successful_refreshes(server, datasource, sample_size=10):
    """
    Retrieves the last N successful extract refreshes for the given datasource.

    This is where server-side filtering really earns its keep. Rather than
    loading all background jobs into memory and filtering them in Python,
    we push the filtering to Tableau using RequestOptions:

      - jobType = RefreshExtract  (only extract refresh jobs)
      - status  = Succeeded       (only jobs that completed successfully)
      - title   = datasource name (only jobs for this specific datasource)

    We also sort by completedAt descending so the most recent jobs come first,
    and set page_size to exactly what we need. The result: Tableau sends us
    only the jobs we care about — no wasted memory, no wasted network calls.

    Note: We call server.jobs.get() directly instead of using TSC.Pager.
    TSC.Pager is great when you need *all* results across many pages, but
    here we want exactly one page of exactly the right size. Using Pager
    would keep fetching pages until it ran out — the opposite of what we want.
    """
    options = TSC.RequestOptions(pagesize=sample_size)

    # Filter: only extract refresh jobs
    options.filter.add(
        TSC.Filter("jobType", TSC.RequestOptions.Operator.Equals, "RefreshExtract")
    )

    # Filter: only jobs that completed successfully
    # "Succeeded" is distinct from "Failed" or "Cancelled"
    options.filter.add(
        TSC.Filter("status", TSC.RequestOptions.Operator.Equals, "Succeeded")
    )

    # Filter: only jobs for this datasource
    # The "title" field on a background job matches the name of the object
    # that was refreshed — in this case, our datasource name
    options.filter.add(
        TSC.Filter("title", TSC.RequestOptions.Operator.Equals, datasource.name)
    )

    # Sort: most recent jobs first
    options.sort.add(
        TSC.Sort("completedAt", TSC.RequestOptions.Direction.Desc)
    )

    # Fetch one page — only the jobs we asked for
    jobs, pagination = server.jobs.get(options)

    print(f"Retrieved {len(jobs)} successful refresh job(s) "
          f"(requested {sample_size}, total available: {pagination.total_available}).")

    return jobs, pagination.total_available


# ---------------------------------------------------------------------------
# Step 3: Calculate statistics
# ---------------------------------------------------------------------------

def calculate_refresh_stats(jobs):
    """
    Calculates duration statistics across a list of completed refresh jobs.

    Duration is measured in seconds from job start to job completion.
    We use Python's built-in statistics module — no external packages needed.

    Returns a dict with:
      - durations      : list of individual durations in seconds
      - mean           : average duration in seconds
      - stdev          : standard deviation (0.0 if only one sample)
      - suggested_wait : mean + 2 standard deviations

    The suggested_wait is the mean + 2 standard deviations — a starting point
    for how long to wait before polling the server for a job's status.
    Waiting this long before your first check covers roughly 95% of typical
    refresh durations, reducing unnecessary API calls.
    """
    durations = []

    for job in jobs:
        if job.started_at and job.completed_at:
            duration = (job.completed_at - job.started_at).total_seconds()
            durations.append(duration)

    if not durations:
        return None

    mean_secs = statistics.mean(durations)

    # stdev requires at least 2 data points — default to 0 if we only have one
    stdev_secs = statistics.stdev(durations) if len(durations) > 1 else 0.0

    # Suggested initial wait before polling: mean + 2 standard deviations
    suggested_wait = mean_secs + (2 * stdev_secs)

    return {
        "durations": durations,
        "mean": mean_secs,
        "stdev": stdev_secs,
        "suggested_wait": suggested_wait,
        "sample_size": len(durations),
    }


# ---------------------------------------------------------------------------
# Step 4: Print a human-readable summary
# ---------------------------------------------------------------------------

def print_refresh_summary(datasource, jobs, stats):
    """
    Prints a formatted summary of the refresh history and analysis.
    """
    def format_duration(seconds):
        """Converts a raw second count into a readable mm:ss string."""
        minutes = int(seconds // 60)
        secs = int(seconds % 60)
        return f"{minutes}m {secs:02d}s"

    print(f"\n{'=' * 55}")
    print(f"  Refresh Analysis: {datasource.name}")
    print(f"{'=' * 55}")
    print(f"  Sample size : {stats['sample_size']} successful refreshes")
    print(f"  Mean        : {format_duration(stats['mean'])} ({stats['mean']:.1f}s)")
    print(f"  Std Dev     : {format_duration(stats['stdev'])} ({stats['stdev']:.1f}s)")
    print(f"  Suggest wait: {format_duration(stats['suggested_wait'])} "
          f"({stats['suggested_wait']:.1f}s)")
    print(f"{'=' * 55}")

    print("\n  Recent refresh durations (newest first):")
    for i, (job, dur) in enumerate(zip(jobs, stats["durations"]), start=1):
        completed = job.completed_at.strftime("%Y-%m-%d %H:%M") if job.completed_at else "unknown"
        print(f"  {i:>2}. {format_duration(dur):>10}   completed {completed}")

    print()


# ---------------------------------------------------------------------------
# Extra Challenge: Validate results
# ---------------------------------------------------------------------------

def validate_results(jobs, stats, sample_size, total_available):
    """
    Runs basic checks on the results and prints warnings if something
    looks off. These checks help catch situations where the query isn't
    returning what you'd expect — before you build a polling strategy on
    data that may not be realistic.
    """
    print("--- Validation Checks ---")
    issues_found = False

    # Check 1: Did we get enough results?
    if len(jobs) < sample_size:
        print(f"  ⚠ Only {len(jobs)} result(s) returned (requested {sample_size}).")
        if total_available == 0:
            print("    No successful refreshes found for this datasource.")
            print("    Confirm the datasource name matches exactly. Check capitalisation.")
        else:
            print(f"    Only {total_available} successful refresh(es) exist in its history.")
        issues_found = True

    # Check 2: Is variability too high to trust the suggested wait?
    if stats["mean"] > 0:
        coefficient_of_variation = stats["stdev"] / stats["mean"]
        if coefficient_of_variation > 0.5:
            print(f"  ⚠ High variability detected (stdev is "
                  f"{coefficient_of_variation:.0%} of mean).")
            print("    The suggested wait time may not be reliable.")
            print("    Consider investigating what causes some refreshes to take much longer.")
            issues_found = True

    # Check 3: Is the most recent refresh exceptionally old?
    if jobs:
        most_recent = jobs[0].completed_at
        if most_recent:
            # Make now timezone-aware to match Tableau's timestamps
            now = datetime.now(timezone.utc)
            days_since = (now - most_recent).days
            if days_since > 7:
                print(f"  ⚠ Most recent successful refresh was {days_since} day(s) ago.")
                print("    Check whether recent refresh jobs are failing or no longer scheduled.")
                issues_found = True

    if not issues_found:
        print("  ✓ All checks passed.")

    print()


# ---------------------------------------------------------------------------
# Extra Challenge: Save execution history
# ---------------------------------------------------------------------------

def save_history(datasource, stats):
    """
    Appends this run's stats to both a CSV and a JSON file so you can
    track how refresh performance changes over time.

    CSV is easy to open in Tableau or Excel for visual analysis.
    JSON keeps the structure and is easy to read programmatically.

    Both files are appended to (not overwritten) so history accumulates
    across multiple runs of the script.
    """
    run_timestamp = datetime.now(timezone.utc).isoformat()

    row = {
        "run_timestamp": run_timestamp,
        "datasource_name": datasource.name,
        "datasource_id": datasource.id,
        "sample_size": stats["sample_size"],
        "mean_seconds": round(stats["mean"], 2),
        "stdev_seconds": round(stats["stdev"], 2),
        "suggested_wait_seconds": round(stats["suggested_wait"], 2),
    }

    # --- Write to CSV ---
    csv_path = Path(HISTORY_CSV)
    write_header = not csv_path.exists()

    with open(csv_path, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=row.keys())
        if write_header:
            writer.writeheader()
        writer.writerow(row)

    print(f"  CSV history updated: {HISTORY_CSV}")

    # --- Write to JSON ---
    json_path = Path(HISTORY_JSON)

    # Load any existing history, or start with an empty list
    if json_path.exists():
        with open(json_path, "r", encoding="utf-8") as f:
            history = json.load(f)
    else:
        history = []

    history.append(row)

    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(history, f, indent=2)

    print(f"  JSON history updated: {HISTORY_JSON}")


# ---------------------------------------------------------------------------
# Main Script
# ---------------------------------------------------------------------------

# Set up authentication using a Personal Access Token (PAT)
tableau_auth = TSC.PersonalAccessTokenAuth(
    token_name=PAT_NAME,
    personal_access_token=PAT_VALUE,
    site_id=SITE_ID
)
server = TSC.Server(TABLEAU_SERVER_URL, use_server_version=True)

with server.auth.sign_in(tableau_auth):

    # Step 1: Find the datasource (by ID if provided, otherwise by name)
    datasource = find_datasource(server, DATASOURCE_NAME, DATASOURCE_ID)

    if datasource is None:
        # find_datasource already printed a helpful message — nothing else to do
        exit(1)

    # Step 2: Retrieve the last N successful refreshes using server-side filters
    jobs, total_available = get_recent_successful_refreshes(
        server, datasource, REFRESH_SAMPLE_SIZE
    )

    if not jobs:
        print("No successful refresh jobs found for this datasource.")
        print("Check that the datasource has extract refreshes configured and has been run at least once.")
        exit(1)

    # Step 3: Calculate timing statistics
    stats = calculate_refresh_stats(jobs)

    if stats is None:
        print("Could not calculate stats — Jobs may be missing start/end timestamps.")
        exit(1)

    # Step 4: Print summary
    print_refresh_summary(datasource, jobs, stats)

    # Extra challenge: Validate the results
    validate_results(jobs, stats, REFRESH_SAMPLE_SIZE, total_available)

    # Extra challenge: Save history to CSV and JSON
    print("Saving execution history...")
    save_history(datasource, stats)
