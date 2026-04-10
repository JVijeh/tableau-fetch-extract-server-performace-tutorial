# Tableau Fetch Extract Server Performance Tutorial

**Challenge:** [#DDQ2026-02 Fetch Recent Refreshes (Intermediate)](https://datadevquest.com/challenges/ddq2026-02-fetch-recent-refreshes-intermediate)  
**Author:** Joshua Vijeh ([@DataDevDiary](https://datadevdiary.com))  
**API Types:** REST API · TSC  
**Difficulty:** Intermediate + Extra Challenge

---

## Overview

This solution demonstrates how to use TSC's filtering and sorting capabilities to retrieve the last 10 successful extract refreshes for a specific datasource — without loading the entire job history into memory. From those results,  you will determine the average duration and standard deviation to produce a statistically informed polling wait time.

The extra challenge takes the solution further by providing a result validation and history of completion saved in .csv and JSON.

The solution essentially follows this order:

```
Find Datasource → Filter Jobs Server-Side → Calculate Stats → Print Summary
                                                            → Validate Results
                                                            → Save History (CSV + JSON)
```

---

## Project Structure

```
tableau-fetch-extract-server-performance-tutorial/
├── .env                        # PAT credentials (replace the template with your own values)
├── .gitignore                  # Ensures .env is never committed
├── fetch_recent_refreshes.py   # Main script
├── refresh_history.csv         # Completion history v1
├── refresh_history.json        # Completion history v2
└── README.md
```

---

## Prerequisites

- Python 3.13+
- [uv](https://docs.astral.sh/uv/) or pip
- A Tableau Cloud site with at least one datasource that has extract refresh history
- A Personal Access Token (PAT) for your Tableau site

> **No datasource with refresh history?** A [Developer Sandbox](https://www.tableau.com/developer/get-site) includes Sample Superstore extract. You can publish a datasource from it, configure an extract refresh schedule, and let it run a few times to build up history before running this script.

---

## Installation

### Using uv

```bash
uv add "tableauserverclient>=0.40" python-dotenv
```

### Using pip

```bash
pip install "tableauserverclient>=0.40" python-dotenv
```

---

## Configuration

Create a `.env` file in the project root with your Tableau credentials (or use the '.env'
template file provided and replace placeholder values with your own):

```env
TABLEAU_SERVER_URL=https://prod-useast-a.online.tableau.com
TABLEAU_SITE_ID=your-site-name
TABLEAU_PAT_NAME=your-pat-name
TABLEAU_PAT_VALUE=your-pat-value
```

Make sure your `.gitignore` includes `.env` so credentials are never accidentally committed:

```
.env
```

Then, at the top of `fetch_recent_refreshes.py`, set your target datasource:

```python
# Set the exact name of the datasource you want to analyze
DATASOURCE_NAME = "Your Datasource Name"

# If the name isn't unique on your site, I recommend setting the ID here instead.
# Leave as 'None' to search by name.
DATASOURCE_ID = None
```

> Not sure if your datasource name is unique? Run the script with `DATASOURCE_ID = None` first. If multiple matches exist, the script will print all of them with their IDs so you can paste the right one in.

---

## How to Run

```bash
python fetch_recent_refreshes.py
```

### Example output

```
Found datasource: 'Sales Pipeline' (ID: 9f3a1b2c-...)
Retrieved 10 successful refresh job(s) (requested 10, total available: 47).

=======================================================
  Refresh Analysis: Sales Pipeline
=======================================================
  Sample size : 10 successful refreshes
  Mean        : 3m 42s (222.1s)
  Std Dev     : 0m 38s (38.4s)
  Suggest wait: 4m 59s (299.0s)
=======================================================

  Recent refresh durations (newest first):
   1.     3m 51s   completed 2026-02-25 06:02
   2.     4m 07s   completed 2026-02-24 06:01
   3.     3m 28s   completed 2026-02-23 06:03
   4.     3m 55s   completed 2026-02-22 06:00
   ...

--- Validation Checks ---
  ✓ All checks passed.

Saving execution history...
  CSV history updated: refresh_history.csv
  JSON history updated: refresh_history.json
```

---

## The Code

```python
import csv
import json
import os
import statistics
from datetime import datetime, timezone
from pathlib import Path

import tableauserverclient as TSC
from dotenv import load_dotenv

load_dotenv()

TABLEAU_SERVER_URL = os.getenv("TABLEAU_SERVER_URL")
SITE_ID = os.getenv("TABLEAU_SITE_ID")
PAT_NAME = os.getenv("TABLEAU_PAT_NAME")
PAT_VALUE = os.getenv("TABLEAU_PAT_VALUE")

DATASOURCE_NAME = "Your Datasource Name"
DATASOURCE_ID = None
REFRESH_SAMPLE_SIZE = 10
HISTORY_CSV = "refresh_history.csv"
HISTORY_JSON = "refresh_history.json"


def find_datasource(server, name, datasource_id=None):
    """
    Finds a datasource on the site by ID (if provided) or by name.

    Using an ID is the most reliable approach — datasource names may not
    always be unique across your projects on a given site. If you search
    by name and get multiple matches, this function will print all of them
    with their IDs so you can set DATASOURCE_ID and re-run.

    Returns a single DatasourceItem, or None if the lookup fails.
    """
    if datasource_id:
        datasource = server.datasources.get_by_id(datasource_id)
        print(f"Found datasource by ID: '{datasource.name}' ({datasource.id})")
        return datasource

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


def get_recent_successful_refreshes(server, datasource, sample_size=10):
    """
    Retrieves the last N successful extract refreshes for the given datasource.

    Filtering is done on the server side so you never have to load all background
    jobs into memory.  You can also call server.jobs.get() directly — NOT TSC.Pager.
    Pager is helpful when you need everything across many pages, but here you want
    exactly one page of exactly the right size.
    """
    options = TSC.RequestOptions(page_size=sample_size)

    options.filter.add(
        TSC.Filter("jobType", TSC.RequestOptions.Operator.Equals, "RefreshExtract")
    )
    options.filter.add(
        TSC.Filter("status", TSC.RequestOptions.Operator.Equals, "Succeeded")
    )
    options.filter.add(
        TSC.Filter("title", TSC.RequestOptions.Operator.Equals, datasource.name)
    )
    options.sort.add(
        TSC.Sort("completedAt", TSC.RequestOptions.Direction.Desc)
    )

    jobs, pagination = server.jobs.get(options)

    print(f"Retrieved {len(jobs)} successful refresh job(s) "
          f"(requested {sample_size}, total available: {pagination.total_available}).")

    return jobs, pagination.total_available


def calculate_refresh_stats(jobs):
    """
    Calculates duration statistics across a list of completed refresh jobs.

    Duration is measured in seconds from the start of the job to completion.
    The suggested_wait is the mean + 2 standard deviations — a reference 
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
    stdev_secs = statistics.stdev(durations) if len(durations) > 1 else 0.0
    suggested_wait = mean_secs + (2 * stdev_secs)

    return {
        "durations": durations,
        "mean": mean_secs,
        "stdev": stdev_secs,
        "suggested_wait": suggested_wait,
        "sample_size": len(durations),
    }


def print_refresh_summary(datasource, jobs, stats):
    """
    Prints a formatted summary of the refresh history and analysis.
    """
    def format_duration(seconds):
        minutes = int(seconds // 60)
        secs = int(seconds % 60)
        return f"{minutes}m {secs:02d}s"

    print(f"\n{'=' * 55}")
    print(f" Refresh Analysis: {datasource.name}")
    print(f"{'=' * 55}")
    print(f" Sample size : {stats['sample_size']} successful refreshes")
    print(f" Mean        : {format_duration(stats['mean'])} ({stats['mean']:.1f}s)")
    print(f" Std Dev     : {format_duration(stats['stdev'])} ({stats['stdev']:.1f}s)")
    print(f" Suggest wait: {format_duration(stats['suggested_wait'])} "
          f"({stats['suggested_wait']:.1f}s)")
    print(f"{'=' * 55}")

    print("\n  Recent refresh durations (newest first):")
    for i, (job, dur) in enumerate(zip(jobs, stats["durations"]), start=1):
        completed = job.completed_at.strftime("%Y-%m-%d %H:%M") if job.completed_at else "unknown"
        print(f"  {i:>2}. {format_duration(dur):>10}   completed {completed}")

    print()


def validate_results(jobs, stats, sample_size, total_available):
    """
    Runs basic checks on the results and prints warnings if something
    looks off. These checks look for situations where the query isn't
    returning what you'd expect — before you build a polling strategy on
    data that may not be realistic.
    """
    print("--- Validation Checks ---")
    issues_found = False

    # Check 1: Did we get enough results?
    if len(jobs) < sample_size:
        print(f" Only {len(jobs)} result(s) returned (requested {sample_size}).")
        if total_available == 0:
            print("  No successful refreshes found for this datasource.")
            print("  Confirm the datasource name matches exactly. Check capitalisation.")
        else:
            print(f"  Only {total_available} successful refresh(es) exist in its history.")
        issues_found = True

    # Check 2: Is variability too high for you to trust the suggested wait?
    if stats["mean"] > 0:
        coefficient_of_variation = stats["stdev"] / stats["mean"]
        if coefficient_of_variation > 0.5:
            print(f"  Large variability found (stdev is "
                  f"{coefficient_of_variation:.0%} of mean).")
            print("  The suggested wait time may not be reliable.")
            print("  Consider looking into what is causing some refreshes to take much longer.")
            issues_found = True

    # Check 3: Is the most recent refresh too old?
    if jobs:
        most_recent = jobs[0].completed_at
        if most_recent:
            now = datetime.now(timezone.utc)
            days_since = (now - most_recent).days
            if days_since > 7:
                print(f" Most recent successful refresh was {days_since} day(s) ago.")
                print("  Check whether the recent refresh jobs are failing or no longer scheduled.")
                issues_found = True

    if not issues_found:
        print("All checks passed!")

    print()


def save_history(datasource, stats):
    """
    Appends this run's stats to both a CSV and a JSON historical file.

    CSV is easy to open in Tableau or Excel for trend analysis over time.
    JSON helps maintain the structure and is easy to read programmatically.
    Both files are cumulative across multiple runs — they are appended to,
    not overwritten.
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

    # Write to CSV — create the header row on the first run
    csv_path = Path(HISTORY_CSV)
    write_header = not csv_path.exists()

    with open(csv_path, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=row.keys())
        if write_header:
            writer.writeheader()
        writer.writerow(row)

    print(f"CSV history updated: {HISTORY_CSV}")

    # Write to JSON — load existing history, append, write back
    json_path = Path(HISTORY_JSON)

    if json_path.exists():
        with open(json_path, "r", encoding="utf-8") as f:
            history = json.load(f)
    else:
        history = []

    history.append(row)

    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(history, f, indent=2)

    print(f"  JSON history updated: {HISTORY_JSON}")


# --- Main Script ---

tableau_auth = TSC.PersonalAccessTokenAuth(
    token_name=PAT_NAME,
    personal_access_token=PAT_VALUE,
    site_id=SITE_ID
)
server = TSC.Server(TABLEAU_SERVER_URL, use_server_version=True)

with server.auth.sign_in(tableau_auth):

    # Step 1: Find the datasource by ID (if completed) or by name
    datasource = find_datasource(server, DATASOURCE_NAME, DATASOURCE_ID)

    if datasource is None:
        exit(1)

    # Step 2: Pull last N successful refreshes using server-side filters
    jobs, total_available = get_recent_successful_refreshes(
        server, datasource, REFRESH_SAMPLE_SIZE
    )

    if not jobs:
        print("No successful refresh jobs found for this datasource.")
        print("Check that the datasource has extract refreshes configured and has been run at least once.")
        exit(1)

    # Step 3: Calculate timing stats
    stats = calculate_refresh_stats(jobs)

    if stats is None:
        print("Could not calculate stats.  Jobs may be missing start/end timestamps.")
        exit(1)

    # Step 4: Print the summary
    print_refresh_summary(datasource, jobs, stats)

    # Extra challenge: Validate results for anything unexpected
    validate_results(jobs, stats, REFRESH_SAMPLE_SIZE, total_available)

    # Extra challenge: Append this run to history files
    print("Saving execution history...")
    save_history(datasource, stats)
```

---

## Key Concepts

### Why server filtering instead of paging through everything?

The initial thought might be to use `TSC.Pager(server.jobs)` to load all background jobs and then filter them in Python. On an active Tableau site, this could mean pulling a large number of jobs into memory in order to find only 10 total. Server-side filtering  using `RequestOptions` with filters, pushes that work to Tableau's servers and returns only the jobs you actually care about.

The filters used here are:

| Filter | Value | Purpose |
|---|---|---|
| `jobType` | `RefreshExtract` | Only extract refresh jobs |
| `status` | `Succeeded` | Only successful completions |
| `title` | datasource name | Only jobs for this specific datasource |

Combined with a `page_size` of exactly 10 and a `completedAt` descending sort, the server returns no more than the 10 most recent successful refreshes.

### `server.jobs.get()` vs. `TSC.Pager`

`TSC.Pager` is helpful when you need all results across many pages. However, in this case you want exactly one page of exactly the right size. Calling `server.jobs.get(options)` directly returns a tuple of `(jobs_list, pagination_item)` — The first page only, under our control. Using `Pager` here would keep fetching additional pages until it ran out of results, which basically defeats the purpose of creating an approach that does not tax the memory.

### How is duration calculated?

Each `JobItem` has `started_at` and `completed_at` as Python `datetime` objects. Subtracting them gives a `timedelta`, and calling `.total_seconds()` converts that to a plain number we can calculate with.

```python
duration = (job.completed_at - job.started_at).total_seconds()
```

### What is the suggested wait time and why use mean + 2 stdev?

When you trigger a refresh programmatically, you eventually need to poll Tableau to check whether it's done. Polling too frequently wastes API calls; waiting too long slows down your workflow unnecessarily.

A starting wait of **mean + 2 standard deviations** is a practical baseline. Assuming refresh times follow a normal distribution, this covers about 95% of typical refresh durations before your first poll.  Most of the time, the job will already be completed when you check. From that point, you can poll on a shorter interval for the remaining jobs.

### Why is datasource name uniqueness important?

Tableau does not enforce unique names across projects. A datasource called "Sales Data" for example, could exist in both the Sales project and the Finance project. If your filter returns multiple matches and you pick the wrong one, your stats are based on the wrong datasource's refresh history.  The script will then run without error and produce unreliable output. Setting `DATASOURCE_ID` eliminates the possibility.

### What does the validation step detect?

Three things that can cause the query to not return what you expect:

**Fewer results than requested** — Either the datasource hasn't run enough times to fill the sample, or the name filter isn't matching what you think it is (capitalisation, trailing spaces, etc.).

**High coefficient of variation** — When the standard deviation is more than 50% of the mean, refresh times are very inconsistent. The suggested wait time becomes less reliable, and it's worth investigating what's causing the variance.

**Stale most-recent refresh** — If the most recent successful refresh is more than 7 days old, something may have changed: the schedule may have been removed, recent runs may be failing, or the datasource may no longer be active.

### Why both CSV and JSON for history?

The CSV opens directly in Tableau or Excel — You can build a trend line showing whether your datasource is getting slower over time without having to write additional code. The JSON keeps the data structure clean and is easy to read by another Python script or  external tool. Writing both gives you flexibility.

---

## Video Walkthrough

*Coming soon — link will be added here.*

---

## Resources

- [TSC Jobs API Reference](https://tableau.github.io/server-client-python/docs/api-ref#jobs)
- [Filtering and Sorting with TSC](https://tableau.github.io/server-client-python/docs/filter-sort)
- [Filtering and Sorting with the REST API](https://help.tableau.com/current/api/rest_api/en-us/REST/rest_api_concepts_filtering_and_sorting.htm)
- [Refreshing Specific Objects on Tableau Server](https://github.com/jorwoods/tableau_articles/blob/main/refresh_objects.md)
- [Tableau Server Client for Python (PyPI)](https://pypi.org/project/tableauserverclient/)
- [Postman Collection for Tableau REST APIs](https://www.postman.com/salesforce-developers/salesforce-developers/collection/x06mp2m/tableau-apis)
- [DataDevQuest Community Slack](https://tableau-datadev.slack.com/archives/C08ETFU5M9P)
