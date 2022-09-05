+++
title = "FANG"


# The homepage contents
[extra]
lead = '<b>Fang</b> is a background task processing for Rust. It uses Postgres DB as a task queue.'
url = "/docs/getting-started/introduction/"
url_button = "Get started"
repo_version = "GitHub v0.9.0"
repo_license = "Open-source MIT License."
repo_url = "https://github.com/ayrat555/fang"

# Menu items
[[extra.menu.main]]
name = "Docs"
section = "docs"
url = "/docs/getting-started/introduction/"
weight = 10

[[extra.menu.main]]
name = "Blog"
section = "blog"
url = "/blog/"
weight = 20


[[extra.list]]
title = "Async and threaded workers"
content = "Workers can be started in threads (threaded workers) or tokio tasks (async workers)"

[[extra.list]]
title = "Scheduled tasks"
content = "Tasks can be scheduled at any time in the future"

[[extra.list]]
title = "Periodic (CRON) tasks"
content = "Tasks can be scheduled use cron schedules"

[[extra.list]]
title = "Unique tasks"
content = "Tasks are not duplicated in the queue if they are unique"
+++
