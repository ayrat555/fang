+++
title = "FANG"


# The homepage contents
[extra]
lead = '<b>Fang</b> is a background task processing for Rust. It uses Postgres DB as a task queue.'

url = "/docs/readme"

url_button = "Get started"
repo_version = "GitHub v0.10.2"
repo_license = "Open-source MIT License."
repo_url = "https://github.com/ayrat555/fang"

# Menu items
[[extra.menu.main]]
name = "README"
section = "docs"
url = "/docs/readme"
weight = 10

[[extra.menu.main]]
name = "CHANGELOG"
section = "docs"
url = "/docs/changelog"

[[extra.menu.main]]
name = "Blog"
section = "blog"
url = "/blog/"
weight = 20

[[extra.list]]
title = "Async and threaded workers"
content = 'Workers can be started in threads (threaded workers) or tokio tasks (async workers)'

[[extra.list]]
title = "Scheduled tasks"
content = 'Tasks can be scheduled at any time in the future'

[[extra.list]]
title = "Periodic (CRON) tasks"
content = 'Tasks can be scheduled using cron expressions'

[[extra.list]]
title = "Unique tasks"
content = 'Tasks are not duplicated in the queue if they are unique'

[[extra.list]]
title = "Single-purpose workers"
content = 'Tasks are stored in a single table but workers can execute only tasks of the specific type'
+++

[[extra.list]]
title = "Retries"
content = 'Tasks can be retried with a custom backoff mode'
+++
