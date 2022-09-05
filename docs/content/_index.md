+++
title = "FANG"


# The homepage contents
[extra]
lead = '<b>Fang</b> is a background task processing for Rust. It uses Postgres DB as a task queue.'
url = "/docs/readme/readme"
url_button = "Get started"
repo_version = "GitHub v0.9.0"
repo_license = "Open-source MIT License."
repo_url = "https://github.com/ayrat555/fang"


# Menu items
[[extra.menu.main]]
name = "README"
section = "docs"
url = "/docs/readme/readme"
weight = 10

[[extra.menu.main]]
name = "CHANGELOG"
section = "docs"
url = "/docs/changelog/changelog"
weight = 10

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
content = 'Tasks can be scheduled with a <a href="https://docs.oracle.com/cd/E12058_01/doc/doc.1014/e12030/cron_expressions.htm">cron expression</a>'

[[extra.list]]
title = "Unique tasks"
content = 'Tasks are not duplicated in the queue if they are unique'
+++

+++
