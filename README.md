# DANDI Cache: `dandiset-id-to-title`

Maps each Dandiset ID to its current title.

For every Dandiset, this cache reads the `name` field from the `draft` version's metadata manifest directly from the public DANDI archive S3 bucket -- the `draft` version always reflects the Dandiset's current title, since edits to it apply immediately, while published versions are frozen snapshots taken at publish time.

The cache is accumulative: a Dandiset's title is refreshed whenever it is readable, and its last known title is retained if the Dandiset later becomes embargoed or otherwise unreadable, rather than being dropped from the map.

Updated daily, since a Dandiset's title can change at any time.

Primarily for use by developers.



## One-time use

If you only plan to use this cache infrequently or from disparate locations, you can directly download the latest version of the cache as a compressed [JSON Lines](https://jsonlines.org/) file from the `dist` branch:

### Python API (recommended)

```python
import gzip
import json

import requests

url = "https://raw.githubusercontent.com/dandi-cache/dandiset-id-to-title/refs/heads/dist/derivatives/dandiset_id_to_title.jsonl.gz"
response = requests.get(url)
lines = gzip.decompress(data=response.content).decode("utf-8").splitlines()
dandiset_id_to_title = {
    dandiset_id: title for line in lines for dandiset_id, title in json.loads(line).items()
}
```

Each line is one JSON record mapping a Dandiset ID to its title:

```json
{"<dandiset id>": "<title>"}
```

### Save to file

```bash
curl https://raw.githubusercontent.com/dandi-cache/dandiset-id-to-title/refs/heads/dist/derivatives/dandiset_id_to_title.jsonl.gz -o dandiset_id_to_title.jsonl.gz
```



## Repeated use

If you plan on using this cache regularly, clone the `derivatives` branch of this repository:

```bash
git clone --branch derivatives https://github.com/dandi-cache/dandiset-id-to-title.git
```

Or, if you prefer [DataLad](https://www.datalad.org/):

```bash
datalad clone https://github.com/dandi-cache/dandiset-id-to-title.git --branch derivatives
```

Then set up a CRON on your system to pull the latest version of the cache at your desired frequency.

For example, through `crontab -e`, add:

```bash
0 0 * * * git -C /path/to/dandiset-id-to-title pull
```

This will minimize data overhead by only loading the most recent changes.
