# bean

This repository contains a minimal example of loading configuration with Pydantic v2.

## Running

Install dependencies:

```bash
pip install -r requirements.txt
```

Then run the CLI:

```bash
python -m cli.main
```

Environment variables such as `MYSQL_HOST`, `MYSQL_PORT`, `MYSQL_USER`,
`MYSQL_PASSWORD`, and `MYSQL_DATABASE` are used to build the MySQL connection URL.
Defaults are provided for host, port, and user.
