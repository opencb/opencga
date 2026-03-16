# OpenCGA Local Docker Deployment

Single-command local environment with MongoDB, Solr, OpenCGA REST, OpenCGA Master, and IVA.
Optionally includes HBase + Phoenix for the Hadoop storage engine.

## Service Dependencies

```
          ┌───────────┐          ┌───────────┐       ┌───────────┐
          │  MongoDB  │          │   Solr    │       │  HBase    │ (hadoop only)
          └─────┬─────┘          └─────┬─────┘       └─────┬─────┘
                │                      │                   │
         ┌──────┴──────┐      ┌────────┴───────────┐      │
         │ mongo-init  │      │solr-configsets-copy │      │
         │ (rs.init)   │      └────────┬───────────┘      │
         └──────┬──────┘               │                   │
                │                ┌─────┴──────┐            │
                │                │ solr-init  │            │
                │                │ (upload)   │            │
                │                └─────┬──────┘            │
                └──────────┬───────────┴───────────────────┘
                           │
                  ┌────────┴─────────┐
                  │   opencga-init   │
                  │ (catalog install)│
                  └────────┬─────────┘
                           │
                  ┌────────┴─────────┐
                  │   opencga-rest   │
                  │   (REST API)     │
                  │     :9090        │
                  └──┬──────────┬────┘
                     │          │
          ┌──────────┴──┐  ┌───┴──────────┐
          │opencga-setup│  │     IVA      │
          │ (org + user)│  │    :8080     │
          └──────┬──────┘  └──────────────┘
                 │
        ┌────────┴────────┐
        │ opencga-master  │
        │  (daemon/jobs)  │
        └────────┬────────┘
                 │
        ┌────────┴────────┐
        │   load-demo     │
        │   (optional)    │
        └─────────────────┘
```

## Prerequisites

- Docker with Compose plugin (`docker compose`)
- Built OpenCGA: `mvn clean install -DskipTests`
- For Hadoop mode: `mvn clean install -DskipTests -Dhadoop=hbase2.5`

## Quick Start

```bash
# Build the Docker image and start all services
./deploy.sh up --build

# Start with demo data
./deploy.sh up --build --load-demo

# Start with Hadoop storage engine (HBase + Phoenix)
./deploy.sh up --build --storage hadoop
```

## Commands

```
./deploy.sh up            # Start all services
./deploy.sh down          # Stop and remove all services
./deploy.sh restart       # Restart all services (down + up)
./deploy.sh build         # Build opencga-base Docker image from local build
./deploy.sh list          # List all instances and their status
./deploy.sh load-demo     # Load demo data (project, study, VCF index jobs)
./deploy.sh cli           # OpenCGA CLI (auto-login as owner)
./deploy.sh shell         # Interactive shell in opencga-base container
./deploy.sh status        # Show service status
./deploy.sh top           # Live dashboard (CPU, memory, I/O, jobs, volumes)
./deploy.sh health        # Check health of all services
./deploy.sh info          # Show deployment configuration and versions
./deploy.sh logs [svc]    # Tail logs (optionally filter by service)
./deploy.sh mongosh       # Open MongoDB shell
./deploy.sh clean         # Remove data/, conf/, iva/ and Docker volumes
./deploy.sh init-conf     # Regenerate conf/ from build templates
```

Run `./deploy.sh <command> --help` for command-specific options.

## Multiple Instances

Run multiple independent deployments side by side using `--name`:

```bash
# Default instance (name: local)
./deploy.sh up --build

# Second instance with Hadoop
./deploy.sh --name hadoop up --build --storage hadoop --rest-port 9091 --mongo-port 27018 --solr-port 8984 --iva-port 8081

# List all instances
./deploy.sh list

# Operate on a specific instance
./deploy.sh --name hadoop top
./deploy.sh --name hadoop logs opencga-rest
./deploy.sh --name hadoop restart opencga-master
./deploy.sh --name hadoop down
./deploy.sh --name hadoop clean
```

Each instance is fully self-contained under `~/.opencga/instances/<name>/` with its own
compose files, scripts, config, data, `.env`, and Docker volumes.

## Restarting Individual Services

```bash
# Restart a single service (recreates container, picks up config changes)
./deploy.sh restart opencga-rest
./deploy.sh restart hbase

# Restart with updated heap
./deploy.sh restart --hbase-heap 4g hbase
```

## File Layout

Source files (checked into git):
```
opencga-app/app/cloud/docker/compose/
├── docker-compose.yml
├── docker-compose.hadoop.yml    # HBase overlay (included when storage=hadoop)
├── deploy.sh
├── .env.template
├── README.md
├── scripts/
│   ├── opencga-install.sh
│   ├── opencga-setup.sh
│   └── opencga-load-demo.sh
└── hadoop/
    ├── Dockerfile               # HBase 2.5.10 + Phoenix 5.2.0 standalone
    └── hbase-site.xml
```

Instance directory (per deployment, under `~/.opencga/instances/<name>/`):
```
~/.opencga/instances/local/
├── .env                  # Instance config (generated from .env.template)
├── docker-compose.yml    # Synced from source on each 'up'
├── docker-compose.hadoop.yml
├── scripts/              # Synced from source on each 'up'
├── hadoop/               # Synced from source on each 'up'
├── conf/                 # Patched config files (hostnames, storage engine)
│   └── hadoop/
│       └── hbase-site.xml  # Client-side HBase config (hadoop mode)
├── data/
│   ├── sessions/
│   └── logs/
└── iva/
    └── server.json
```

## Configuration

On first run, `deploy.sh` generates `.env` from `.env.template`, auto-detecting
`OPENCGA_VERSION` (from `pom.xml`) and `DOCKER_SOCK`. Edit `.env` to customize
ports, versions, passwords, etc.

Config files in `conf/` are generated from `build/cloud/docker/compose/conf/` with
hostnames patched for Docker networking. Regenerate with `--regen-conf`.

### Storage Engine

Default is MongoDB. To use Hadoop (HBase + Phoenix):

```bash
./deploy.sh up --storage hadoop
```

This:
- Adds the HBase container (standalone mode: ZK + Master + RegionServer in one JVM)
- Patches `storage-configuration.yml`: default engine, embedded MR executor, gz compression
- Generates client-side `hbase-site.xml` for the OpenCGA containers
- Reduces `preSplit.numSplits` from 500 to 10 (appropriate for local/standalone)

## Services

| Service | URL | Mode |
|---------|-----|------|
| OpenCGA REST | http://localhost:9090/opencga | always |
| IVA | http://localhost:8080/iva | always |
| MongoDB | localhost:27017 | always |
| Solr | http://localhost:8983 | always |
| HBase Master UI | http://localhost:16010 | hadoop only |
| ZooKeeper | localhost:2181 | hadoop only |

## Memory Configuration

Default memory limits, configurable in `.env` or via CLI flags:

| Service | Java Heap | Container Limit | Formula |
|---------|-----------|-----------------|---------|
| OpenCGA REST | 500m | 650m | heap * 1.3 |
| OpenCGA Master | 400m | 1820m | (master + job) * 1.3 |
| Job processes | 1g | (within master) | — |
| HBase | 2g | 2662m | heap * 1.3 |
| MongoDB | — | 1g | fixed |
| Solr | — | 1g | fixed |
| IVA | — | 20m | fixed |

Override via CLI (persisted to `.env`):

```bash
./deploy.sh up --rest-heap 2g --master-heap 512m --job-heap 2g --hbase-heap 4g
```

Or edit `.env` directly:

```
OPENCGA_REST_HEAP=2g
OPENCGA_MASTER_HEAP=512m
OPENCGA_JOB_HEAP=2g
OPENCGA_HBASE_HEAP=4g
OPENCGA_MONGO_MEM=2g
OPENCGA_SOLR_MEM=2g
```

## Default Credentials

Configured in `.env`:

- **Admin**: `opencga` / `OPENCGA_ADMIN_PASSWORD`
- **Owner**: `OPENCGA_OWNER_ID` / `OPENCGA_OWNER_PASSWORD` (organization: `OPENCGA_ORG_ID`)
