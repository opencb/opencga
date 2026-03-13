# OpenCGA Local Docker Deployment

Single-command local environment with MongoDB, Solr, OpenCGA REST, OpenCGA Master, and IVA.

## Service Dependencies

```
          ┌───────────┐          ┌───────────┐
          │  MongoDB  │          │   Solr    │
          └─────┬─────┘          └─────┬─────┘
                │                      │
         ┌──────┴──────┐      ┌────────┴───────────┐
         │ mongo-init  │      │solr-configsets-copy │
         │ (rs.init)   │      └────────┬───────────┘
         └──────┬──────┘               │
                │                ┌─────┴──────┐
                │                │ solr-init  │
                │                │ (upload)   │
                │                └─────┬──────┘
                └──────────┬───────────┘
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
- Built OpenCGA (run `mvn clean install -DskipTests` from the project root)

## Quick Start

```bash
# Build the Docker image and start all services
./deploy.sh up --build

# Start with demo data (project, study, VCF index jobs)
./deploy.sh up --load-demo
```

## Commands

```
./deploy.sh up            # Start all services
./deploy.sh down          # Stop and remove all services
./deploy.sh restart       # Restart all services (down + up)
./deploy.sh build         # Build opencga-base Docker image from local build
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

## File Layout

Source files (checked into git, survive `mvn clean`):
```
opencga-app/app/cloud/docker/compose/
├── docker-compose.yml
├── deploy.sh
├── opencga.sh
├── .env.template
├── README.md
└── scripts/
    ├── opencga-install.sh
    ├── opencga-setup.sh
    └── opencga-load-demo.sh
```

Runtime data (user's home, never in repo):
```
~/.opencga/local/               (override with $OPENCGA_LOCAL_HOME)
├── .env              # user config (generated from .env.template on first run)
├── conf/             # patched config files (generated from build output)
├── data/
│   ├── sessions/     # OpenCGA sessions
│   └── logs/         # OpenCGA logs
└── iva/
    └── server.json   # IVA config
```

## Configuration

On first run, `deploy.sh` generates `~/.opencga/local/.env` from `.env.template`,
auto-detecting `OPENCGA_VERSION` (from `pom.xml`) and `DOCKER_SOCK`. Edit `.env`
to customize ports, versions, passwords, etc. — it won't be overwritten on subsequent runs.

Config files in `~/.opencga/local/conf/` are generated at runtime from
`build/cloud/docker/compose/conf/` with hostnames patched for Docker networking.

The runtime data location can be overridden with the `OPENCGA_LOCAL_HOME` environment
variable, for example for CI or custom setups.

## Services

| Service | URL |
|---------|-----|
| OpenCGA REST | http://localhost:9090/opencga/webservices/rest/v3/meta/status |
| IVA | http://localhost:8080/iva |
| MongoDB | localhost:27017 |
| Solr | http://localhost:8983 |

## Memory Configuration

Default memory limits, configurable in `.env`:

| Service | Java Heap | Multiplier | Container Limit |
|---------|-----------|------------|-----------------|
| OpenCGA REST | 500m | 1.3x | 650m |
| OpenCGA Master | 1g | 2.2x | 2253m |
| MongoDB | — | — | 1g |
| Solr | — | — | 1g |
| IVA | — | — | 20m |

The Master multiplier (2.2x) accounts for child Java processes spawned during job execution.

Override via CLI (persisted to `.env`):

```bash
./deploy.sh up --rest-heap 2g --master-heap 512m
```

Or edit `.env` directly:

```
OPENCGA_REST_HEAP=2g
OPENCGA_MASTER_HEAP=512m
OPENCGA_MONGO_MEM=2g
OPENCGA_SOLR_MEM=2g
```

## Default Credentials

Configured in `.env`:

- **Admin**: `opencga` / `OPENCGA_ADMIN_PASSWORD`
- **Owner**: `OPENCGA_OWNER_ID` / `OPENCGA_OWNER_PASSWORD` (organization: `OPENCGA_ORG_ID`)
