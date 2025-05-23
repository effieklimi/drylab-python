# Drylab (python package)

Event-sourced lab automation core library for Python.

## Overview

Drylab delivers **unbreakable reproducibility** and an **immutable audit trail** for every step of the autonomous science workflows carried out by your scientific AI agents.

Drylab uses an event‐sourced ledger that **cryptographically seals**, **schema‐validates** and **version‐controls** every instrument readout, simulation snapshot and llm insight

- **instant provenance**: trace any result back to the exact raw bytes
- **built-in verification**: reject bad data before it contaminates your analysis
- **replay & branching**: fork a run, tweak parameters and diff outcomes in seconds

## Features

- Event-sourced architecture for reliable lab automation
- Schema validation and registry for lab protocols
- SQLite-based ledger for event persistence
- Type-safe interfaces for lab operations
- Pydantic integration for data validation

## Requirements

- Python >= 3.10
- Dependencies:
  - pydantic >= 2.6
  - sqlite-utils >= 3.35
  - jsonschema >= 4.22

## Installation

You can install DryLab using pip:

```bash
pip install drylab
```

Or install from source:

```bash
git clone https://github.com/drylab/drylab-python.git
cd drylab-python
pip install -e .
```

## Quick Start

```python
from drylab import Reactor, Ledger

# Initialize the reactor and ledger
ledger = Ledger("lab_events.db")
reactor = Reactor(ledger)

# Your lab automation code here
```

## Project Structure

```
drylab/
├── schemas/         # JSON schemas for lab protocols
├── reactor.py      # Core reactor implementation
├── ledger.py       # Event persistence layer
├── types.py        # Type definitions
└── schema_registry.py  # Schema management
```

## Documentation

For detailed documentation, visit [https://drylab.bio](https://drylab.bio)

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the terms of the license included in the repository.

## Contact

- Email: dev@drylab.io
- Website: https://drylab.bio
