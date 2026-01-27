# TUDGOI - The Unofficial Directory --- Government of India

`tudgoi` is a Rust-based application designed to manage and present information about the Government of India. The application provides tools to import data, render it into static HTML, and serve it.

The core of the project is an SQLite database that stores data about entities such as people and offices. The data is internally versioned using a Merkle Search Tree.

### Key Technologies

*   **Backend:** Rust
*   **Web Framework:** Axum
*   **Database:** SQLite
*   **Templating:** Askama
*   **CLI:** Clap

## Architecture

The project is structured as a binary with a command-line interface that exposes several key functionalities:

*   **`import`**: Imports data from an external source into the SQLite database (`output/directory.db`).
*   **`export`**: Exports data from the database back to the source directory.
*   **`render`**: Generates static HTML pages from the database content into the `output/html` directory.
*   **`serve`**: Runs a web server using Axum to serve the generated content, with live-reloading for development.
*   **`augment` & `ingest`**: Additional data processing commands.

The database schema (`sql/schema.sql`) defines tables for `entity`, `entity_photo`, `entity_contact`, tenures, and supervisors, utilizing views and FTS5 for efficient querying and search.

## Building and Running

The project relies on a set of shell functions defined in `scripts/env.sh` for common development tasks.

### Initial Setup

1.  Source the environment script to make the helper functions available:
    ```bash
    source scripts/env.sh
    ```

2.  The project depends on an external data repository. Clone `tudgoi-data` into the parent directory:
    ```bash
    git clone <repository_url> ../tudgoi-data
    ```

### Common Commands

*   **Build everything (import data and render HTML):**
    ```bash
    all
    ```

*   **Run the development web server:**
    This command, found in `bacon.toml`, starts the Axum server, which serves the application and provides live-reloading.
    ```bash
    cargo run -- serve output/directory.db
    ```
    Or, if you have `bacon` installed:
    ```bash
    bacon run-serve
    ```
    The site will be available at `http://localhost:8000`.

*   **Run tests and checks:**
    The use of `bacon.toml` suggests that `bacon` is the preferred tool for continuous checking and testing.
    ```bash
    # For continuous checking
    bacon check

    # For running tests
    bacon test
    ```

### Release Process

The `release` function in `scripts/env.sh` automates the process of building the static site, copying it to a corresponding `tudgoi.github.io` directory, and committing the changes for publication.

## Development Conventions

*   **Data Management:** Data is managed in a separate repository and imported into the application. Changes should be made in the data repository and then re-imported.
*   **Workflows:** Use the helper functions in `scripts/env.sh` for standard development tasks like building and serving the application.
*   **Static Generation:** The primary output is a static website generated into the `output/html` directory. The `serve` command in `scripts/env.sh` is for serving these static files, while the `cargo run -- serve` command is for development with live reload.