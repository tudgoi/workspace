import () {
    rm -rf output && mkdir output &&
    cargo run --manifest-path xform/Cargo.toml import data output/directory.db
}

export-data () {
    if [ -n "$(git status --porcelain data)" ]; then
        echo "Error: There are uncommitted changes in the 'data' directory."
        echo "Please commit your changes before running the export."
        return 1
    fi
    
    cargo run --manifest-path xform/Cargo.toml export output/directory.db data
}

render() {
    (
        set -e
        rm -rf output/html output/search
        cargo run --manifest-path xform/Cargo.toml render output/directory.db templates output
        if [ -d "output/wasm_output" ]; then
            cp output/wasm_output/tinysearch_engine.* output/html
        fi
    )
}

search-index() {
    (
        set -e

        cd output
        tinysearch -m wasm -p wasm_output search/index.json
        cp wasm_output/tinysearch_engine.* html

        # Add cache-busting query string to wasm file requests
        WASM_HASH=$(sha256sum html/tinysearch_engine.wasm | cut -d' ' -f1 | head -c 8)
        echo "Generated WASM hash for cache busting: $WASM_HASH"
        sed -i "s/tinysearch_engine\.wasm/tinysearch_engine.wasm?v=${WASM_HASH}/g" html/index.html
        sed -i "s/tinysearch_engine\.wasm/tinysearch_engine.wasm?v=${WASM_HASH}/g" wasm_output/demo.html
    )
}

serve () {
    (
        set -e
        cd output/html
        uv run python -m http.server 8000
    )
}

render-json() {
    rm -rf output/json &&
    cargo run --manifest-path xform/Cargo.toml render output/directory.db templates output -o=json
}

ingest() {
    cargo run --manifest-path xform/Cargo.toml ingest $@
}

all () {
    import && render && search-index
}

release () {
    if [ "$(git branch --show-current)" != "main" ]; then
        echo "Error: You must be on the 'main' branch to release."
        return 1
    fi

    if [ -n "$(git status --porcelain)" ]; then
        echo "Error: Your working directory is not clean. Please commit or stash your changes."
        return 1
    fi

    git fetch
    if [ "$(git rev-parse main)" != "$(git rev-parse origin/main)" ]; then
        echo "Error: Your local 'main' branch is not up-to-date with 'origin/main'."
        echo "Please sync with the latest changes before releasing."
        return 1
    fi

    # Run the core release process in a subshell.
    # `set -e` will cause the subshell to exit immediately if a command fails,
    # preventing a partial release without exiting your main shell.
    (
        set -e

        echo "Building site..."
        all

        echo "Syncing with tudgoi.github.io..."
        HASH=$(git rev-parse --short HEAD)
        rsync -rv --exclude '.git' output/html/ ../tudgoi.github.io/

        cd ../tudgoi.github.io

        if [ -n "$(git status --porcelain)" ]; then
            echo "Releasing changes..."
            git add .
            git commit -m "Releasing from tudgoi@${HASH}"
            git push origin main
        else
            echo "No changes to release."
        fi
    )
}

fp () {
    FILE=$(rg -l $1) &&
    ffp $FILE
}

ffp () {
    FILE=$1 &&
    BASE=$(basename $FILE .md) &&
    mv $FILE $HOME/workspace/tudgoi/data/person/$BASE.toml ;
}
fo () {
    FILE=$(rg -l $1) &&
    ffo $FILE
}

ffo () {
    FILE=$1 &&
    BASE=$(basename $FILE .md) &&
    mv $FILE $HOME/workspace/tudgoi/data/office/$BASE.toml ;
}