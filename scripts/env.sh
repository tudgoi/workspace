TUDGOI_DATA=../tudgoi-data

import () {
    rm -rf output && mkdir output &&
    cargo run import $TUDGOI_DATA output/directory.db
}

export-data () {
(
    cd $TUDGOI_DATA
    if [ -n "$(git status --porcelain)" ]; then
        echo "Error: There are uncommitted changes in the 'data' directory."
        echo "Please commit your changes before running the export."
        return 1
    fi
)
    
    cargo run export output/directory.db $TUDGOI_DATA
}

render() {
    (
        set -e
        rm -rf output/html output/search
        cargo run render output/directory.db output/html
    )
}

serve () {
    (
        set -e
        cd output/html
        uv run python -m http.server 8000
    )
}

all () {
    import && render 
}

check-main() {
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
}

release () {
    check-main || return 1
    
    (
        cd $TUDGOI_DATA
        check-main
    ) || return 1

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
