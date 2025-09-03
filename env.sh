import () {
    rm -rf output && mkdir output &&
    cargo run --manifest-path tool/Cargo.toml import data output/directory.db
}

render() {
    rm -rf output/html
    cargo run --manifest-path tool/Cargo.toml render output/directory.db templates output/html
}

all () {
    import && render
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