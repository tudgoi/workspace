import () {
    rm -rf output && mkdir output &&
    cargo run --manifest-path tool/Cargo.toml import data output/directory.db
}

render() {
    cargo run --manifest-path tool/Cargo.toml render output/directory.db templates output/html
}

all () {
    import && render
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