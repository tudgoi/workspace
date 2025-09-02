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