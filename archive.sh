#!/bin/bash

function logln () {
    if [ $VERBOSE -eq 1 ]; then
        echo $1
    fi
}

function log () {
    if [ $VERBOSE -eq 1 ]; then
        echo -n $1
    fi
}

PROJECT=owncloud
TMPDIR=.tmp
THIRDPARTY=3rdparty
APPLIST=applist
FORMAT=tar
PREFIX=owncloud/
VERBOSE=0
THEMES=themes

# Process command-line arguments.
while test $# -gt 0; do
    case $1 in
        --prefix )
            shift
            PREFIX=$1/
            shift
            ;;

        --version )
            shift
            version=$1
            shift
            ;;

        --theme )
            shift
            theme=$1
            shift
            ;;

        --verbose | -v )
            shift
            VERBOSE=1
            ;;

        * )
            break
            ;;
    esac
done

workspace=$(pwd)
today=$(date +%Y%m%d)

mkdir $TMPDIR

# archive core
if [ -z $version ]; then
    tarball=$PROJECT-$today.tar
else
    tarball=$PROJECT-$version.tar
fi

dest=$workspace/$TMPDIR/$tarball

git archive --format=$FORMAT --prefix=$PREFIX HEAD > $dest

# archive 3rdparty
log "Archiving submodule : 3rdparty .."

cd $THIRDPARTY

thirdparty=$workspace/$TMPDIR/$THIRDPARTY.tar

git archive --format=$FORMAT --prefix=$PREFIX$THIRDPARTY/ HEAD > $thirdparty

tar -Af $dest $thirdparty

logln ". done"

cd $workspace

# archive themes
if [ -z $theme ]; then
    :
else 
    log "Archiving theme : $theme .."

    cd $THEMES/$theme

    themetar=$workspace/$TMPDIR/$theme.tar

    git archive --format=$FORMAT --prefix=$PREFIX$THEMES/$theme/ HEAD > $themetar

    tar -Af $dest $themetar

    logln ". done"

    cd $workspace
fi

# archive apps
applist=$TMPDIR/$APPLIST

touch $applist

find apps -mindepth 2 -name .git -type d | sed -e "s/^\.\///" -e "s/\.git$//" >> $applist

while read path; do
    cd $path

    appname=$(echo $path | sed -e "s/apps\///" -e "s/\/$//")
    app=$workspace/$TMPDIR/$appname.tar

    log "Archiving app : $appname .."

    git archive --format=$FORMAT --prefix=$PREFIX$path HEAD > $app

    tar -Af $dest $app

    logln ". done"

    cd $workspace
done < $applist

log "Compressing $tarball .."

bzip2 $dest

logln ". done"
