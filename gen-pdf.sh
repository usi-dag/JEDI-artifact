#!/bin/bash

thisdir=$(readlink -f "${BASH_SOURCE[0]%/*}")
source $thisdir/_common.sh

cd $BASEDIR/analysis/out

pdflatex -synctex=1 -interaction=nonstopmode data.tex