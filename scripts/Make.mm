# -*- Makefile -*-

PROJECT = pathos
PACKAGE = scripts

PROJ_TIDY += *.log *.out
PROJ_CLEAN =

#--------------------------------------------------------------------------
#

all: export

#--------------------------------------------------------------------------
#

EXPORT_BINS = \
    pathos_server.py \
    tunneled_pathos_server.py \
    pathos_tunnel.py \

export:: export-binaries release-binaries

# End of file
