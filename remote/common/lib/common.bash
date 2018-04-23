#!/bin/bash

. "$LIBDIR/hooks.bash"

if [ x$SCALING_XARGS_CHUNKSIZE = x ]; then
    SCALING_XARGS_CHUNKSIZE=100
fi

bash_xargs() {
    local EXIT_CODE=0
    local CHUNK=()

    while read -r elem; do
        CHUNK+=("$elem")
        if [ ${#CHUNK[@]} -ge $SCALING_XARGS_CHUNKSIZE ]; then
            "$@" "${CHUNK[@]}"
            if [ $? -ne 0 ]; then
                EXIT_CODE=$?
            fi

            CHUNK=()
        fi
    done

    if [ ${#CHUNK[@]} -ge 1 ]; then
        "$@" "${CHUNK[@]}"
        if [ $? -ne 0 ]; then
            EXIT_CODE=$?
        fi
    fi

    exit $EXIT_CODE
}
