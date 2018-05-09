do_submit() {
    pre_submit_hook "$@" || exit

    SBATCH_OUT=$(sbatch --output="$OUTDIR/%j.out" --error="$OUTDIR/%j.err" -D "$CWD" "${SBATCH_ARGS[@]}" "${BATCH_SCRIPT_ARGS[@]}") || exit
    echo "$SBATCH_OUT" | awk '{ print $4 }'
}
