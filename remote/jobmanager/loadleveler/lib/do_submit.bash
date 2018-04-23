STATUSDIR="$HOME/.scaling-status"

do_submit() {
    JCF="$("$BINDIR/make_jcf" "$CWD" "$OUTDIR" "$@")"
    "$BINDIR"/llsubmit-with-monitor "$JCF" "$BINDIR/monitor" "$STATUSDIR"
}
