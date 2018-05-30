SODIR="somedir"

job_manager_hook() {
    module add slurm
}

pre_submit_hook() {
    local TYPE="$1"
    shift
    local MODULES="$1"
    shift
    local PARTITION="$1"
    shift
    local NTASKS="$1"
    shift
    local NTASKS_PER_NODE="$1"
    shift
    local PRELOAD="$1"
    shift

    read -ra MODULE_ARRAY <<< "$MODULES"
    module add "${MODULE_ARRAY[@]}" || return 2

    SBATCH_ARGS=("-p" "$PARTITION" "-n" "$NTASKS" "--ntasks-per-node" "$NTASKS_PER_NODE")

    case $TYPE in
        ompi)
            BATCH_SCRIPT_ARGS=("ompi")
            if [ "$PRELOAD" -eq 1 ]; then
                BATCH_SCRIPT_ARGS+=("-x" "LD_PRELOAD=$SODIR/mpiperf.so")
            fi
            BATCH_SCRIPT_ARGS+=("$@") ;;
        impi)
            BATCH_SCRIPT_ARGS=("impi" "$@") ;;
        *)
            return 2;;
    esac
}
