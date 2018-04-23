#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#include <llapi.h>


int main(int argc, char **argv) {
    if (argc != 4) {
        fprintf(stderr, "Usage: %s jcf monitor_program monitor_arg\n", argv[0]);
        exit(1);
    }

    LL_job job;

    int ret = llsubmit(argv[1], argv[2], argv[3], &job, LL_JOB_VERSION);
    if (ret == 0) {
        printf("%s\n", job.job_name);
        llfree_job_info(&job, LL_JOB_VERSION);
    }

    return ret != 0;
}
