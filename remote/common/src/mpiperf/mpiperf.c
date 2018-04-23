#include <stdio.h>
#include <stdbool.h>

#include <mpi.h>


static double my_start_time;


int
MPI_Init(int *argc, char ***argv)
{
    int err = PMPI_Init(argc, argv);
    if (err != MPI_SUCCESS)
        return err;

    my_start_time = MPI_Wtime();

    return err;
}


int
MPI_Finalize()
{
    int err;

    double my_end_time = MPI_Wtime();

    int rank;
    err = MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    double start_time, end_time;
    if (err == MPI_SUCCESS)
        err = MPI_Reduce(&my_start_time, &start_time, 1, MPI_DOUBLE, MPI_MIN, 0, MPI_COMM_WORLD);
    if (err == MPI_SUCCESS)
        err = MPI_Reduce(&my_end_time, &end_time, 1, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);

    if (err == MPI_SUCCESS) {
        if (rank == 0) {
            FILE *file = fopen("__mpiperf.txt", "w");
            if (file) {
                fprintf(file, "%.17g\n", end_time - start_time);
                fclose(file);
            }
        }
    }

    return PMPI_Finalize();
}
