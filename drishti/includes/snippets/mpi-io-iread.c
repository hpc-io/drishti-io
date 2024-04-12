MPI_File fh;
MPI_Status s;
MPI_Request r;
...
MPI_File_open(MPI_COMM_WORLD, "output-example.txt", MPI_MODE_CREATE|MPI_MODE_RDONLY, MPI_INFO_NULL, &fh);
...
MPI_File_iread(fh, &buffer, BUFFER_SIZE, n, MPI_CHAR, &r);
...
// compute something
...
MPI_Test(&r, &completed, &s);
...
if (!completed) {
	// compute something

	MPI_Wait(&r, &s);
}