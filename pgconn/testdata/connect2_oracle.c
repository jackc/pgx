/* Optional connection oracle, linked against libpq 18.6. No credentials or
 * server error strings are printed. Normal Go tests do not build this file. */
#include <libpq-fe.h>
#include <stdio.h>

int main(int argc, char **argv)
{
    if (PQlibVersion() != 180006) {
        fprintf(stderr, "expected libpq 18.6 (180006), got %d\n", PQlibVersion());
        return 2;
    }
    if (argc != 2)
        return 2;
    PGconn *conn = PQconnectdb(argv[1]);
    printf("%s\n", PQstatus(conn) == CONNECTION_OK ? "connected" : "failed");
    PQfinish(conn);
    return 0;
}
