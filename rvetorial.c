#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <mpi.h>
 
#define NUM_PROCESSOS 3
#define TAG_CLOCK 0
 
typedef struct Clock {
    int p[NUM_PROCESSOS];
} Clock;
 
static void VerificaMPI(int codigo, const char *operacao) {
    if (codigo != MPI_SUCCESS) {
        fprintf(stderr, "Erro MPI em %s (codigo %d)\n", operacao, codigo);
        MPI_Abort(MPI_COMM_WORLD, codigo);
    }
}
 
void PrintClock(const char *acao, Clock *clock) {
    int pid;
    MPI_Comm_rank(MPI_COMM_WORLD, &pid);
    printf("[Atualizado por P%d] %s -> Clock = (%d,%d,%d)\n",
           pid, acao, clock->p[0], clock->p[1], clock->p[2]);
    fflush(stdout);
}
 
void Event(Clock *clock) {
    int pid;
    MPI_Comm_rank(MPI_COMM_WORLD, &pid);
    clock->p[pid]++;
 
    PrintClock("Evento interno", clock);
}
 
void Send(int dest, Clock *clock) {
    int pid;
    MPI_Comm_rank(MPI_COMM_WORLD, &pid);
 
    clock->p[pid]++;
    VerificaMPI(MPI_Send(clock->p, NUM_PROCESSOS, MPI_INT, dest, TAG_CLOCK,
                         MPI_COMM_WORLD), "MPI_Send");
 
    PrintClock("Envio de mensagem", clock);
}
 
void Receive(int src, Clock *clock) {
    int pid;
    int recebido[NUM_PROCESSOS];
    MPI_Comm_rank(MPI_COMM_WORLD, &pid);
 
    VerificaMPI(MPI_Recv(recebido, NUM_PROCESSOS, MPI_INT, src, TAG_CLOCK,
                         MPI_COMM_WORLD, MPI_STATUS_IGNORE), "MPI_Recv");
 
    clock->p[pid]++;
    for (int i = 0; i < NUM_PROCESSOS; i++) {
        if (recebido[i] > clock->p[i])
            clock->p[i] = recebido[i];
    }
 
    PrintClock("Recebimento de mensagem", clock);
}
 
void process0() {
    Clock clock = {{0,0,0}};
    PrintClock("Estado inicial", &clock);
 
    Event(&clock);
    Send(1, &clock);
    Receive(1, &clock);
    Send(2, &clock);
    Receive(2, &clock);
    Send(1, &clock);
    Event(&clock);
    Event(&clock);
    Receive(1, &clock);
    Send(2, &clock);
    Receive(2, &clock);
    Send(1, &clock);
}
 
void process1() {
    Clock clock = {{0,0,0}};
    PrintClock("Estado inicial", &clock);
 
    Send(0, &clock);
    Receive(0, &clock);
    Receive(0, &clock);
    Receive(2, &clock);
    Send(0, &clock);
    Event(&clock);
    Receive(0, &clock);
    Send(2, &clock);
}
 
void process2() {
    Clock clock = {{0,0,0}};
    PrintClock("Estado inicial", &clock);
 
    Event(&clock);
    Send(0, &clock);
    Receive(0, &clock);
    Send(1, &clock);
    Event(&clock);
    Send(0, &clock);
    Receive(0, &clock);
    Receive(1, &clock);
}
 
int main(void) {
    int my_rank, num_procs;
 
    MPI_Init(NULL, NULL);
    MPI_Comm_rank(MPI_COMM_WORLD, &my_rank);
    MPI_Comm_size(MPI_COMM_WORLD, &num_procs);
 
    if (num_procs != NUM_PROCESSOS) {
        if (my_rank == 0)
            fprintf(stderr, "Este programa requer exatamente %d processos "
                            "(use: mpiexec -n %d ./rvet)\n",
                    NUM_PROCESSOS, NUM_PROCESSOS);
        MPI_Finalize();
        return EXIT_FAILURE;
    }
 
    if (my_rank == 0)
        process0();
    else if (my_rank == 1)
        process1();
    else if (my_rank == 2)
        process2();
 
    MPI_Finalize();
    return EXIT_SUCCESS;
}
 

