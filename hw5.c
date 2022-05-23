#include <stdio.h>
#include <stdlib.h>
#include <math.h>
#include <errno.h>
#include <unistd.h>
#include <fcntl.h>
#include <signal.h>
#include <sys/types.h>
#include <pthread.h>
#include <string.h>
#include <complex.h>

#define NO_EINTR(stmt) while((stmt) < 0 && errno == EINTR);

typedef struct calc_thread_args {
    int thread_id;
} CalcThreadArgs;

typedef struct calc_thread_return {
    double *matrix_c_column;
    int column_count;
} CalcThreadReturn;

int n;
int thread_count;
char *matrix_a;
char *matrix_b;
double *matrix_c;
sig_atomic_t arrived = 0;
pthread_mutex_t barrier_mutex;
pthread_cond_t barrier_cond;
double complex *ma;

sig_atomic_t sigint_interrupt = 0;

void sigint_handler(int signum){
    if(signum == SIGINT){
        sigint_interrupt = 1;
    }
}

int check_arguments(int argc, char *argv[], char *matrix_file_a, char *matrix_file_b, char *output_file, int *n, int *thread_count);
void *calculation_thread(void *arg);
int read_matrices(const char* matrix_file_a, const char *matrix_file_b, char *matrix_a, char *matrix_b, int matrix_row_col_size);
int write_matrix(const char *output_file, double complex *matrix_c, int matrix_row_col_size);
void __matrix_multiply(char *matrix_a, char *matrix_b, double *matrix_c, int matrix_row_col_size, int column_num, int thread_count);
void insert_columns(double* matrix_c, CalcThreadReturn *calc_thread_return, int thread_count, int thread_id, int matrix_row_col_size);
void discrete_fourier_transform(int k, int l, int matrix_row_col_size, double complex *ma);
void syncronization_barrier();
void get_timestamp(char *timestamp_buf);

int main(int argc, char *argv[])
{
    char matrix_file_a[1024];
    char matrix_file_b[1024];
    char output_file[1024];
    char timestamp_buf[26];

    int matrix_row_col_size;
    pthread_t *threads;
    CalcThreadArgs *thread_args;
    int total_matrix_size;

    double time_start, time_end, delta_time;

    CalcThreadReturn *thread_return = NULL;

    struct sigaction act;
    memset(&act, 0, sizeof(act));
    act.sa_handler = sigint_handler;
    sigaction(SIGINT, &act, NULL);

    if(check_arguments(argc, argv, matrix_file_a, matrix_file_b, output_file, &n, &thread_count) < 0){
        fprintf(stderr, "Error in arguments, usage: ./hw5 -i filePath1 -j filePath2 -o output -n 4 -m 2\n");
        fprintf(stderr, "n should be bigger than 2, m should be bigger than 1\n");
        return -1;
    }

    matrix_row_col_size = (int) pow(2.0, (double) n);
    total_matrix_size = matrix_row_col_size * matrix_row_col_size;

    matrix_a = (char *)malloc(total_matrix_size * sizeof(char));
    matrix_b = (char *)malloc(total_matrix_size * sizeof(char));
    matrix_c = (double *)malloc(total_matrix_size * sizeof(double));

    ma = (double complex *)malloc(total_matrix_size * total_matrix_size * sizeof(double complex));

    if(matrix_a == NULL || matrix_b == NULL || matrix_c == NULL){
        perror("matrix memory malloc: ");
        return -1;
    }

    for(int i = 0; i < total_matrix_size; i++){
        matrix_c[i] = 0;
    }

    if(read_matrices(matrix_file_a, matrix_file_b, matrix_a, matrix_b, matrix_row_col_size) < 0){
        fprintf(stderr, "Error in reading matrices\n");
        return -1;
    }
    get_timestamp(timestamp_buf);
    printf("%s Two matricex of size %dx%d have been read. The number of threads is %d\n", timestamp_buf, matrix_row_col_size, matrix_row_col_size, thread_count);

    threads = (pthread_t *)malloc(thread_count * sizeof(pthread_t));
    if(threads == NULL){
        perror("thread memory malloc: ");
        return -1;
    }

    thread_args = (CalcThreadArgs *)malloc(thread_count * sizeof(CalcThreadArgs));

    if(pthread_mutex_init(&barrier_mutex, NULL) != 0){
        perror("mutex init: ");
        return -1;
    }

    if(pthread_cond_init(&barrier_cond, NULL) != 0){
        perror("cond init: ");
        return -1;
    }

    time_start = clock();

    for(int i = 0; i < thread_count; i++){
        thread_args[i].thread_id = i;
        if(pthread_create(&threads[i], NULL, calculation_thread, (void *) (&thread_args[i])) != 0){
            perror("pthread_create: ");
            return -1;
        }
    }

   

    for(int i = 0; i < thread_count; i++){
        if(pthread_join(threads[i], (void*)&thread_return) != 0){
            perror("pthread_join: ");
            return -1;
        }
    }

    pthread_cond_destroy(&barrier_cond);
    pthread_mutex_destroy(&barrier_mutex);
    free(threads);
    free(thread_args);
    free(thread_return);
    free(matrix_a);
    free(matrix_b);
    free(matrix_c);

    write_matrix(output_file, ma, matrix_row_col_size);

    free(ma);

    time_end = clock();
    delta_time = (time_end - time_start) / CLOCKS_PER_SEC;
    get_timestamp(timestamp_buf);
    printf("%s The process has written the output file. The total time spent is %lf seconds.\n", timestamp_buf, delta_time);

    return 0;
}

void *calculation_thread(void *arg){
    CalcThreadArgs *thread_args = (CalcThreadArgs *)arg;
    CalcThreadReturn *thread_return = (CalcThreadReturn *)malloc(sizeof(CalcThreadReturn));
    int thread_id = thread_args->thread_id;
    int matrix_row_col_size = (int) pow(2.0, (double) n);
    double delta_time;
    double time_start, time_end;
    char timestamp_buf[26];

    time_start = clock();

    thread_return->column_count = (int) ceil((double) matrix_row_col_size / (double) thread_count);
    thread_return->matrix_c_column = (double *)malloc(thread_return->column_count * matrix_row_col_size * sizeof(double));
    if(thread_return->matrix_c_column == NULL){
        perror("matrix memory malloc: ");
        return NULL;
    }

    for(int i = 0; i < thread_return->column_count * matrix_row_col_size; i++){
        thread_return->matrix_c_column[i] = 0;
    }

    __matrix_multiply(matrix_a, matrix_b, thread_return->matrix_c_column, matrix_row_col_size, thread_id, thread_count);
    insert_columns(matrix_c, thread_return, thread_count, thread_id, matrix_row_col_size);

    time_end = clock();
    delta_time = (time_end - time_start) / CLOCKS_PER_SEC;
    get_timestamp(timestamp_buf);
    printf("%s Thread %d has reached the rendezvous point in %lf seconds.\n", timestamp_buf, thread_id, delta_time);

    syncronization_barrier();

    get_timestamp(timestamp_buf);
    printf("%s Thread %d is advancing to the second part\n", timestamp_buf, thread_id);
    
    time_start = clock();

    for(int i = 0; i < matrix_row_col_size; i++){
        for(int j = thread_id; j < matrix_row_col_size; j+=thread_count){
            discrete_fourier_transform(i, j, matrix_row_col_size, &ma[i * matrix_row_col_size + j]);
            if(sigint_interrupt){
                free(thread_return->matrix_c_column);
                free(thread_return);
                pthread_exit(NULL);
            }
        }
    }

    time_end = clock();
    delta_time = (time_end - time_start) / CLOCKS_PER_SEC;

    get_timestamp(timestamp_buf);
    printf("%s Thread %d has has finished the second part in %lf seconds.\n", timestamp_buf, thread_id, delta_time);

    free(thread_return->matrix_c_column);
    free(thread_return);
    pthread_exit(NULL);
}

int check_arguments(int argc, char *argv[], char *matrix_file_a, char *matrix_file_b, char *output_file, int *n, int *thread_count){
    int flag_file_a = 0;
    int flag_file_b = 0;
    int flag_output = 0;
    int flag_n = 0;
    int flag_thread_count = 0;

    if (argc != 11){
        return -1;
    }

    for(int i = 1; i < argc - 1; ++i){
        if(strcmp(argv[i], "-i") == 0){
            flag_file_a = 1;
            strcpy(matrix_file_a, argv[i + 1]);
        }
        else if(strcmp(argv[i], "-j") == 0){
            flag_file_b = 1;
            strcpy(matrix_file_b, argv[i + 1]);
        }
        else if(strcmp(argv[i], "-o") == 0){
            flag_output = 1;
            strcpy(output_file, argv[i + 1]);
        }
        else if(strcmp(argv[i], "-n") == 0){
            flag_n = 1;
            *n = atoi(argv[i + 1]);
        }
        else if(strcmp(argv[i], "-m") == 0){
            flag_thread_count = 1;
            *thread_count = atoi(argv[i + 1]);
        }
    }

    if(flag_file_a == 0 || flag_file_b == 0 || flag_output == 0 || flag_n == 0 || flag_thread_count == 0){
        return -1;
    }

    if(*n <= 2 || *thread_count < 2){
        return -1;
    }

    return 0;
}


int read_matrices(const char* matrix_file_a, const char *matrix_file_b, char *matrix_a, char *matrix_b, int matrix_row_col_size){
    int fd; 
    int read_bytes;

    fd = open(matrix_file_a, O_RDONLY);
    if(fd < 0){
        perror("open matrix_file_a: ");
        return -1;
    }

    NO_EINTR(read_bytes = read(fd, matrix_a, matrix_row_col_size * matrix_row_col_size * sizeof(char)));
    if(read_bytes < 0){
        perror("read matrix_file_a: ");
        return -1;
    }
    else if(read_bytes < matrix_row_col_size * matrix_row_col_size * sizeof(char)){
        fprintf(stderr, "read matrix_file_a: insufficient characters in file, read only %d bytes\n", read_bytes);
        return -1;
    }

    if(close(fd)){
        perror("close matrix_file_a: ");
        return -1;
    }

    fd = open(matrix_file_b, O_RDONLY);
    if(fd < 0){
        perror("open matrix_file_b: ");
        return -1;
    }

    NO_EINTR(read_bytes = read(fd, matrix_b, matrix_row_col_size * matrix_row_col_size * sizeof(char)));
    if(read_bytes < 0){
        perror("read matrix_file_b: ");
        return -1;
    }
    else if(read_bytes < matrix_row_col_size * matrix_row_col_size * sizeof(char)){
        fprintf(stderr, "read matrix_file_b: insufficient characters in file, read only %d bytes\n", read_bytes);
        return -1;
    }

    if(close(fd)){
        perror("close matrix_file_b: ");
        return -1;
    }

    return 0;
}

int write_matrix(const char *output_file, double complex *matrix_ft, int matrix_row_col_size){
    int fd;
    int write_bytes;
    char value[1024];

    fd = open(output_file, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if(fd < 0){
        perror("open output_file: ");
        return -1;
    }

    for(int i = 0; i < matrix_row_col_size; ++i){
        for(int j = 0; j < matrix_row_col_size; ++j){
            if(j == matrix_row_col_size - 1){
                sprintf(value, "%.3f + (i %.3f)\n", creal(matrix_ft[i * matrix_row_col_size + j]), cimag(matrix_ft[i * matrix_row_col_size + j]));
            }
            else{
                sprintf(value, "%.3f + (i %.3f),", creal(matrix_ft[i * matrix_row_col_size + j]), cimag(matrix_ft[i * matrix_row_col_size + j]));
            }
            NO_EINTR(write_bytes = write(fd, value, strlen(value) * sizeof(char)));
            if(write_bytes < 0){
                perror("write output_file: ");
                return -1;
            }
        }
    }


    if(close(fd) < 0){
        perror("close output_file: ");
        return -1;
    }

    return 0;
}

void __matrix_multiply(char *matrix_a, char *matrix_b, double *matrix_c_column, int matrix_row_col_size, int column_num, int thread_count){
    int ind = 0;
    for(int i = 0; i < matrix_row_col_size; i++){
        for(int j = column_num; j < matrix_row_col_size; j+=thread_count){
            if(sigint_interrupt)
                return;
            for(int k = 0; k < matrix_row_col_size; k++){
                matrix_c_column[ind] += (double)matrix_a[j * matrix_row_col_size + k] * (double)matrix_b[k * matrix_row_col_size + i];
            }
            ind++;
        }
    }
}

void insert_columns(double* matrix_c, CalcThreadReturn *calc_thread_return, int thread_count, int thread_id, int matrix_row_col_size){
    int k = 0;
    for(int j = 0; j < matrix_row_col_size; j++){
        for(int i = thread_id; i < matrix_row_col_size; i+=thread_count){
            matrix_c[(i * matrix_row_col_size) + j] = calc_thread_return->matrix_c_column[k++];
        }
    }
}

void discrete_fourier_transform(int k, int l, int matrix_row_col_size, double complex *ma){
    double cell_value;
    double complex *mb = (double complex*)malloc(matrix_row_col_size * matrix_row_col_size * sizeof(double complex));
    *ma = 0;

    for(int m = 0; m < matrix_row_col_size; ++m){
        *mb = 0;
        for(int n = 0; n < matrix_row_col_size; ++n){
            if(sigint_interrupt)
                return;
            cell_value = matrix_c[m * matrix_row_col_size + n];
            *mb += (cell_value) * cexp(I * (-2 * M_PI * (double)k * (double)m / (double)matrix_row_col_size)) * cexp(I * (-2 * M_PI * (double)l * (double)n / (double)matrix_row_col_size));
        }
        *ma += *mb;
    }

    free(mb);
}

void syncronization_barrier(){
    pthread_mutex_lock(&barrier_mutex);
    arrived++;

    if(arrived < thread_count)
        pthread_cond_wait(&barrier_cond, &barrier_mutex);
    else
        pthread_cond_broadcast(&barrier_cond);
    pthread_mutex_unlock(&barrier_mutex);
}

void get_timestamp(char *timestamp_buf){
    time_t start;
    struct tm* tm_info;

    time(&start);
    tm_info = localtime(&start);
    strftime(timestamp_buf, 26, "%Y-%m-%d %H:%M:%S", tm_info);
}