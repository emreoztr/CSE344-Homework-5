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

int check_arguments(int argc, char *argv[], char *matrix_file_a, char *matrix_file_b, char *output_file, int *n, int *thread_count);
void *calculation_thread(void *arg);
int read_matrices(const char* matrix_file_a, const char *matrix_file_b, char *matrix_a, char *matrix_b, int matrix_row_col_size);
int write_matrix(const char *output_file, double complex *matrix_c, int matrix_row_col_size);
void __matrix_multiply(char *matrix_a, char *matrix_b, double *matrix_c, int matrix_row_col_size, int column_num, int thread_count);
void insert_columns(double* matrix_c, CalcThreadReturn *calc_thread_return, int thread_count, int thread_id, int matrix_row_col_size);
void discrete_fourier_transform(int k, int l, int matrix_row_col_size, double complex *ma);


int main(int argc, char *argv[])
{
    char matrix_file_a[100];
    char matrix_file_b[100];
    char output_file[100];

    int matrix_row_col_size;
    pthread_t *threads;
    CalcThreadArgs *thread_args;
    int total_matrix_size;

    CalcThreadReturn *thread_return = NULL;

    if(check_arguments(argc, argv, matrix_file_a, matrix_file_b, output_file, &n, &thread_count) < 0){
        fprintf(stderr, "Error in arguments, usage: ./hw5 -i filePath1 -j filePath2 -o output -n 4 -m 2\n");
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

    threads = (pthread_t *)malloc(thread_count * sizeof(pthread_t));
    if(threads == NULL){
        perror("thread memory malloc: ");
        return -1;
    }

    thread_args = (CalcThreadArgs *)malloc(thread_count * sizeof(CalcThreadArgs));

    barrier_mutex = (pthread_mutex_t)PTHREAD_MUTEX_INITIALIZER;
    barrier_cond = (pthread_cond_t)PTHREAD_COND_INITIALIZER;

    for(int i = 0; i < thread_count; i++){
        thread_args[i].thread_id = i;
        if(pthread_create(&threads[i], NULL, calculation_thread, (void *) (&thread_args[i])) != 0){
            perror("pthread_create: ");
            return -1;
        }
    }

    pthread_cond_destroy(&barrier_cond);
    pthread_mutex_destroy(&barrier_mutex);

    for(int i = 0; i < thread_count; i++){
        if(pthread_join(threads[i], (void*)&thread_return) != 0){
            perror("pthread_join: ");
            return -1;
        }
        if(thread_return != NULL){
            
        }
        else{
            fprintf(stderr, "Error in thread return\n");
            return -1;
        }
    }


    //TODO

    write_matrix(output_file, ma, matrix_row_col_size);

    return 0;
}

void *calculation_thread(void *arg){
    CalcThreadArgs *thread_args = (CalcThreadArgs *)arg;
    CalcThreadReturn *thread_return = (CalcThreadReturn *)malloc(sizeof(CalcThreadReturn));
    int thread_id = thread_args->thread_id;
    int matrix_row_col_size = (int) pow(2.0, (double) n);

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

    pthread_mutex_lock(&barrier_mutex);
    arrived++;

    if(arrived < thread_count)
        pthread_cond_wait(&barrier_cond, &barrier_mutex);
    else
        pthread_cond_broadcast(&barrier_cond);
    pthread_mutex_unlock(&barrier_mutex);

    

     for(int i = 0; i < matrix_row_col_size; i++){
        for(int j = thread_id; j < matrix_row_col_size; j+=thread_count){
            discrete_fourier_transform(j, i, matrix_row_col_size, &ma[j * matrix_row_col_size + i]);
            printf("%d + %d %d\n", j, i, thread_id);
        }
    }

    pthread_exit((void *)thread_return);
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
                //fprintf(stderr,"%f|%fi\n", creal(matrix_ft[i * matrix_row_col_size + j]), cimag(matrix_ft[i * matrix_row_col_size + j]));
                sprintf(value, "%.3f|%.3f\n", creal(matrix_ft[i * matrix_row_col_size + j]), cimag(matrix_ft[i * matrix_row_col_size + j]));
                fprintf(stderr, "%lf   ", matrix_c[i * matrix_row_col_size + j]);
            }
            else{
                //fprintf(stderr,"%f|%fi\n", creal(matrix_ft[i * matrix_row_col_size + j]), cimag(matrix_ft[i * matrix_row_col_size + j]));
                sprintf(value, "%.3f|%.3f,", creal(matrix_ft[i * matrix_row_col_size + j]), cimag(matrix_ft[i * matrix_row_col_size + j]));
                fprintf(stderr, "%lf    ", matrix_c[i * matrix_row_col_size + j]);
            }
            NO_EINTR(write_bytes = write(fd, value, strlen(value) * sizeof(char)));
            if(write_bytes < 0){
                perror("write output_file: ");
                return -1;
            }
        }
        printf("\n");
    }


    if(close(fd) < 0){
        perror("close output_file: ");
        return -1;
    }

    return 0;
}

void __matrix_multiply(char *matrix_a, char *matrix_b, double *matrix_c_column, int matrix_row_col_size, int column_num, int thread_count){
    printf("%d %d %d\n", column_num, thread_count, matrix_row_col_size);
    int ind = 0;
    for(int i = 0; i < matrix_row_col_size; i++){
        for(int j = column_num; j < matrix_row_col_size; j+=thread_count){
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

//calculate fourier transform of ma
void discrete_fourier_transform(int k, int l, int matrix_row_col_size, double complex *ma){
    double cell_value;
    double complex *mb = (double complex*)malloc(matrix_row_col_size * matrix_row_col_size * sizeof(double complex));
    *ma = 0;

    for(int m = 0; m < matrix_row_col_size; ++m){
        *mb = 0;
        for(int n = 0; n < matrix_row_col_size; ++n){
            cell_value = matrix_c[m * matrix_row_col_size + n];
            *mb += (cell_value) * cexp(-2 * I * (M_PI * (double)k * (double)m / (double)matrix_row_col_size)) * cexp(-2 * I * (M_PI * (double)l * (double)n / (double)matrix_row_col_size));
            printf("%lf\n", cell_value);
        }
        *ma += *mb;
    }
}
